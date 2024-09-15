from fastapi.exception_handlers import (
    request_validation_exception_handler,
)
from pprint import pprint
from fastapi import FastAPI, Request, status, BackgroundTasks
from fastapi.responses import ORJSONResponse, RedirectResponse
from fastapi.exceptions import RequestValidationError
import httpx
from exchange.stock.kis import KoreaInvestment
from exchange.model import MarketOrder, PriceRequest, HedgeData, OrderRequest
from exchange.utility import (
    settings,
    log_order_message,
    log_alert_message,
    print_alert_message,
    logger_test,
    log_order_error_message,
    log_validation_error_message,
    log_hedge_message,
    log_error_message,
    log_message,
)
import traceback
from exchange import get_exchange, log_message, db, settings, get_bot
import ipaddress
import os
import sys
from devtools import debug
import asyncio
from concurrent.futures import ThreadPoolExecutor

VERSION = "0.1.3"
app = FastAPI(default_response_class=ORJSONResponse)

# 글로벌 딕셔너리 추가
ongoing_pairs = {}

def get_error(e):
    tb = traceback.extract_tb(e.__traceback__)
    target_folder = os.path.abspath(os.path.dirname(tb[0].filename))
    error_msg = []

    for tb_info in tb:
        error_msg.append(
            f"File {tb_info.filename}, line {tb_info.lineno}, in {tb_info.name}"
        )
        error_msg.append(f"  {tb_info.line}")

    error_msg.append(str(e))

    return error_msg


@app.on_event("startup")
async def startup():
    log_message(f"POABOT 실행 완료! - 버전:{VERSION}")


@app.on_event("shutdown")
async def shutdown():
    db.close()


whitelist = [
    "52.89.214.238",
    "34.212.75.30",
    "54.218.53.128",
    "52.32.178.7",
    "127.0.0.1",
]
whitelist = whitelist + settings.WHITELIST


@app.middleware("http")
async def whitelist_middleware(request: Request, call_next):
    try:
        if (
            request.client.host not in whitelist
            and not ipaddress.ip_address(request.client.host).is_private
        ):
            msg = f"{request.client.host}는 안됩니다"
            print(msg)
            return ORJSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": f"{request.client.host}는 허용되지 않습니다"},
            )
    except Exception as e:
        log_error_message(traceback.format_exc(), "미들웨어 에러")
    else:
        response = await call_next(request)
        return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    msgs = [
        f"[에러{index+1}] " + f"{error.get('msg')} \n{error.get('loc')}"
        for index, error in enumerate(exc.errors())
    ]
    message = "[Error]\n"
    for msg in msgs:
        message = message + msg + "\n"

    log_validation_error_message(f"{message}\n {exc.body}")
    return await request_validation_exception_handler(request, exc)


@app.get("/ip")
async def get_ip():
    try:
        data = httpx.get("https://ipv4.jsonip.com").json()["ip"]
        log_message(f"IP 조회: {data}")
        return {"ip": data}
    except Exception as e:
        log_error_message(f"IP 조회 중 오류 발생: {str(e)}", {})
        return {"error": "IP 조회 중 오류가 발생했습니다."}


@app.get("/hi")
async def welcome():
    return "hi!!"


@app.post("/price")
async def price(price_req: PriceRequest, background_tasks: BackgroundTasks):
    try:
        exchange = get_exchange(price_req.exchange)
        price = exchange.dict()[price_req.exchange].fetch_price(
            price_req.base, price_req.quote
        )
        log_message(f"가격 조회: {price_req.base}/{price_req.quote} = {price}")
        return {"price": price}
    except Exception as e:
        error_msg = get_error(e)
        log_error_message("\n".join(error_msg), {})
        return {"error": "가격 조회 중 오류가 발생했습니다."}


def log(exchange_name, result, order_info):
    log_order_message(exchange_name, result, order_info)
    print_alert_message(order_info)


def log_error(error_message, order_info):
    log_order_error_message(error_message, order_info)
    log_alert_message(order_info, "실패")


# 헬퍼 함수 추가
async def wait_for_pair_sell_completion_and_buy(
    exchange_name: str,
    pair_ticker: str,
    order_info: MarketOrder,
    kis_number: int,
    exchange_instance: KoreaInvestment
):
    try:
        # DEBUG: 함수 시작
        print(f"DEBUG: wait_for_pair_sell_completion_and_buy 시작 - 페어: {pair_ticker}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            for attempt in range(10):  # 최대 10초 동안 시도
                # DEBUG: 시도 번호
                print(f"DEBUG: 시도 {attempt + 1}/10 - 페어: {pair_ticker}")

                # 비동기적으로 동기 함수를 실행
                korea_balance = await loop.run_in_executor(pool, exchange_instance.korea_fetch_balance)
                usa_balance = await loop.run_in_executor(pool, exchange_instance.usa_fetch_balance)

                # DEBUG: 잔고 확인
                print(f"DEBUG: 한국 잔고 - {korea_balance}")
                print(f"DEBUG: 미국 잔고 - {usa_balance}")

                if korea_balance is None or usa_balance is None:
                    print(f"DEBUG: 잔고 정보가 없어서 대기 중 - 페어: {pair_ticker}")
                    await asyncio.sleep(1)
                    continue

                # 한국 주식 보유 수량 확인 (페어 티커 기준)
                korea_holding = next(
                    (item for item in korea_balance.output1 if item.prdt_name == pair_ticker), None
                )
                korea_holding_qty = int(korea_holding.hldg_qty) if korea_holding else 0

                # 미국 주식 보유 수량 확인 (페어 티커 기준)
                usa_holding = next(
                    (item for item in usa_balance.output1 if item.ovrs_item_name == pair_ticker), None
                )
                usa_holding_qty = int(usa_holding.ovrs_cblc_qty) if usa_holding else 0

                # DEBUG: 보유 수량 확인
                print(f"DEBUG: 한국 보유량 - {korea_holding_qty}, 미국 보유량 - {usa_holding_qty}")

                if korea_holding_qty == 0 and usa_holding_qty == 0:
                    # DEBUG: 매수 주문 진행
                    print(f"DEBUG: 페어 매도 완료 - 매수 주문 진행 중 - 페어: {pair_ticker}")

                    # 해당 페어의 모든 매도 주문이 완료되었으므로 매수 주문을 진행
                    buy_result = exchange_instance.create_order(
                        exchange=exchange_name,
                        ticker=order_info.base,
                        order_type=order_info.type.lower(),
                        side="buy",
                        amount=order_info.amount,
                    )
                    log(exchange_name, buy_result, order_info)
                    break
                else:
                    # DEBUG: 매도 미완료
                    print(f"DEBUG: 매도 주문 미완료 - 1초 대기 중 - 페어: {pair_ticker}")
                    await asyncio.sleep(1)
            else:
                # DEBUG: 타임아웃 발생
                print(f"DEBUG: 페어 매도가 10초 내에 완료되지 않음 - 페어: {pair_ticker}")
                raise TimeoutError(f"페어 {pair_ticker}의 매도가 10초 내에 완료되지 않았습니다.")
    except Exception as e:
        error_msg = get_error(e)
        log_error("\n".join(error_msg), order_info)
    finally:
        # DEBUG: 작업 완료
        print(f"DEBUG: wait_for_pair_sell_completion_and_buy 작업 완료 - 페어: {pair_ticker}")

        # 작업이 끝났으므로 글로벌 딕셔너리에서 제거
        ongoing_pairs.pop(pair_ticker, None)


@app.post("/order")
@app.post("/")
async def order(order_info: MarketOrder, background_tasks: BackgroundTasks):
    order_result = None
    try:
        exchange_name = order_info.exchange
        bot = get_bot(exchange_name, order_info.kis_number)
        bot.init_info(order_info)

        # 주식 주문 처리
        if bot.order_info.is_stock:
            # PAIR가 없으면 기존 로직 수행
            if not order_info.pair:
                # DEBUG: PAIR 없음 로그
                print(f"DEBUG: PAIR 없음 - 기존 주문 처리 중 - 주문: {order_info}")

                order_result = bot.create_order(
                    bot.order_info.exchange,
                    bot.order_info.base,
                    order_info.type.lower(),
                    order_info.side.lower(),
                    order_info.amount,
                )

            # PAIR가 있을 때 추가 로직 (해당 페어만 매도)
            else:
                pair_ticker = order_info.pair

                # DEBUG: PAIR 처리 시작 로그
                print(f"DEBUG: PAIR 존재 - 처리 중 - 페어: {pair_ticker}")

                # 이미 해당 페어에 대한 주문이 진행 중인지 확인
                if pair_ticker in ongoing_pairs:
                    # DEBUG: PAIR 주문 중복
                    print(f"DEBUG: PAIR 주문 중복 - 진행 중인 주문 있음 - 페어: {pair_ticker}")
                    return ORJSONResponse(
                        status_code=status.HTTP_409_CONFLICT,
                        content={"detail": f"{pair_ticker}에 대한 주문이 이미 진행 중입니다."},
                    )

                # 현재 보유량 조회를 위한 잔고 확인
                # DEBUG: 잔고 조회 시작
                print(f"DEBUG: 잔고 조회 시작 - 페어: {pair_ticker}")

                # 비동기적으로 잔고 조회
                korea_balance = await asyncio.get_event_loop().run_in_executor(None, bot.korea_fetch_balance)
                usa_balance = await asyncio.get_event_loop().run_in_executor(None, bot.usa_fetch_balance)

                # DEBUG: 잔고 확인
                print(f"DEBUG: 한국 잔고 - {korea_balance}")
                print(f"DEBUG: 미국 잔고 - {usa_balance}")

                if korea_balance is None or usa_balance is None:
                    print(f"DEBUG: 잔고 정보가 없어서 매도 주문을 진행할 수 없음 - 페어: {pair_ticker}")
                    return ORJSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={"detail": f"{pair_ticker}의 잔고 정보를 가져올 수 없습니다."},
                    )

                # 한국 주식 보유 수량 확인 (페어 티커 기준)
                korea_holding = next(
                    (item for item in korea_balance.output1 if item.prdt_name == pair_ticker), None
                )
                korea_holding_qty = int(korea_holding.hldg_qty) if korea_holding else 0

                # 미국 주식 보유 수량 확인 (페어 티커 기준)
                usa_holding = next(
                    (item for item in usa_balance.output1 if item.ovrs_item_name == pair_ticker), None
                )
                usa_holding_qty = int(usa_holding.ovrs_cblc_qty) if usa_holding else 0

                # DEBUG: 보유 수량 확인
                print(f"DEBUG: 한국 보유량 - {korea_holding_qty}, 미국 보유량 - {usa_holding_qty}")

                # 총 보유 수량 계산
                pair_amount = korea_holding_qty + usa_holding_qty

                # DEBUG: PAIR 총 보유 수량
                print(f"DEBUG: PAIR 총 보유량 - {pair_amount}")

                if pair_amount > 0:
                    # 해당 페어의 매도 주문이 진행 중임을 표시
                    ongoing_pairs[pair_ticker] = True

                    # 페어 주식을 모두 매도
                    sell_result = bot.create_order(
                        bot.order_info.exchange,
                        pair_ticker,
                        "market",
                        "sell",
                        pair_amount,
                    )
                    # DEBUG: 매도 주문 결과
                    print(f"DEBUG: 매도 주문 결과 - {sell_result}")

                    # DEBUG: 백그라운드 작업 추가
                    print(f"DEBUG: 백그라운드 작업 추가 - 페어: {pair_ticker}")

                    # 백그라운드 작업으로 페어 매도 완료 확인 후 매수 주문 진행
                    background_tasks.add_task(
                        wait_for_pair_sell_completion_and_buy,
                        exchange_name,
                        pair_ticker,
                        order_info,
                        order_info.kis_number,
                        bot
                    )

        # 결과를 로그에 기록
        background_tasks.add_task(log, exchange_name, order_result, order_info)

    except Exception as e:
        error_msg = get_error(e)  # KoreaInvestment 클래스의 get_error 메서드 호출
        background_tasks.add_task(log_error, "\n".join(error_msg), order_info)
    else:
        return {"result": "success"}

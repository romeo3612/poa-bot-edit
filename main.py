from fastapi.exception_handlers import request_validation_exception_handler
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
from exchange import get_exchange, log_message, db, settings, get_bot, pocket
import ipaddress
import os
import sys
from devtools import debug
import asyncio
from concurrent.futures import ThreadPoolExecutor

VERSION = "1.0.0"
app = FastAPI(default_response_class=ORJSONResponse)

# 글로벌 딕셔너리 추가 (페어 진행 상태 저장)
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

# 페어트레이딩 매도 처리 로직 추가 (매수와 매도 구분)
async def wait_for_pair_sell_completion_and_buy(
    exchange_name: str,
    pair_ticker: str,
    order_info: MarketOrder,
    kis_number: int,
    exchange_instance: KoreaInvestment
):
    try:
        print(f"DEBUG: wait_for_pair_sell_completion_and_buy 시작 - 페어: {pair_ticker}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            for attempt in range(12):  # 최대 12번, 1분 동안 시도
                print(f"DEBUG: 시도 {attempt + 1}/12 - 페어: {pair_ticker}")

                # 거래소에 따라 KRX 또는 USA 잔고만 조회
                if exchange_name == "KRX":
                    korea_balance = await loop.run_in_executor(pool, exchange_instance.korea_fetch_balance)
                    print(f"DEBUG: 한국 잔고 - {korea_balance}")

                    korea_holding = next(
                        (item for item in korea_balance.output1 if item.prdt_name == pair_ticker), None
                    )
                    korea_holding_qty = int(korea_holding.hldg_qty) if korea_holding else 0

                    if korea_holding_qty == 0:
                        print(f"DEBUG: 페어 매도 완료 - 매수 주문 진행 중 - 페어: {pair_ticker}")

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
                        print(f"DEBUG: 잔고 남음 - 시장가 매도 반복 - 남은 보유량: {korea_holding_qty}")

                        sell_result = exchange_instance.create_order(
                            exchange=exchange_name,
                            ticker=pair_ticker,
                            order_type="market",
                            side="sell",
                            amount=korea_holding_qty,
                        )
                        print(f"DEBUG: 추가 시장가 매도 주문 결과 - {sell_result}")

                elif exchange_name in ["NASDAQ", "NYSE", "AMEX"]:
                    usa_balance = await loop.run_in_executor(pool, exchange_instance.usa_fetch_balance)
                    print(f"DEBUG: 미국 잔고 - {usa_balance}")

                    usa_holding = next(
                        (item for item in usa_balance.output1 if item.ovrs_item_name == pair_ticker), None
                    )
                    usa_holding_qty = int(usa_holding.ovrs_cblc_qty) if usa_holding else 0

                    if usa_holding_qty == 0:
                        print(f"DEBUG: 페어 매도 완료 - 매수 주문 진행 중 - 페어: {pair_ticker}")

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
                        print(f"DEBUG: 잔고 남음 - 시장가 매도 반복 - 남은 보유량: {usa_holding_qty}")

                        sell_result = exchange_instance.create_order(
                            exchange=exchange_name,
                            ticker=pair_ticker,
                            order_type="market",
                            side="sell",
                            amount=usa_holding_qty,
                        )
                        print(f"DEBUG: 추가 시장가 매도 주문 결과 - {sell_result}")

                await asyncio.sleep(5)

            else:
                print(f"DEBUG: 페어 매도가 12번 시도 후에도 완료되지 않음 - 페어: {pair_ticker}")
                raise TimeoutError(f"페어 {pair_ticker}의 매도가 1분 내에 완료되지 않았습니다.")
    except Exception as e:
        error_msg = get_error(e)
        log_error("\n".join(error_msg), order_info)
    finally:
        print(f"DEBUG: wait_for_pair_sell_completion_and_buy 작업 완료 - 페어: {pair_ticker}")
        ongoing_pairs.pop(pair_ticker, None)

@app.post("/order")
@app.post("/")
async def order(order_info: MarketOrder, background_tasks: BackgroundTasks):
    order_result = None
    try:
        exchange_name = order_info.exchange
        bot = get_bot(exchange_name, order_info.kis_number)
        bot.init_info(order_info)

        print(f"DEBUG: 주문 시작 - exchange_name: {exchange_name}, order_info: {order_info}")

        # 주식 주문 처리
        if bot.order_info.is_stock:
            print(f"DEBUG: 주식 주문 - is_stock: {bot.order_info.is_stock}")

            # 페어가 있을 때만 페어트레이딩 로직 진입
            if not order_info.pair:
                print(f"DEBUG: PAIR 없음 - 기존 주문 처리 중 - 주문: {order_info}")

                # 일반 주문 처리
                order_result = bot.create_order(
                    bot.order_info.exchange,
                    bot.order_info.base,
                    order_info.type.lower(),
                    order_info.side.lower(),
                    order_info.amount,
                )
                print(f"DEBUG: 일반 주문 처리 결과 - {order_result}")

            else:
                pair_ticker = order_info.pair
                print(f"DEBUG: PAIR 존재 - 페어트레이딩 처리 중 - 페어: {pair_ticker}")

                if pair_ticker in ongoing_pairs:
                    print(f"DEBUG: {pair_ticker}에 대한 주문이 이미 진행 중입니다.")
                    return ORJSONResponse(
                        status_code=status.HTTP_409_CONFLICT,
                        content={"detail": f"{pair_ticker}에 대한 주문이 이미 진행 중입니다."},
                    )

                print(f"DEBUG: 잔고 조회 시작 - 페어: {pair_ticker}")
                if exchange_name == "KRX":
                    korea_balance = await asyncio.get_event_loop().run_in_executor(None, bot.korea_fetch_balance)
                    print(f"DEBUG: 한국 잔고 - {korea_balance}")

                    korea_holding = next(
                        (item for item in korea_balance.output1 if item.prdt_name == pair_ticker), None
                    )
                    korea_holding_qty = int(korea_holding.hldg_qty) if korea_holding else 0

                    if korea_holding_qty > 0:
                        ongoing_pairs[pair_ticker] = True
                        sell_result = bot.create_order(
                            bot.order_info.exchange,
                            pair_ticker,
                            "market",
                            "sell",
                            korea_holding_qty,
                        )
                        print(f"DEBUG: 매도 주문 결과 - {sell_result}")

                        background_tasks.add_task(
                            log,
                            exchange_name,
                            sell_result,
                            order_info,
                        )

                        print(f"DEBUG: 백그라운드 작업 추가 - 페어 매도 후 매수 진행")
                        background_tasks.add_task(
                            wait_for_pair_sell_completion_and_buy,
                            exchange_name,
                            pair_ticker,
                            order_info,
                            order_info.kis_number,
                            bot
                        )
                    else:
                        print(f"DEBUG: 페어 보유량이 0 - 바로 매수 주문 진행")
                        buy_result = bot.create_order(
                            bot.order_info.exchange,
                            bot.order_info.base,
                            "market",
                            "buy",
                            order_info.amount,
                        )
                        print(f"DEBUG: 매수 주문 결과 - {buy_result}")

                        background_tasks.add_task(
                            log,
                            exchange_name,
                            buy_result,
                            order_info,
                        )

                elif exchange_name in ["NASDAQ", "NYSE", "AMEX"]:
                    usa_balance = await asyncio.get_event_loop().run_in_executor(None, bot.usa_fetch_balance)
                    print(f"DEBUG: 미국 잔고 - {usa_balance}")

                    usa_holding = next(
                        (item for item in usa_balance.output1 if item.ovrs_item_name == pair_ticker), None
                    )
                    usa_holding_qty = int(usa_holding.ovrs_cblc_qty) if usa_holding else 0

                    if usa_holding_qty > 0:
                        ongoing_pairs[pair_ticker] = True
                        sell_result = bot.create_order(
                            bot.order_info.exchange,
                            pair_ticker,
                            "market",
                            "sell",
                            usa_holding_qty,
                        )
                        print(f"DEBUG: 매도 주문 결과 - {sell_result}")

                        background_tasks.add_task(
                            log,
                            exchange_name,
                            sell_result,
                            order_info,
                        )

                        print(f"DEBUG: 백그라운드 작업 추가 - 페어 매도 후 매수 진행")
                        background_tasks.add_task(
                            wait_for_pair_sell_completion_and_buy,
                            exchange_name,
                            pair_ticker,
                            order_info,
                            order_info.kis_number,
                            bot
                        )
                    else:
                        print(f"DEBUG: 페어 보유량이 0 - 바로 매수 주문 진행")
                        buy_result = bot.create_order(
                            bot.order_info.exchange,
                            bot.order_info.base,
                            "market",
                            "buy",
                            order_info.amount,
                        )
                        print(f"DEBUG: 매수 주문 결과 - {buy_result}")

                        background_tasks.add_task(
                            log,
                            exchange_name,
                            buy_result,
                            order_info,
                        )

        print(f"DEBUG: 최종 주문 처리 결과 - {order_result if order_result else 'success'}")
        background_tasks.add_task(log, exchange_name, order_result, order_info)

    except Exception as e:
        error_msg = get_error(e)
        print(f"DEBUG: 주문 처리 중 예외 발생 - {error_msg}")
        background_tasks.add_task(log_error, "\n".join(error_msg), order_info)
    else:
        return {"result": order_result if order_result else "success"}


@app.post("/hedge")
async def hedge(hedge_data: HedgeData, background_tasks: BackgroundTasks):
    exchange_name = hedge_data.exchange.upper()
    bot = get_bot(exchange_name)
    upbit = get_bot("UPBIT")

    base = hedge_data.base
    quote = hedge_data.quote
    amount = hedge_data.amount
    leverage = hedge_data.leverage
    hedge = hedge_data.hedge

    foreign_order_info = OrderRequest(
        exchange=exchange_name,
        base=base,
        quote=quote,
        side="entry/sell",
        type="market",
        amount=amount,
        leverage=leverage,
    )
    bot.init_info(foreign_order_info)
    if hedge == "ON":
        try:
            if amount is None:
                raise Exception("헷지할 수량을 요청하세요")
            binance_order_result = bot.market_entry(foreign_order_info)
            binance_order_amount = binance_order_result["amount"]
            pocket.create(
                "kimp",
                {
                    "exchange": "BINANCE",
                    "base": base,
                    "quote": quote,
                    "amount": binance_order_amount,
                },
            )
            if leverage is None:
                leverage = 1
            try:
                korea_order_info = OrderRequest(
                    exchange="UPBIT",
                    base=base,
                    quote="KRW",
                    side="buy",
                    type="market",
                    amount=binance_order_amount,
                )
                upbit.init_info(korea_order_info)
                upbit_order_result = upbit.market_buy(korea_order_info)
            except Exception as e:
                hedge_records = get_hedge_records(base)
                binance_records_id = hedge_records["BINANCE"]["records_id"]
                binance_amount = hedge_records["BINANCE"]["amount"]
                binance_order_result = bot.market_close(
                    OrderRequest(
                        exchange=exchange_name,
                        base=base,
                        quote=quote,
                        side="close/buy",
                        amount=binance_amount,
                    )
                )
                for binance_record_id in binance_records_id:
                    pocket.delete("kimp", binance_record_id)
                log_message(
                    "[헷지 실패] 업비트에서 에러가 발생하여 바이낸스 포지션을 종료합니다"
                )
            else:
                upbit_order_info = upbit.get_order(upbit_order_result["id"])
                upbit_order_amount = upbit_order_info["filled"]
                pocket.create(
                    "kimp",
                    {
                        "exchange": "UPBIT",
                        "base": base,
                        "quote": "KRW",
                        "amount": upbit_order_amount,
                    },
                )
                log_hedge_message(
                    exchange_name,
                    base,
                    quote,
                    binance_order_amount,
                    upbit_order_amount,
                    hedge,
                )

        except Exception as e:
            # log_message(f"{e}")
            background_tasks.add_task(
                log_error_message, traceback.format_exc(), "헷지 에러"
            )
            return {"result": "error"}
        else:
            return {"result": "success"}

    elif hedge == "OFF":
        try:
            records = pocket.get_full_list(
                "kimp", query_params={"filter": f'base = "{base}"'}
            )
            binance_amount = 0.0
            binance_records_id = []
            upbit_amount = 0.0
            upbit_records_id = []
            for record in records:
                if record.exchange == "BINANCE":
                    binance_amount += record.amount
                    binance_records_id.append(record.id)
                elif record.exchange == "UPBIT":
                    upbit_amount += record.amount
                    upbit_records_id.append(record.id)

            if binance_amount > 0 and upbit_amount > 0:
                # 바이낸스
                order_info = OrderRequest(
                    exchange="BINANCE",
                    base=base,
                    quote=quote,
                    side="close/buy",
                    amount=binance_amount,
                )
                binance_order_result = bot.market_close(order_info)
                for binance_record_id in binance_records_id:
                    pocket.delete("kimp", binance_record_id)
                # 업비트
                order_info = OrderRequest(
                    exchange="UPBIT",
                    base=base,
                    quote="KRW",
                    side="sell",
                    amount=upbit_amount,
                )
                upbit_order_result = upbit.market_sell(order_info)
                for upbit_record_id in upbit_records_id:
                    pocket.delete("kimp", upbit_record_id)

                log_hedge_message(
                    exchange_name, base, quote, binance_amount, upbit_amount, hedge
                )
            elif binance_amount == 0 and upbit_amount == 0:
                log_message(f"{exchange_name}, UPBIT에 종료할 수량이 없습니다")
            elif binance_amount == 0:
                log_message(f"{exchange_name}에 종료할 수량이 없습니다")
            elif upbit_amount == 0:
                log_message("UPBIT에 종료할 수량이 없습니다")
        except Exception as e:
            background_tasks.add_task(
                log_error_message, traceback.format_exc(), "헷지종료 에러"
            )
            return {"result": "error"}
        else:
            return {"result": "success"}

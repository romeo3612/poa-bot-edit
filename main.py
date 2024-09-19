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

VERSION = "1.0.1"
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

# 백그라운드에서 비동기 처리 되는 페어트레이드 매도 로직
async def wait_for_pair_sell_completion(
    exchange_name: str,
    order_info: MarketOrder,
    kis_number: int,
    exchange_instance: KoreaInvestment,
    initial_holding_qty: int  # 미리 조회한 잔고 수량을 받는 인수 추가
):
    try:
        pair = order_info.pair
        print(f"DEBUG: wait_for_pair_sell_completion 시작 - 페어: {pair}, 초기 잔고 수량: {initial_holding_qty}")
        
        total_sell_amount = 0.0
        total_sell_value = 0.0  # 총 매도 금액 추가
        attempt_count = 0  # 시도 횟수 관리 변수
        max_attempts = 12  # 최대 12번 시도 (총 60초)

        # 먼저 초기 잔고 수량에 대해 시장가 매도를 수행
        if initial_holding_qty > 0:
            print(f"DEBUG: 초기 잔고 수량 {initial_holding_qty}, 매도 작업 시작")
            sell_result = exchange_instance.create_order(
                exchange=exchange_name,
                ticker=pair,
                order_type="market",
                side="sell",
                amount=initial_holding_qty,
            )
            print(f"DEBUG: 초기 매도 주문 완료 - 잔고 업데이트 대기, 매도 결과: {sell_result}")
            total_sell_amount += initial_holding_qty
            sell_price = float(sell_result['output']['ORD_TMD'])  # 매도 가격이 어디에 있는지 확인해야 합니다
            total_sell_value += initial_holding_qty * sell_price

        # 이후 남은 잔고가 있는지 확인
        while True:
            print(f"DEBUG: 잔고 확인 중... 시도 횟수: {attempt_count + 1}/{max_attempts}")
            if exchange_name == "KRX":
                balance = exchange_instance.korea_fetch_balance()
                holding = next((item for item in balance.output1 if item.pdno == pair), None)
                holding_qty = int(holding.hldg_qty) if holding else 0
            elif exchange_name in ["NASDAQ", "NYSE", "AMEX"]:
                balance = exchange_instance.usa_fetch_balance()
                holding = next((item for item in balance.output1 if item.ovrs_item_name == pair), None)
                holding_qty = int(holding.ovrs_cblc_qty) if holding else 0
            else:
                raise ValueError(f"지원하지 않는 거래소: {exchange_name}")

            print(f"DEBUG: 현재 잔고 수량: {holding_qty}")

            # 잔고가 없으면 매도 완료
            if holding_qty == 0:
                print(f"DEBUG: 잔고가 0으로 확인되어 매도 완료")
                break

            # 잔고가 남아있으면 추가 매도
            print(f"DEBUG: 잔고 {holding_qty} 남아있음, 추가 매도 시작")
            sell_result = exchange_instance.create_order(
                exchange=exchange_name,
                ticker=pair,
                order_type="market",
                side="sell",
                amount=holding_qty,
            )
            print(f"DEBUG: 추가 매도 완료 - 매도 결과: {sell_result}")
            total_sell_amount += holding_qty
            sell_price = float(sell_result['output']['ORD_TMD'])  # 실제 매도 가격 확인 필요
            total_sell_value += holding_qty * sell_price

            attempt_count += 1
            if attempt_count >= max_attempts:
                raise Exception(f"매도 작업이 {max_attempts}번 시도 후에도 완료되지 않았습니다.")

            await asyncio.sleep(5)  # 5초 대기 후 다시 확인

        # 매도 결과를 PocketBase에 기록
        print(f"DEBUG: 매도 작업 완료, 총 매도량: {total_sell_amount}, 총 매도 금액: {total_sell_value}")
        if total_sell_amount > 0:
            from datetime import datetime
            timestamp = datetime.now().isoformat()
            record_data = {
                "pair_id": order_info.pair_id,
                "amount": total_sell_amount,
                "value": total_sell_value,
                "ticker": pair,
                "exchange": exchange_name,
                "timestamp": timestamp,
                "trade_type": "sell"
            }
            print(f"DEBUG: PocketBase에 기록할 데이터 - {record_data}")
            response = pocket.create("pair_order_history", record_data)
            print(f"DEBUG: PocketBase 기록 응답 - {response}")
            print(f"DEBUG: PocketBase 기록 완료 - 페어: {pair}, 총 매도량: {total_sell_amount}, 총 매도 금액: {total_sell_value}")

        return {"status": "success", "total_sell_amount": total_sell_amount, "total_sell_value": total_sell_value}

    except Exception as e:
        error_msg = get_error(e)
        print(f"DEBUG: 매도 작업 중 예외 발생 - {error_msg}")
        log_error("\n".join(error_msg), order_info)
        return {"status": "error", "error_msg": str(e)}
    finally:
        ongoing_pairs.pop(pair, None)



@app.post("/order")
@app.post("/")
async def order(order_info: MarketOrder, background_tasks: BackgroundTasks):
    order_result = None
    exchange_name = order_info.exchange
    bot = get_bot(exchange_name, order_info.kis_number)
    bot.init_info(order_info)

    print(f"DEBUG: 주문 시작 - exchange_name: {exchange_name}, order_info: {order_info}")

    # 중복 주문 방지
    if order_info.pair in ongoing_pairs:
        print(f"DEBUG: {order_info.pair}에 대한 주문이 이미 진행 중입니다.")
        return {"status": "error", "error_msg": f"{order_info.pair}에 대한 주문이 이미 진행 중입니다."}
    ongoing_pairs[order_info.pair] = True

    try:
        # 주식 주문 처리
        if bot.order_info.is_stock:
            print(f"DEBUG: 주식 주문 - is_stock: {bot.order_info.is_stock}")

            # 페어와 pair_id가 있는 경우
            if order_info.pair and order_info.pair_id:
                pair = order_info.pair
                pair_id = order_info.pair_id
                print(f"DEBUG: PAIR 및 PAIR_ID 존재 - 페어트레이딩 처리 중 - 페어: {pair}, 페어 ID: {pair_id}")
            
                if order_info.side == "buy":
                    # 1. 페어의 보유 수량 확인
                    if exchange_name == "KRX":
                        balance = bot.korea_fetch_balance()
                        holding = next((item for item in balance.output1 if item.pdno == order_info.pair), None)
                        holding_qty = int(holding.hldg_qty) if holding else 0
                    elif exchange_name in ["NASDAQ", "NYSE", "AMEX"]:
                        balance = bot.usa_fetch_balance()
                        holding = next((item for item in balance.output1 if item.ovrs_item_name == order_info.pair), None)
                        holding_qty = int(holding.ovrs_cblc_qty) if holding else 0
                    else:
                        raise ValueError(f"지원하지 않는 거래소: {exchange_name}")

                    # 2. 보유 수량이 0이 아닌 경우 전량 매도 작업을 백그라운드에서 실행
                    if holding_qty > 0:
                        print(f"DEBUG: 페어 보유량이 {holding_qty}입니다. 전량 매도 진행 중.")
                        background_tasks.add_task(wait_for_pair_sell_completion, exchange_name, order_info, order_info.kis_number, bot, holding_qty)
                        print(f"DEBUG: 페어 {order_info.pair} 전량 매도 작업 백그라운드로 실행")

                    # 3. PocketBase에서 동일한 pair_id를 가진 마지막 매도 데이터를 조회
                    print(f"DEBUG: PocketBase에서 조회할 쿼리 - pair_id: {pair_id}, trade_type: 'sell'")
                    records = pocket.get_full_list(
                        "pair_order_history",
                        query_params = {
                            "filter": f'pair_id = "{pair_id}" && trade_type = "sell"',
                            "sort": "-timestamp",
                            "limit": 1
                        }
                    )
                      
                    print(f"DEBUG: PocketBase에서 조회한 기록 - {records}")

                    if records:
                        last_sell_record = records[0]
                        total_sell_value = last_sell_record.value
                        print(f"DEBUG: 마지막 매도 기록 찾음 - value: {total_sell_value}")

                        # 주문 수량 계산
                        adjusted_value = total_sell_value * 0.995
                        price = order_info.price  # 웹훅 메시지에 포함된 가격 사용
                        buy_amount = int(adjusted_value // price)  # 정수 나눗셈, 나머지 버림

                        print(f"DEBUG: 계산된 매수 수량 - buy_amount: {buy_amount}")

                        if buy_amount > 0:
                            # 매수 주문 진행
                            buy_result = bot.create_order(
                                bot.order_info.exchange,
                                bot.order_info.base,
                                "market",
                                "buy",
                                buy_amount,
                            )
                            print(f"DEBUG: 매수 주문 결과 - {buy_result}")
                            background_tasks.add_task(log, exchange_name, buy_result, order_info)
                        else:
                            msg = "계산된 매수 수량이 0입니다."
                            print(f"DEBUG: {msg}")
                            background_tasks.add_task(log_error, msg, order_info)
                    else:
                        # 동일한 pair_id를 가진 데이터가 없으므로 웹훅의 amount로 주문
                        print(f"DEBUG: 동일한 pair_id의 매도 기록이 없음, 웹훅의 amount로 주문 진행")
                        buy_result = bot.create_order(
                            bot.order_info.exchange,
                            bot.order_info.base,
                            "market",
                            "buy",
                            int(order_info.amount),  # 수량은 정수여야 함
                        )
                        print(f"DEBUG: 매수 주문 결과 - {buy_result}")
                        background_tasks.add_task(log, exchange_name, buy_result, order_info)

                elif order_info.side == "sell":
                    # 자신의 상품을 보유하고 있는지 확인
                    if exchange_name == "KRX":
                        balance = bot.korea_fetch_balance()
                        holding = next((item for item in balance.output1 if item.pdno == order_info.base), None)
                        holding_qty = int(holding.hldg_qty) if holding else 0
                    elif exchange_name in ["NASDAQ", "NYSE", "AMEX"]:
                        balance = bot.usa_fetch_balance()
                        holding = next((item for item in balance.output1 if item.ovrs_item_name == order_info.base), None)
                        holding_qty = int(holding.ovrs_cblc_qty) if holding else 0
                    else:
                        raise ValueError(f"지원하지 않는 거래소: {exchange_name}")

                    if holding_qty > 0:
                        # 전량 매도 진행
                        sell_result = bot.create_order(
                            bot.order_info.exchange,
                            bot.order_info.base,
                            "market",
                            "sell",
                            holding_qty,
                        )
                        print(f"DEBUG: 전량 매도 주문 결과 - {sell_result}")
                        # 매도 결과에서 체결 수량과 가격을 가져옴
                        sell_amount = sell_result['filled']
                        sell_price = sell_result['price']
                        sell_value = sell_amount * sell_price

                        # timestamp를 적절한 형태로 저장
                        from datetime import datetime
                        timestamp = datetime.now().isoformat()

                        # PocketBase에 매도 주문 기록
                        record_data = {
                            "pair_id": order_info.pair_id,
                            "amount": sell_amount,
                            "value": sell_value,
                            "ticker": order_info.base,
                            "exchange": exchange_name,
                            "timestamp": timestamp,
                            "trade_type": "sell"
                        }
                        pocket.create("pair_order_history", record_data)
                        print(f"DEBUG: PocketBase 기록 완료 - 티커: {order_info.base}, 매도량: {sell_amount}, 매도금액: {sell_value}")

                        background_tasks.add_task(log, exchange_name, sell_result, order_info)
                    else:
                        # 잔고가 존재하지 않음
                        msg = "잔고가 존재하지 않습니다"
                        print(f"DEBUG: {msg}")
                        background_tasks.add_task(log_error, msg, order_info)
            else:
                # 페어가 없는 경우 기존 주문 처리
                print(f"DEBUG: PAIR 없음 - 기존 주문 처리 중 - 주문: {order_info}")
                order_result = bot.create_order(
                    bot.order_info.exchange,
                    bot.order_info.base,
                    order_info.type.lower(),
                    order_info.side.lower(),
                    int(order_info.amount),  # 수량은 정수여야 함
                )
                print(f"DEBUG: 일반 주문 처리 결과 - {order_result}")
                background_tasks.add_task(log, exchange_name, order_result, order_info)
        else:
            # 기타 자산 주문 처리 (필요 시 추가)
            pass

    except Exception as e:
        error_msg = get_error(e)
        print(f"DEBUG: 주문 처리 중 예외 발생 - {error_msg}")
        print(f"DEBUG: PocketBase에서 기록 조회 중 오류 발생 - {str(e)}")
        background_tasks.add_task(log_error, "\n".join(error_msg), order_info)
    finally:
        ongoing_pairs.pop(order_info.pair, None)

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

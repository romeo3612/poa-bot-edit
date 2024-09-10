from datetime import datetime
import json
import time
import httpx
from exchange.stock.error import TokenExpired
from exchange.stock.schemas import *
from exchange.database import db
from pydantic import validate_arguments
import traceback
import copy
from exchange.model import MarketOrder
from devtools import debug
from queue import Queue
import asyncio

# 주문 대기 큐 생성
order_queue = Queue()

class KoreaInvestment:
    def __init__(
        self,
        key: str,
        secret: str,
        account_number: str,
        account_code: str,
        kis_number: int,
    ):
        self.key = key
        self.secret = secret
        self.kis_number = kis_number
        self.base_url = (
            BaseUrls.base_url.value
            if kis_number != 4
            else BaseUrls.paper_base_url.value
        )
        self.is_auth = False
        self.account_number = account_number
        self.base_headers = {}
        self.session = httpx.Client()
        self.async_session = httpx.AsyncClient()
        self.auth()

        self.base_body = {}
        self.base_order_body = AccountInfo(
            CANO=account_number, ACNT_PRDT_CD=account_code
        )
        self.order_exchange_code = {
            "NASDAQ": ExchangeCode.NASDAQ,
            "NYSE": ExchangeCode.NYSE,
            "AMEX": ExchangeCode.AMEX,
        }
        self.query_exchange_code = {
            "NASDAQ": QueryExchangeCode.NASDAQ,
            "NYSE": QueryExchangeCode.NYSE,
            "AMEX": QueryExchangeCode.AMEX,
        }

        self.order_processing = False  # 중복 주문 방지용

    def init_info(self, order_info: MarketOrder):
        self.order_info = order_info

    def close_session(self):
        self.session.close()

    def get(self, endpoint: str, params: dict = None, headers: dict = None):
        url = f"{self.base_url}{endpoint}"
        return self.session.get(url, params=params, headers=headers).json()

    def post_with_error_handling(
        self, endpoint: str, data: dict = None, headers: dict = None
    ):
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, json=data, headers=headers).json()
        if "access_token" in response.keys() or response["rt_cd"] == "0":
            return response
        else:
            raise Exception(response)

    def post(self, endpoint: str, data: dict = None, headers: dict = None):
        return self.post_with_error_handling(endpoint, data, headers)

    def get_hashkey(self, data) -> str:
        headers = {"appKey": self.key, "appSecret": self.secret}
        endpoint = "/uapi/hashkey"
        url = f"{self.base_url}{endpoint}"
        return self.session.post(url, json=data, headers=headers).json()["HASH"]

    def open_auth(self):
        return self.open_json("auth.json")

    def write_auth(self, auth):
        self.write_json("auth.json", auth)

    def check_auth(self, auth, key, secret, kis_number):
        if auth is None:
            return False
        access_token, access_token_token_expired = auth
        try:
            if access_token == "nothing":
                return False
            else:
                if not self.is_auth:
                    response = self.session.get(
                        "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-ccnl",
                        headers={
                            "authorization": f"BEARER {access_token}",
                            "appkey": key,
                            "appsecret": secret,
                            "custtype": "P",
                            "tr_id": "FHKST01010300",
                        },
                        params={
                            "FID_COND_MRKT_DIV_CODE": "J",
                            "FID_INPUT_ISCD": "005930",
                        },
                    ).json()
                    if response["msg_cd"] == "EGW00123":
                        return False

            access_token_token_expired = datetime.strptime(
                access_token_token_expired, "%Y-%m-%d %H:%M:%S"
            )
            diff = access_token_token_expired - datetime.now()
            total_seconds = diff.total_seconds()
            if total_seconds < 60 * 60:
                return False
            else:
                return True

        except Exception as e:
            print(traceback.format_exc())

    def create_auth(self, key: str, secret: str):
        data = {"grant_type": "client_credentials", "appkey": key, "appsecret": secret}
        base_url = BaseUrls.base_url.value
        endpoint = "/oauth2/tokenP"

        url = f"{base_url}{endpoint}"

        response = self.session.post(url, json=data).json()
        if "access_token" in response.keys() or response.get("rt_cd") == "0":
            return response["access_token"], response["access_token_token_expired"]
        else:
            raise Exception(response)

    def auth(self):
        auth_id = f"KIS{self.kis_number}"
        auth = db.get_auth(auth_id)
        if not self.check_auth(auth, self.key, self.secret, self.kis_number):
            auth = self.create_auth(self.key, self.secret)
            db.set_auth(auth_id, auth[0], auth[1])
        else:
            self.is_auth = True
        access_token = auth[0]
        self.base_headers = BaseHeaders(
            authorization=f"Bearer {access_token}",
            appkey=self.key,
            appsecret=self.secret,
            custtype="P",
        ).dict()
        return auth

    # 새로운 주문을 큐에 추가하고 처리
    def handle_order(self, ticker: str, amount: int):
        """주문을 큐에 추가하고 처리"""
        order_queue.put((ticker, amount, self.kis_number))
        if not self.order_processing:
            self.process_next_order()

    def process_next_order(self):
        """큐에서 다음 주문을 처리"""
        if self.order_processing:
            return

        while not order_queue.empty():
            self.order_processing = True
            ticker, amount, kis_number = order_queue.get()

            # kis_number == 1일 때만 특별 주문 처리
            if kis_number == 1:
                asyncio.run(self.execute_special_order(ticker, amount))
            else:
                # 기본 주문 처리
                print(f"기본 주문 처리 중: {ticker}, 수량: {amount}")
                self.create_market_buy_order("KRX", ticker, amount)

        self.order_processing = False

    # 특별 주문 처리 로직 (kis_number == 1일 때만 실행)
    async def execute_special_order(self, ticker: str, amount: int):
        """특별 주문 처리: kis_number == 1일 때만 주식이 있으면 매도 후 매수"""
        try:
            if self.kis_number != 1:
                print("특별 주문은 kis_number == 1일 때만 실행됩니다.")
                return

            if self.order_processing:
                print("이미 주문이 진행 중입니다.")
                return

            self.order_processing = True

            # 1. 계좌에 주식이 있는지 확인
            stock_info = self.account_has_stocks(self.account_number, self.base_order_body.ACNT_PRDT_CD)

            # 2. 주식이 계좌에 없으면 바로 매수 주문 실행
            if not stock_info:
                print("계좌에 주식이 없습니다. 새로운 매수 주문을 실행합니다.")
                self.create_market_buy_order("KRX", ticker, amount)
            else:
                # 3. 주식이 있으면 전량 시장가 매도
                print("계좌에 주식이 존재합니다. 전량 매도 처리 중...")
                for stock in stock_info:
                    current_ticker = stock["pdno"]
                    stock_amount = stock["hldg_qty"]
                    if stock_amount > 0:
                        self.create_market_sell_order("KRX", current_ticker, stock_amount)
                    else:
                        print(f"매도할 주식이 없습니다: {current_ticker}")

                # 4. 1초마다 계좌 상태를 확인하여 주식이 모두 매도되었는지 확인 (최대 20초)
                for _ in range(20):
                    stock_info = self.account_has_stocks(self.account_number, self.base_order_body.ACNT_PRDT_CD)
                    if not stock_info:
                        print("주식이 모두 매도되었습니다. 새로운 매수 주문을 실행합니다.")
                        self.create_market_buy_order("KRX", ticker, amount)
                        break
                    await asyncio.sleep(1)
                else:
                    print("20초 동안 주식이 매도되지 않았습니다. 다음 주문으로 넘어갑니다.")

            # 5. 1초마다 매수 주문 완료 여부 확인 (최대 20초)
            for _ in range(20):
                if self.is_order_settled():
                    print("매수 주문이 완료되었습니다.")
                    break
                await asyncio.sleep(1)
            else:
                print("20초 동안 매수 주문이 완료되지 않았습니다. 다음 주문으로 넘어갑니다.")

        except Exception as e:
            print(f"특별 주문 처리 중 오류 발생: {str(e)}")
            traceback.print_exc()
        finally:
            self.order_processing = False  # 주문 처리 완료 후 초기화

    def is_order_settled(self):
        """계좌 정보를 조회하여 주문 체결 여부 확인"""
        try:
            stock_info = self.account_has_stocks(self.account_number, self.base_order_body.ACNT_PRDT_CD)
            if stock_info:
                print("주문이 체결되었습니다.")
                return True
            else:
                print("아직 체결되지 않았습니다.")
                return False
        except Exception as e:
            print(f"계좌 정보를 조회하는 중 오류 발생: {str(e)}")
            return False

    def account_has_stocks(self, account_number: str, account_code: str) -> list:
        """계좌에 주식이 존재하는지 확인"""
        endpoint = "/uapi/domestic-stock/v1/trading/inquire-balance"
        url = f"{self.base_url}{endpoint}"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.key,
            "appsecret": self.secret,
            "tr_id": "VTTC8434R" if self.kis_number == 4 else "TTTC8434R",
        }
        params = {
            "CANO": account_number,
            "ACNT_PRDT_CD": account_code,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }

        response = self.session.get(url, headers=headers, params=params).json()

        if response["rt_cd"] == "0":
            return response["output1"]
        else:
            raise Exception(f"Error: {response['msg1']}")

    def create_market_buy_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        amount: int,
        price: int = 0,
    ):
        if exchange == "KRX":
            return self.create_order(exchange, ticker, "market", "buy", amount)
        elif exchange == "usa":
            return self.create_order(exchange, ticker, "market", "buy", amount, price)

    def create_market_sell_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        amount: int,
        price: int = 0,
    ):
        if exchange == "KRX":
            return self.create_order(exchange, ticker, "market", "sell", amount)
        elif exchange == "usa":
            return self.create_order(exchange, ticker, "market", "sell", amount, price)

    @validate_arguments
    def create_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        order_type: Literal["limit", "market"],
        side: Literal["buy", "sell"],
        amount: int,
        price: int = 0,
        mintick=0.01,
    ):
        endpoint = (
            Endpoints.korea_order.value
            if exchange == "KRX"
            else Endpoints.usa_order.value
        )
        body = self.base_order_body.dict()
        headers = copy.deepcopy(self.base_headers)
        price = str(price)

        amount = str(int(amount))

        if exchange == "KRX":
            if self.base_url == BaseUrls.base_url:
                headers |= (
                    KoreaBuyOrderHeaders(**headers)
                    if side == "buy"
                    else KoreaSellOrderHeaders(**headers)
                )
            elif self.base_url == BaseUrls.paper_base_url:
                headers |= (
                    KoreaPaperBuyOrderHeaders(**headers)
                    if side == "buy"
                    else KoreaPaperSellOrderHeaders(**headers)
                )

            if order_type == "market":
                body |= KoreaMarketOrderBody(**body, PDNO=ticker, ORD_QTY=amount)
            elif order_type == "limit":
                body |= KoreaOrderBody(
                    **body,
                    PDNO=ticker,
                    ORD_DVSN=KoreaOrderType.limit,
                    ORD_QTY=amount,
                    ORD_UNPR=price,
                )
        elif exchange in ("NASDAQ", "NYSE", "AMEX"):
            exchange_code = self.order_exchange_code.get(exchange)
            current_price = self.fetch_current_price(exchange, ticker)
            price = (
                current_price + mintick * 50
                if side == "buy"
                else current_price - mintick * 50
            )
            if price < 1:
                price = 1.0
            price = float("{:.2f}".format(price))
            if self.base_url == BaseUrls.base_url:
                headers |= (
                    UsaBuyOrderHeaders(**headers)
                    if side == "buy"
                    else UsaSellOrderHeaders(**headers)
                )
            elif self.base_url == BaseUrls.paper_base_url:
                headers |= (
                    UsaPaperBuyOrderHeaders(**headers)
                    if side == "buy"
                    else UsaPaperSellOrderHeaders(**headers)
                )

            if order_type == "market":
                body |= UsaOrderBody(
                    **body,
                    PDNO=ticker,
                    ORD_DVSN=UsaOrderType.limit.value,
                    ORD_QTY=amount,
                    OVRS_ORD_UNPR=price,
                    OVRS_EXCG_CD=exchange_code,
                )
            elif order_type == "limit":
                body |= UsaOrderBody(
                    **body,
                    PDNO=ticker,
                    ORD_DVSN=UsaOrderType.limit.value,
                    ORD_QTY=amount,
                    OVRS_ORD_UNPR=price,
                    OVRS_EXCG_CD=exchange_code,
                )
        return self.post(endpoint, body, headers)

if __name__ == "__main__":
    pass

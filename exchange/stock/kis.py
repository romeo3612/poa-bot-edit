from queue import Queue
import time
from datetime import datetime
import json
import httpx
from exchange.stock.error import TokenExpired
from exchange.stock.schemas import *
from exchange.database import db
from pydantic import validate_arguments
import traceback
import copy
from exchange.model import MarketOrder
import asyncio  # 비동기 처리를 위해 추가

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
        self.kis_number = kis_number  # kis_number 그대로 사용

        self.base_url = (
            BaseUrls.base_url.value
            if self.kis_number != 4
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

        self.order_processing = False  # 주문 처리 중인지 여부 확인
        self.order_in_progress = False  # 중복 주문 방지

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

    def account_has_stocks(self, account_number: str, account_code: str) -> list:
        """계좌에 주식이 존재하는지 확인하는 함수"""
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

    # 주문 체결 여부 확인
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

    # 비동기 처리로 1초마다 확인 (최대 20초)
    async def wait_for_order_settlement(self, timeout=20):
        """비동기 방식으로 1초마다 주문 완료 여부 확인"""
        for _ in range(timeout):
            if self.is_order_settled():
                return True
            await asyncio.sleep(1)  # 비동기 대기
        return False

    # 새로운 주문 처리 로직: 주문을 큐에 추가하고 순차적으로 처리
    def handle_market_buy_order(self, ticker: str, amount: int):
        """주문을 큐에 추가하고 순차적으로 처리"""
        # 주문을 큐에 추가
        order_queue.put((ticker, amount, self.kis_number))

        # 현재 주문이 처리 중이지 않다면, 다음 주문 처리 시작
        if not self.order_processing:
            self.process_next_order()

    def process_next_order(self):
        """큐에서 다음 주문을 처리"""
        if self.order_in_progress:
            print("이미 주문이 진행 중입니다.")
            return

        while not order_queue.empty():
            self.order_processing = True  # 주문 처리를 시작
            ticker, amount, kis_number = order_queue.get()

            # 특별 주문 처리 분기
            if kis_number == 1:
                print(f"특별 주문 처리 중 (kis_number: {kis_number})")
                asyncio.run(self.execute_special_order(ticker, amount))
            else:
                print("기본 주문 처리 중")
                self.execute_order(ticker, amount)

        self.order_processing = False

    def execute_order(self, ticker: str, amount: int):
        """실제 주문을 실행하는 기본 로직"""
        print(f"매수 주문 처리: {ticker}, 수량: {amount}")
        # 여기서 실제 매수 주문 실행 로직을 추가하면 됩니다.

    async def execute_special_order(self, ticker: str, amount: int):
        """특별 매수 주문 처리 로직"""
        try:
            if self.order_in_progress:
                print("이미 주문이 진행 중입니다.")
                return
            self.order_in_progress = True  # 주문 중복 방지

            # 1. 계좌 확인
            stock_info = self.account_has_stocks(self.account_number, self.base_order_body.ACNT_PRDT_CD)

            # 2. 주식이 계좌에 존재하지 않으면 새로운 매수 주문 실행
            if not stock_info:
                print("계좌에 주식이 존재하지 않습니다. 새로운 매수 주문을 실행합니다.")
                self.execute_order(ticker, amount)
            else:
                # 3. 주식이 존재하면 전량 시장가 매도
                print("계좌에 주식이 존재합니다. 전량 시장가 매도 주문을 실행합니다.")
                for stock in stock_info:
                    current_ticker = stock["pdno"]  # 주식 코드
                    stock_amount = stock["hldg_qty"]  # 보유 주식 수량
                    if stock_amount > 0:
                        self.create_market_sell_order("KRX", current_ticker, stock_amount)
                    else:
                        print(f"매도할 주식이 없습니다: {current_ticker}")

                # 4. 계좌 상태를 1초마다 확인하여 주식이 전량 매도 되었는지 확인
                settled = await self.wait_for_order_settlement(20)
                if not settled:
                    print("20초가 지나도 매도가 완료되지 않았습니다. 다음 주문으로 넘어갑니다.")
                else:
                    print("전량 매도 완료. 새로운 매수 주문을 실행합니다.")
                    self.execute_order(ticker, amount)

            # 5. 새로운 매수 주문이 완료되었는지 확인 (1초마다 최대 20초 동안)
            settled = await self.wait_for_order_settlement(20)
            if settled:
                print("매수 주문이 완료되었습니다.")
            else:
                print("20초가 지나도 매수 주문이 완료되지 않았습니다. 다음 주문으로 넘어갑니다.")

        except Exception as e:
            print(f"특별 주문 처리 중 오류 발생: {str(e)}")
            traceback.print_exc()
        finally:
            self.order_in_progress = False  # 주문 처리 완료 후 상태 초기화

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

    def fetch_ticker(
        self, exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"], ticker: str
    ):
        if exchange == "KRX":
            endpoint = Endpoints.korea_ticker.value
            headers = KoreaTickerHeaders(**self.base_headers).dict()
            query = KoreaTickerQuery(FID_INPUT_ISCD=ticker).dict()
        elif exchange in ("NASDAQ", "NYSE", "AMEX"):
            exchange_code = self.query_exchange_code.get(exchange)
            endpoint = Endpoints.usa_ticker.value
            headers = UsaTickerHeaders(**self.base_headers).dict()
            query = UsaTickerQuery(EXCD=exchange_code, SYMB=ticker).dict()
        ticker = self.get(endpoint, query, headers)
        return ticker.get("output")

    def fetch_current_price(self, exchange, ticker: str):
        try:
            if exchange == "KRX":
                return float(self.fetch_ticker(exchange, ticker)["stck_prpr"])
            elif exchange in ("NASDAQ", "NYSE", "AMEX"):
                return float(self.fetch_ticker(exchange, ticker)["last"])

        except KeyError:
            print(traceback.format_exc())
            return None

    def open_json(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def write_json(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f)


if __name__ == "__main__":
    pass

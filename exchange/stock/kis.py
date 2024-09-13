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
from devtools import debug


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
        # kis_number == 1일 때 특별 로직 수행
        if self.kis_number == 1 and exchange == "KRX":
            print(f"KIS 번호가 1입니다. 특별 로직을 수행합니다. Ticker: {ticker}, Amount: {amount}")

            # 잔고 조회 수행
            balance = self.fetch_balance()

            # 잔고가 있는 경우 모든 주식을 매도
            if balance and balance["output1"]:
                print("잔고가 있습니다. 보유 주식을 매도합니다.")
                for stock in balance["output1"]:
                    ticker_to_sell = stock["pdno"]
                    amount_to_sell = int(stock["ord_psbl_qty"])  # 주문 가능 수량
                    if amount_to_sell > 0:
                        print(f"{ticker_to_sell} 주식 {amount_to_sell}개 매도 주문을 실행합니다.")
                        self.create_market_sell_order(
                            exchange="KRX", ticker=ticker_to_sell, amount=amount_to_sell
                        )

                # 잔고 매도 후 매수 주문 실행
                print(f"잔고 매도 후 {ticker} 주식 {amount}개 매수 주문을 실행합니다.")
                return self.create_order(exchange, ticker, "market", "buy", amount)
            else:
                # 잔고가 없을 경우 바로 매수 주문
                print(f"잔고가 없습니다. 바로 {ticker} 주식 {amount}개 매수 주문을 실행합니다.")
                return self.create_order(exchange, ticker, "market", "buy", amount)

        # kis_number가 1이 아닌 경우 기존 로직 수행
        print(f"KIS 번호가 1이 아닙니다. 기존 로직을 수행합니다. Ticker: {ticker}, Amount: {amount}")
        return self.create_order(exchange, ticker, "market", "buy", amount)

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
            return self.create_order(exchange, ticker, "market", "buy", amount, price)

    def create_korea_market_buy_order(self, ticker: str, amount: int):
        return self.create_market_buy_order("KRX", ticker, amount)

    def create_korea_market_sell_order(self, ticker: str, amount: int):
        return self.create_market_sell_order("KRX", ticker, amount)

    def create_usa_market_buy_order(self, ticker: str, amount: int, price: int):
        return self.create_market_buy_order("usa", ticker, amount, price)

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

    def fetch_balance(self):
        endpoint = Endpoints.korea_balance.value
        params = {
            "CANO": self.account_number,
            "ACNT_PRDT_CD": self.base_order_body.ACNT_PRDT_CD,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "01",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
        return self.get(endpoint, params=params, headers=self.base_headers)

    def open_json(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def write_json(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f)


if __name__ == "__main__":
    pass

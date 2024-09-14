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
        # headers |= self.base_headers
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
            if self.kis_number == 1 and exchange == "KRX":
                return self.handle_special_order(
                    exchange, ticker, order_type, side, amount, price
                )
            else:
                return self.process_order(
                    exchange, ticker, order_type, side, amount, price, mintick
                )

    def handle_special_order(self, exchange, ticker, order_type, side, amount, price):
        print(f"KIS 특별 주문 처리: {self.kis_number}")
        # 잔고 조회를 수행
        balance_response = self.fetch_balance()

        # 잔고 조회 응답 확인
        if balance_response and balance_response["rt_cd"] == "0":
            holdings = balance_response.get("output1", [])
            if not holdings:
                print("잔고에 보유한 종목이 없습니다. 매수 주문을 실행합니다.")
                # 매수 주문 실행
                order_response = self.process_order(exchange, ticker, order_type, side, amount, price)
                print("매수 주문 응답:", order_response)
                return order_response
            else:
                print("잔고에 보유한 종목이 있습니다. 전량 매도 후 매수 주문을 실행합니다.")
                # 보유한 종목들을 전량 매도
                for holding in holdings:
                    sell_ticker = holding["pdno"]
                    sell_amount = holding["hldg_qty"]
                    if int(sell_amount) > 0:
                        print(f"종목 {sell_ticker}를 {sell_amount}주 매도합니다.")
                        # 시장가 매도 주문 실행
                        sell_response = self.create_korea_market_sell_order(sell_ticker, int(sell_amount))
                        print(f"매도 주문 응답 ({sell_ticker}):", sell_response)
                        # 매도 주문 응답 확인
                        if sell_response["rt_cd"] != "0":
                            print(f"매도 주문 실패: {sell_response['msg1']}")
                            continue
                # 매도 주문 체결 대기 또는 확인 로직 추가 가능

                # 매수 주문 실행
                order_response = self.process_order(exchange, ticker, order_type, side, amount, price)
                print("매수 주문 응답:", order_response)
                return order_response
        else:
            print("잔고 조회 실패 또는 응답 오류.")
            return None


    @validate_arguments
    def fetch_balance(self):
        try:
            endpoint = Endpoints.korea_balance.value
            headers = copy.deepcopy(self.base_headers)
            headers["tr_id"] = TransactionId.korea_balance.value  # 한국 주식 잔고 조회 트랜잭션 ID

            # 요청 본문 설정
            request_body = KoreaStockBalanceRequest(
                CANO=self.account_number,
                ACNT_PRDT_CD=self.base_order_body.ACNT_PRDT_CD,
                AFHR_FLPR_YN="N",  # 시간외단일가여부는 기본값 'N' 설정
                INQR_DVSN="02",  # 조회구분은 종목별로 설정
                UNPR_DVSN="01",  # 단가는 기본값 '01'
                FUND_STTL_ICLD_YN="N",  # 펀드 결제 포함 여부는 기본값 'N'
                FNCG_AMT_AUTO_RDPT_YN="N",  # 융자금액 자동 상환 여부 기본값 'N'
                PRCS_DVSN="00",  # 전일 매매 포함
            ).dict()

            # API 호출
            response = self.post(endpoint, data=request_body, headers=headers)

            # 응답 출력
            if response["rt_cd"] == "0":
                print("잔고 조회 성공")
                print(json.dumps(response, indent=2, ensure_ascii=False))  # 응답 JSON 포맷으로 출력
            else:
                print(f"잔고 조회 실패: {response['msg1']}")

            return response

        except Exception as e:
            print(f"잔고 조회 중 오류 발생: {str(e)}")
            return None


    @validate_arguments
    def process_order(
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

    def open_json(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def write_json(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f)


if __name__ == "__main__":
    pass

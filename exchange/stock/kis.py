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
from pydantic import ValidationError  # kis.py 파일에 추가
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
        elif exchange in ("NASDAQ", "NYSE", "AMEX"):
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
        elif exchange in ("NASDAQ", "NYSE", "AMEX"):
            return self.create_order(exchange, ticker, "market", "sell", amount, price)

    def create_korea_market_buy_order(self, ticker: str, amount: int):
        return self.create_market_buy_order("KRX", ticker, amount)

    def create_korea_market_sell_order(self, ticker: str, amount: int):
        return self.create_market_sell_order("KRX", ticker, amount)

    def create_usa_market_buy_order(self, ticker: str, amount: int, price: int):
        return self.create_market_buy_order("NASDAQ", ticker, amount, price)  # 예시로 NASDAQ 사용

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

    @validate_arguments
    def korea_fetch_balance(self):
        try:
            endpoint = Endpoints.korea_balance.value
            headers = copy.deepcopy(self.base_headers)
            headers["tr_id"] = TransactionId.korea_balance.value  # 'TTTC8434R'

            # 요청 파라미터 설정
            request_params = KoreaStockBalanceRequest(
                CANO=self.account_number,  # 8자리 계좌번호
                ACNT_PRDT_CD=self.base_order_body.ACNT_PRDT_CD,  # 2자리 계좌상품코드
                AFHR_FLPR_YN="N",  # 시간외단일가여부
                OFL_YN="",  # 오프라인 여부
                INQR_DVSN="02",  # 조회구분: 종목별
                UNPR_DVSN="01",  # 단가구분
                FUND_STTL_ICLD_YN="N",  # 펀드결제분포함여부
                FNCG_AMT_AUTO_RDPT_YN="N",  # 융자금액자동상환여부
                PRCS_DVSN="00",  # 처리구분: 전일매매포함
                CTX_AREA_FK100="",  # 연속조회검색조건100
                CTX_AREA_NK100="",  # 연속조회키100
            ).dict()

            # 디버깅: 잔고 조회 요청 파라미터 출력
            print("잔고 조회 요청 파라미터:")
            print(json.dumps(request_params, indent=2, ensure_ascii=False))

            # API 호출 (GET 요청)
            response = self.get(endpoint, params=request_params, headers=headers)

            # 디버깅: 잔고 조회 응답 출력
            print("잔고 조회 응답:")
            print(json.dumps(response, indent=2, ensure_ascii=False))

            if response["rt_cd"] == "0":
                print("잔고 조회 성공")
                return KoreaStockBalanceResponse(**response)
            else:
                print(f"잔고 조회 실패: {response['msg1']}")
                return None

        except Exception as e:
            print(f"잔고 조회 중 오류 발생: {str(e)}")
            return None  # 예외 발생 시 None 반환
        


    @validate_arguments
    def usa_fetch_balance(self):
        try:
            endpoint = Endpoints.usa_balance.value
            headers = copy.deepcopy(self.base_headers)
            headers["tr_id"] = TransactionId.usa_balance.value  # 'TTTS3012R'

            # 요청 파라미터 설정
            request_params = UsaStockBalanceRequest(
                CANO=self.account_number,  # 8자리 계좌번호
                ACNT_PRDT_CD=self.base_order_body.ACNT_PRDT_CD,  # 2자리 계좌상품코드
                OVRS_EXCG_CD="NASD",  # 매핑된 해외 거래소 코드
                TR_CRCY_CD="USD",     # 거래 통화 코드
                CTX_AREA_FK200="",    # 연속조회 검색조건200
                CTX_AREA_NK200="",    # 연속조회 키200
            ).dict()

            # 디버깅: 해외 잔고 조회 요청 파라미터 출력
            print("해외 잔고 조회 요청 파라미터:")
            print(json.dumps(request_params, indent=2, ensure_ascii=False))

            # API 호출 (GET 요청)
            response = self.get(endpoint, params=request_params, headers=headers)

            # 디버깅: 해외 잔고 조회 응답 출력
            print("해외 잔고 조회 응답:")
            print(json.dumps(response, indent=2, ensure_ascii=False))

            if response.get("rt_cd") == "0":
                print("해외 잔고 조회 성공")

                # `output1` 리스트에서 모든 필드가 빈 문자열인 항목을 필터링
                filtered_output1 = [
                    item for item in response.get("output1", [])
                    if any(value not in ("", "0", "0.0000", "0.00000000", "0.000000") for value in item.values())
                ]
                response["output1"] = filtered_output1

                try:
                    usa_balance = UsaStockBalanceResponse(**response)
                    print(f"Parsed USA Balance: {usa_balance}")
                    return usa_balance
                except ValidationError as ve:
                    print(f"해외 잔고 조회 중 Pydantic 검증 오류 발생: {ve}")
                    return None
            else:
                print(f"해외 잔고 조회 실패: {response.get('msg1', '알 수 없는 오류')}")
                return None

        except ValidationError as ve:
            print(f"해외 잔고 조회 중 유효성 검사 오류 발생: {ve.errors()}")
            return None
        except Exception as e:
            print(f"해외 잔고 조회 중 오류 발생: {str(e)}")
            print(traceback.format_exc())
            return None  # 예외 발생 시 None 반환
      
        
    @validate_arguments
    def fetch_balance_and_price(self, exchange_name: str, fetch_ticker: str):
        try:
            # KRX 거래소 처리
            if exchange_name == "KRX":
                balance = self.korea_fetch_balance()
                if balance is None:
                    raise ValueError("KRX 잔고 조회 실패")

                holding = next((item for item in balance.output1 if item.pdno == fetch_ticker), None)
                holding_qty = int(holding.hldg_qty) if holding else 0
                holding_price = float(holding.prpr) if holding else 0.0  # 가격 조회

            # 미국 거래소 처리
            elif exchange_name in ["NASDAQ", "NYSE", "AMEX"]:
                balance = self.usa_fetch_balance()
                if balance is None:
                    raise ValueError(f"{exchange_name} 잔고 조회 실패")

                holding = next((item for item in balance.output1 if item.ovrs_pdno == fetch_ticker), None)
                holding_qty = int(holding.ovrs_cblc_qty) if holding else 0
                holding_price = float(holding.now_pric2) if holding else 0.0  # 가격 조회

            # 지원하지 않는 거래소 처리
            else:
                raise ValueError(f"지원하지 않는 거래소: {exchange_name}")

            # 보유 수량과 페어 가격 반환
            return holding_qty, holding_price

        except Exception as e:
            print(f"잔고 조회 중 오류 발생: {str(e)}")
            return 0, 0.0  # 오류 발생 시 기본값 반환

if __name__ == "__main__":
    pass

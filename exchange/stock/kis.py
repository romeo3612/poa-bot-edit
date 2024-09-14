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
from typing import Literal
import threading  # threading 모듈 추가


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

        # 동시성 문제 해결을 위한 락 초기화
        self._lock = threading.Lock()

    def init_info(self, order_info: MarketOrder):
        self.order_info = order_info

    def close_session(self):
        self.session.close()

    # GET 요청에 대한 예외 처리를 위한 메서드 추가
    def get_with_error_handling(self, endpoint: str, params: dict = None, headers: dict = None):
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, params=params, headers=headers)
            response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
            response_data = response.json()
            if response_data.get("rt_cd") == "0":
                return response_data
            else:
                print(f"API 오류 발생: {response_data.get('msg1')}")
                return response_data
        except httpx.HTTPStatusError as http_err:
            print(f"HTTP 오류 발생: {str(http_err)}")
            return {"rt_cd": "99", "msg1": str(http_err)}
        except Exception as e:
            print(f"예외 발생: {str(e)}")
            return {"rt_cd": "99", "msg1": str(e)}

    def get(self, endpoint: str, params: dict = None, headers: dict = None):
        return self.get_with_error_handling(endpoint, params, headers)

    def post_with_error_handling(
        self, endpoint: str, data: dict = None, headers: dict = None
    ):
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.post(url, json=data, headers=headers)
            response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
            response_data = response.json()
            if "access_token" in response_data.keys() or response_data.get("rt_cd") == "0":
                return response_data
            else:
                print(f"API 오류 발생: {response_data.get('msg1')}")
                return response_data
        except httpx.HTTPStatusError as http_err:
            print(f"HTTP 오류 발생: {str(http_err)}")
            return {"rt_cd": "99", "msg1": str(http_err)}
        except Exception as e:
            print(f"예외 발생: {str(e)}")
            return {"rt_cd": "99", "msg1": str(e)}

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
            return False

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
        mintick: float = 0.01,
    ):
        try:
            # 디버깅: kis_number와 exchange를 확인
            print(
                f"create_order 호출: kis_number={self.kis_number}, exchange={exchange}, ticker={ticker}"
            )

            if self.kis_number == 1 and exchange == "KRX":
                # KIS 특별 주문 분기로 들어가는지 확인
                print(f"KIS 특별 주문 처리: kis_number={self.kis_number}, 종목: {ticker}")
                return self.handle_special_order(
                    exchange, ticker, order_type, side, amount, price
                )
            else:
                # 일반 주문 처리
                print(f"일반 주문 처리: kis_number={self.kis_number}, 종목: {ticker}")
                return self.process_order(
                    exchange, ticker, order_type, side, amount, price, mintick
                )
        except Exception as e:
            print(f"create_order 중 예외 발생: {str(e)}")
            return {"rt_cd": "99", "msg1": str(e)}

    @validate_arguments
    def handle_special_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        order_type: Literal["limit", "market"],
        side: Literal["buy", "sell"],
        amount: int,
        price: int = 0,
    ):
        with self._lock:
            try:
                print("락 획득: 주문 처리 시작")

                # 잔고 조회를 수행
                balance_response = self.fetch_balance()

                # 잔고 조회 응답 확인
                if balance_response and balance_response["rt_cd"] == "0":
                    # 보유 수량이 0인 종목은 제외
                    holdings = [
                        holding
                        for holding in balance_response.get("output1", [])
                        if int(holding.get("hldg_qty", 0)) > 0
                    ]
                    if not holdings:
                        print("잔고에 보유한 종목이 없습니다. 매수 주문을 실행합니다.")
                        # 매수 주문 실행
                        order_response = self.process_order(
                            exchange, ticker, order_type, side, amount, price
                        )
                        print("매수 주문 응답:", order_response)
                        return order_response
                    else:
                        print("잔고에 보유한 종목이 있습니다. 전량 매도 후 매수 주문을 실행합니다.")
                        # 보유한 종목들을 전량 매도
                        for holding in holdings:
                            sell_ticker = holding["pdno"]
                            sell_amount = holding["hldg_qty"]
                            print(f"종목 {sell_ticker}를 {sell_amount}주 매도합니다.")
                            # 시장가 매도 주문 실행
                            sell_response = self.create_korea_market_sell_order(
                                sell_ticker, int(sell_amount)
                            )
                            print(f"매도 주문 응답 ({sell_ticker}):", sell_response)
                            # 매도 주문 응답 확인
                            if sell_response.get("rt_cd") != "0":
                                print(f"매도 주문 실패: {sell_response.get('msg1')}")
                                continue

                        # 매도 주문 후 잔고 재확인
                        import time

                        max_attempts = 5
                        for attempt in range(max_attempts):
                            print(f"{attempt+1}번째 잔고 재조회 중...")
                            time.sleep(1)  # 1초 대기
                            balance_response = self.fetch_balance()
                            if balance_response and balance_response["rt_cd"] == "0":
                                holdings = [
                                    holding
                                    for holding in balance_response.get("output1", [])
                                    if int(holding.get("hldg_qty", 0)) > 0
                                ]
                                if not holdings:
                                    print("잔고에 보유한 종목이 없습니다. 매수 주문을 실행합니다.")
                                    # 매수 주문 실행
                                    order_response = self.process_order(
                                        exchange, ticker, order_type, side, amount, price
                                    )
                                    print("매수 주문 응답:", order_response)
                                    return order_response
                                else:
                                    print("잔고에 아직 종목이 남아있습니다.")
                            else:
                                print("잔고 조회 실패 또는 응답 오류.")

                        print("최대 재시도 횟수를 초과하여 매수 주문을 실행하지 않습니다.")
                        return None
                else:
                    print("잔고 조회 실패 또는 응답 오류.")
                    return None
            except Exception as e:
                print(f"handle_special_order 중 예외 발생: {str(e)}")
                return {"rt_cd": "99", "msg1": str(e)}
            finally:
                print("락 해제: 주문 처리 완료")

    @validate_arguments
    def fetch_balance(self):
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
            else:
                print(f"잔고 조회 실패: {response['msg1']}")

            return response

        except Exception as e:
            print(f"fetch_balance 중 예외 발생: {str(e)}")
            return {"rt_cd": "99", "msg1": str(e)}

    @validate_arguments
    def process_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        order_type: Literal["limit", "market"],
        side: Literal["buy", "sell"],
        amount: int,
        price: int = 0,
        mintick: float = 0.01,
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

        # 디버깅: 주문 요청 초기 헤더와 본문 출력
        print("주문 요청 초기 헤더:")
        headers_to_log = headers.copy()
        # 민감한 정보 마스킹
        headers_to_log["authorization"] = "***"
        headers_to_log["appkey"] = "***"
        headers_to_log["appsecret"] = "***"
        print(json.dumps(headers_to_log, indent=2, ensure_ascii=False))
        print("주문 요청 초기 본문:")
        print(json.dumps(body, indent=2, ensure_ascii=False))

        if exchange == "KRX":
            if self.base_url == BaseUrls.base_url.value:
                headers |= (
                    KoreaBuyOrderHeaders(**headers)
                    if side == "buy"
                    else KoreaSellOrderHeaders(**headers)
                )
            elif self.base_url == BaseUrls.paper_base_url.value:
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
                    ORD_DVSN=KoreaOrderType.limit.value,
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
            if self.base_url == BaseUrls.base_url.value:
                headers |= (
                    UsaBuyOrderHeaders(**headers)
                    if side == "buy"
                    else UsaSellOrderHeaders(**headers)
                )
            elif self.base_url == BaseUrls.paper_base_url.value:
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

        # 디버깅: 최종 주문 요청 헤더와 본문 출력
        print("최종 주문 요청 헤더:")
        headers_to_log = headers.copy()
        # 민감한 정보 마스킹
        headers_to_log["authorization"] = "***"
        headers_to_log["appkey"] = "***"
        headers_to_log["appsecret"] = "***"
        print(json.dumps(headers_to_log, indent=2, ensure_ascii=False))
        print("최종 주문 요청 본문:")
        print(json.dumps(body, indent=2, ensure_ascii=False))

        # 주문 요청
        response = self.post(endpoint, body, headers)

        # 디버깅: 주문 응답 출력
        print("주문 응답:")
        print(json.dumps(response, indent=2, ensure_ascii=False))

        return response

    @validate_arguments
    def create_market_buy_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        amount: int,
        price: int = 0,
    ):
        return self.create_order(exchange, ticker, "market", "buy", amount, price)

    @validate_arguments
    def create_market_sell_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        amount: int,
        price: int = 0,
    ):
        return self.create_order(exchange, ticker, "market", "sell", amount, price)

    def create_korea_market_buy_order(self, ticker: str, amount: int):
        return self.create_market_buy_order("KRX", ticker, amount)

    def create_korea_market_sell_order(self, ticker: str, amount: int):
        return self.create_market_sell_order("KRX", ticker, amount)

    def create_usa_market_buy_order(self, ticker: str, amount: int, price: int):
        return self.create_market_buy_order("USA", ticker, amount, price)

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
        ticker_data = self.get(endpoint, query, headers)
        return ticker_data.get("output")

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

from datetime import datetime
import json
import httpx
import traceback
import copy
from enum import Enum
from typing import Literal
from pydantic import BaseModel, validate_arguments, ValidationError
from exchange.stock.schemas import (
    BaseHeaders,
    Endpoints,
    TransactionId,
    KoreaStockBalanceRequest,
    KoreaStockBalanceResponse,
    UsaStockBalanceRequest,
    UsaStockBalanceResponse,
)
from exchange.database import db
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
        response = self.session.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    def post_with_error_handling(
        self, endpoint: str, data: dict = None, headers: dict = None
    ):
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, json=data, headers=headers)
        response.raise_for_status()
        resp_json = response.json()
        if "access_token" in resp_json.keys() or resp_json.get("rt_cd") == "0":
            return resp_json
        else:
            raise Exception(resp_json)

    def post(self, endpoint: str, data: dict = None, headers: dict = None):
        return self.post_with_error_handling(endpoint, data, headers)

    def get_hashkey(self, data) -> str:
        headers = {"appKey": self.key, "appSecret": self.secret}
        endpoint = "/uapi/hashkey"
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, json=data, headers=headers)
        response.raise_for_status()
        return response.json()["HASH"]

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
                            "authorization": f"Bearer {access_token}",
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

        response = self.session.post(url, json=data)
        response.raise_for_status()
        resp_json = response.json()
        if "access_token" in resp_json.keys() or resp_json.get("rt_cd") == "0":
            return resp_json["access_token"], resp_json["access_token_token_expired"]
        else:
            raise Exception(resp_json)

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
        # 기존 create_order 함수 내용 유지
        pass  # 실제 코드 유지

    def fetch_current_price(self, exchange, ticker: str):
        # 기존 fetch_current_price 함수 내용 유지
        pass  # 실제 코드 유지

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

            if response.get("rt_cd") == "0":
                print("잔고 조회 성공")
                return KoreaStockBalanceResponse(**response)
            else:
                print(f"잔고 조회 실패: {response.get('msg1', '알 수 없는 오류')}")
                return None

        except ValidationError as ve:
            print(f"잔고 조회 중 유효성 검사 오류 발생: {ve.errors()}")
            return None
        except Exception as e:
            print(f"잔고 조회 중 오류 발생: {str(e)}")
            print(traceback.format_exc())
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
                OVRS_EXCG_CD="NAS",  # 해외 거래소 코드 (올바른 값으로 수정: "NAS")
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
                return UsaStockBalanceResponse(**response)
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

if __name__ == "__main__":
    pass

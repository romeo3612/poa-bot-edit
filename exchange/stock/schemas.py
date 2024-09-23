from enum import Enum
from typing import Literal, List, Dict, Any, Optional
from pydantic import BaseModel, Field

korea_stocks = ("KRX")
us_stocks = ("NASDAQ", "NYSE", "AMEX")


class BaseUrls(str, Enum):
    base_url = "https://openapi.koreainvestment.com:9443"  # 실제 거래를 위한 기본 URL
    paper_base_url = "https://openapivts.koreainvestment.com:29443"  # 모의 거래를 위한 URL


class BaseHeaders(BaseModel):
    authorization: str  # 인증 토큰
    appkey: str         # 앱 키
    appsecret: str      # 앱 시크릿
    custtype: str = "P" # 고객 유형을 기본적으로 'P'로 설정 (개인 투자자)


class Endpoints(str, Enum):
    korea_order_base = "/uapi/domestic-stock/v1"
    korea_order = f"{korea_order_base}/trading/order-cash"  # 현금 주문
    korea_order_buyable = f"{korea_order_base}/trading/inquire-psbl-order"  # 주문 가능 여부 조회
    korea_balance = f"{korea_order_base}/trading/inquire-balance"  # 주식 잔고 조회 (국내)
    
    usa_order_base = "/uapi/overseas-stock/v1"
    usa_order = f"{usa_order_base}/trading/order"  # 현금 주문
    usa_order_buyable = f"{usa_order_base}/trading/inquire-psamount"  # 주문 가능 여부 조회
    usa_current_price = f"/uapi/overseas-price/v1/quotations/price"  # 미국 주식 현재 가격 조회
    usa_balance = f"{usa_order_base}/trading/inquire-balance"  # 미국 주식 잔고 조회

    korea_ticker = "/uapi/domestic-stock/v1/quotations/inquire-price"
    usa_ticker = "/uapi/overseas-price/v1/quotations/price"


class TransactionId(str, Enum):
    korea_buy = "TTTC0802U"  # 한국 주식 매수
    korea_sell = "TTTC0801U"  # 한국 주식 매도
    korea_balance = "TTTC8434R"  # 한국 주식 잔고 조회 (실전)
    korea_paper_balance = "VTTC8434R"  # 한국 주식 잔고 조회 (모의)

    korea_paper_buy = "VTTC0802U"  # 모의 매수
    korea_paper_sell = "VTTC0801U"  # 모의 매도
    korea_paper_cancel = "VTTC0803U"  # 모의 주문 취소

    usa_buy = "JTTT1002U"  # 미국 주식 매수
    usa_sell = "JTTT1006U"  # 미국 주식 매도
    usa_balance = "TTTS3012R"  # 미국 주식 잔고 조회 (실전)
    usa_balance_mock = "VTTS3012R"  # 미국 주식 잔고 조회 (모의)
 
    usa_paper_buy = "VTTT1002U"  # 모의 매수
    usa_paper_sell = "VTTT1001U"  # 모의 매도

    korea_ticker = "FHKST01010100"  # 한국 주식 티커 조회
    usa_ticker = "HHDFS00000300"    # 미국 주식 티커 조회


class KoreaTickerQuery(BaseModel):
    FID_COND_MRKT_DIV_CODE: str = "J"  # 한국 주식 시장 분류 코드
    FID_INPUT_ISCD: str                  # 종목 코드


class UsaTickerQuery(BaseModel):
    AUTH: str = ""                        # 인증 정보
    EXCD: Literal["NYS", "NAS", "AMS"]    # 거래소 코드 (뉴욕, 나스닥, 아멕스)
    SYMB: str                             # 종목 코드


class ExchangeCode(str, Enum):
    NYSE = "NYSE"    # 뉴욕 증권거래소
    NASDAQ = "NASD"  # 나스닥 증권거래소
    AMEX = "AMEX"    # 아멕스 증권거래소


class QueryExchangeCode(str, Enum):
    NYSE = "NYS"
    NASDAQ = "NAS"
    AMEX = "AMS"


class KoreaOrderType(str, Enum):
    market = "01"  # 시장가 주문
    limit = "00"   # 지정가 주문


class UsaOrderType(str, Enum):
    limit = "00"  # 지정가 주문


class OrderSide(str, Enum):
    buy = "buy"    # 매수
    sell = "sell"  # 매도


class TokenInfo(BaseModel):
    access_token: str                     # 액세스 토큰
    access_token_token_expired: str       # 토큰 만료 시간


class KoreaTickerHeaders(BaseHeaders):
    tr_id: str = TransactionId.korea_ticker.value  # 거래 ID를 티커 조회로 설정


class UsaTickerHeaders(BaseHeaders):
    tr_id: str = TransactionId.usa_ticker.value    # 거래 ID를 미국 티커 조회로 설정


class KoreaBuyOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.korea_buy.value      # 거래 ID를 매수로 설정


class KoreaSellOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.korea_sell.value     # 거래 ID를 매도로 설정


class KoreaPaperBuyOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.korea_paper_buy.value  # 모의 거래 ID를 매수로 설정


class KoreaPaperSellOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.korea_paper_sell.value  # 모의 거래 ID를 매도로 설정


class UsaBuyOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.usa_buy.value


class UsaSellOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.usa_sell.value


class UsaPaperBuyOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.usa_paper_buy.value


class UsaPaperSellOrderHeaders(BaseHeaders):
    tr_id: str = TransactionId.usa_paper_sell.value


class AccountInfo(BaseModel):
    CANO: str          # 계좌번호 앞 8자리
    ACNT_PRDT_CD: str  # 계좌상품코드 (2자리)


class OrderBody(BaseModel):
    PDNO: str      # 종목코드 6자리
    ORD_QTY: str   # 주문 수량


class KoreaOrderBody(OrderBody):
    ORD_DVSN: Literal[KoreaOrderType.market, KoreaOrderType.limit]  # 주문 형식 (시장가 또는 지정가)
    ORD_UNPR: str  # 주문 가격


class KoreaMarketOrderBody(KoreaOrderBody):
    ORD_DVSN: str = KoreaOrderType.market.value  # 주문 형식을 시장가로 고정
    ORD_UNPR: str = "0"                           # 시장가 주문이므로 가격은 0으로 설정


class UsaOrderBody(OrderBody):
    ORD_DVSN: str = UsaOrderType.limit.value     # 주문 형식은 지정가로 고정
    OVRS_ORD_UNPR: str                         # 주문 가격
    OVRS_EXCG_CD: Literal[ExchangeCode.NYSE, ExchangeCode.NASDAQ, ExchangeCode.AMEX]  # 거래소 코드 (NYS: NYSE, NAS: NASDAQ, AMS: AMEX)
    ORD_SVR_DVSN_CD: str = "0"                    # 서버 구분 코드 (기본값 0)

# 한국 주식 잔고 조회 요청 스키마 정의
class KoreaStockBalanceRequest(BaseModel):
    CANO: str                       # 종합계좌번호 (8자리)
    ACNT_PRDT_CD: str               # 계좌상품코드 (2자리)
    AFHR_FLPR_YN: Literal['N', 'Y'] # 시간외단일가여부
    OFL_YN: str = ""                # 오프라인여부 (기본값 공란)
    INQR_DVSN: Literal['01', '02']  # 조회구분 (01: 대출일별, 02: 종목별)
    UNPR_DVSN: Literal['01']        # 단가구분 (01: 기본값)
    FUND_STTL_ICLD_YN: Literal['N', 'Y'] # 펀드결제 포함 여부 (N: 기본값)
    FNCG_AMT_AUTO_RDPT_YN: Literal['N', 'Y'] # 융자금액자동상환여부 (N: 기본값)
    PRCS_DVSN: Literal['00', '01']  # 처리구분 (00: 전일매매포함, 01: 전일매매미포함)
    CTX_AREA_FK100: str = ""        # 연속조회 검색조건 (공란 시 최초 조회)
    CTX_AREA_NK100: str = ""        # 연속조회 키 (공란 시 최초 조회)

# 한국 주식 잔고 조회 응답 스키마 정의
class KoreaStockBalanceItem(BaseModel):
    pdno: str                      # 종목번호
    prdt_name: str                 # 종목명
    hldg_qty: int                  # 보유 수량
    ord_psbl_qty: int              # 주문 가능 수량
    prpr: float                    # 현재가
    evlu_amt: int                  # 평가 금액
   
    class Config:
        extra = "ignore"  # 정의되지 않은 필드는 무시

class KoreaStockBalanceResponse(BaseModel):
    output1: List[KoreaStockBalanceItem]        # 잔고 목록
    rt_cd: str                                 # 응답 코드
    msg_cd: str                                # 메시지 코드
    msg1: str                                  # 응답 메시지

    class Config:
        extra = "ignore"  # 정의되지 않은 필드는 무시

# --- 해외 주식 잔고 조회 스키마 추가 시작 ---

# 미국 주식 잔고 조회 요청 스키마 정의
class UsaStockBalanceRequest(BaseModel):
    CANO: str                       # 종합계좌번호 (8자리)
    ACNT_PRDT_CD: str               # 계좌상품코드 (2자리)
    OVRS_EXCG_CD: Literal["NYS", "NAS", "AMS", "NASD"]  # 해외 거래소 코드
    TR_CRCY_CD: Literal["USD"]      # 거래 통화 코드 (USD)
    CTX_AREA_FK200: str = ""        # 연속조회 검색조건200 (공란 시 최초 조회)
    CTX_AREA_NK200: str = ""        # 연속조회 키200 (공란 시 최초 조회)

# 미국 주식 잔고 조회 응답 항목을 정의하는 클래스입니다.
class UsaStockBalanceItem(BaseModel):
    cano: Optional[str] = Field(None, alias="cano")                           # 종목번호 (미국 티커)
    acnt_prdt_cd: Optional[str] = Field(None, alias="acnt_prdt_cd")           # 계좌상품코드 (2자리)
    prdt_type_cd: Optional[str] = Field(None, alias="prdt_type_cd")           # 상품 유형 코드
    ovrs_pdno: Optional[str] = Field(None, alias="ovrs_pdno")                 # 해외 종목 코드 (e.g., TSLA)
    ovrs_item_name: Optional[str] = Field(None, alias="ovrs_item_name")       # 해외 종목명 (e.g., Tesla)
    frcr_evlu_pfls_amt: Optional[str] = Field(None, alias="frcr_evlu_pfls_amt")  # 외화평가손익금액
    evlu_pfls_rt: Optional[str] = Field(None, alias="evlu_pfls_rt")           # 평가손익율
    pchs_avg_pric: Optional[str] = Field(None, alias="pchs_avg_pric")         # 매입평균가격
    ovrs_cblc_qty: Optional[str] = Field(None, alias="ovrs_cblc_qty")         # 해외잔고수량
    ord_psbl_qty: Optional[str] = Field(None, alias="ord_psbl_qty")           # 주문 가능 수량
    frcr_pchs_amt1: Optional[str] = Field(None, alias="frcr_pchs_amt1")       # 외화매입금액1
    ovrs_stck_evlu_amt: Optional[str] = Field(None, alias="ovrs_stck_evlu_amt")  # 해외주식평가금액
    now_pric2: Optional[str] = Field(None, alias="now_pric2")                 # 현재가격2
    tr_crcy_cd: Optional[str] = Field(None, alias="tr_crcy_cd")               # 거래통화코드
    ovrs_excg_cd: Optional[str] = Field(None, alias="ovrs_excg_cd")           # 해외거래소코드
    loan_type_cd: Optional[str] = Field(None, alias="loan_type_cd")           # 대출유형코드
    loan_dt: Optional[str] = Field(None, alias="loan_dt")                     # 대출일자
    expd_dt: Optional[str] = Field(None, alias="expd_dt")                     # 만기일자

    class Config:
        allow_population_by_field_name = True
        extra = "ignore"  # 정의되지 않은 필드는 무시

# 미국 주식 잔고 조회 응답 요약을 정의하는 클래스입니다.
class UsaStockBalanceSummary(BaseModel):
    frcr_pchs_amt1: Optional[str] = Field(None, alias="frcr_pchs_amt1")         # 외화매입금액1
    ovrs_rlzt_pfls_amt: Optional[str] = Field(None, alias="ovrs_rlzt_pfls_amt") # 해외실현손익금액
    ovrs_tot_pfls: Optional[str] = Field(None, alias="ovrs_tot_pfls")           # 해외총손익
    rlzt_erng_rt: Optional[str] = Field(None, alias="rlzt_erng_rt")             # 실현수익율
    tot_evlu_pfls_amt: Optional[str] = Field(None, alias="tot_evlu_pfls_amt")   # 총평가손익금액
    tot_pftrt: Optional[str] = Field(None, alias="tot_pftrt")                   # 총수익률
    frcr_buy_amt_smtl1: Optional[str] = Field(None, alias="frcr_buy_amt_smtl1") # 외화매수금액합계1
    ovrs_rlzt_pfls_amt2: Optional[str] = Field(None, alias="ovrs_rlzt_pfls_amt2") # 해외실현손익금액2
    frcr_buy_amt_smtl2: Optional[str] = Field(None, alias="frcr_buy_amt_smtl2") # 외화매수금액합계2

    class Config:
        allow_population_by_field_name = True
        extra = "ignore"  # 정의되지 않은 필드는 무시

# 미국 주식 잔고 조회 응답 스키마 정의
class UsaStockBalanceResponse(BaseModel):
    output1: List[UsaStockBalanceItem]         # 잔고 목록
    output2: Optional[UsaStockBalanceSummary]  # 잔고 요약 (Optional로 설정)
    rt_cd: str                                 # 응답 코드
    msg_cd: str                                # 메시지 코드
    msg1: str                                  # 응답 메시지

    class Config:
        allow_population_by_field_name = True
        extra = "ignore"  # 정의되지 않은 필드는 무시

# --- 해외 주식 잔고 조회 스키마 추가 끝 ---

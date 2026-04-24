"""
common/calendar.py
KRX 거래일 관리 및 Forward Fill 유틸리티
- Look-ahead bias 방지를 위해 '기준일 이전' 거래일 필터링 제공
"""
import pandas as pd
from typing import Optional


def get_krx_trading_dates(
    start: str,
    end: Optional[str] = None,
) -> pd.DatetimeIndex:
    """
    pykrx 기반 KRX 실제 거래일 목록 반환.
    pykrx 미설치 시 pd.bdate_range 폴백 사용 (공휴일 미반영).
    """
    end = end or pd.Timestamp.today().strftime("%Y-%m-%d")
    try:
        from pykrx import stock
        raw = stock.get_index_ohlcv_by_date(start, end, "1028")  # KOSPI
        return pd.DatetimeIndex(raw.index)
    except Exception:
        # 폴백: 영업일(월~금) 기준
        return pd.bdate_range(start=start, end=end)


def forward_fill_to_trading_dates(
    df: pd.DataFrame,
    trading_dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    df를 trading_dates 기준으로 reindex한 뒤 Forward Fill.
    글로벌 지표(SOX, 환율 등)와 국내 종목 조인 시 공백 처리용.

    Parameters
    ----------
    df : DatetimeIndex를 인덱스로 갖는 DataFrame
    trading_dates : KRX 거래일 인덱스

    Returns
    -------
    Forward Fill 적용된 DataFrame
    """
    combined = df.index.union(trading_dates)
    df_reindexed = df.reindex(combined).ffill()
    return df_reindexed.loc[trading_dates]


def lag_n_trading_days(
    df: pd.DataFrame,
    n: int = 1,
    trading_dates: Optional[pd.DatetimeIndex] = None,
) -> pd.DataFrame:
    """
    T-n 거래일 shift (Look-ahead bias 방지용).
    글로벌 T-1 → 국내 T 반영(Lag-join)에 사용.

    Parameters
    ----------
    df : DatetimeIndex 기반 DataFrame
    n  : 몇 거래일 lag
    trading_dates : 거래일 인덱스 (None이면 df.index 사용)

    Returns
    -------
    n거래일 lag된 DataFrame
    """
    if trading_dates is not None:
        df = df.reindex(trading_dates).ffill()
    return df.shift(n)

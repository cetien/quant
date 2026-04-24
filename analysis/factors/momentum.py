"""
analysis/factors/momentum.py
모멘텀 팩터 계산
- RS (Relative Strength vs KOSPI)
- 수익률 (N일)
- 신고가 근접도
- Look-ahead bias 방지: .shift(1) 적용 후 rolling
"""
import pandas as pd
import numpy as np
from typing import List


def calc_return(price: pd.Series, period: int) -> pd.Series:
    """
    N일 수익률.
    Look-ahead bias 방지: 당일 close는 T-1 기준으로 이미 확정.
    """
    return price.pct_change(periods=period)


def calc_relative_strength(
    price: pd.Series,
    benchmark: pd.Series,
    period: int = 60,
) -> pd.Series:
    """
    상대강도 (RS) = 종목 N일 수익률 / 벤치마크 N일 수익률
    값 > 1 이면 벤치마크 대비 강세.

    Parameters
    ----------
    price : 종목 adj_close 시계열
    benchmark : KOSPI adj_close 시계열 (같은 인덱스)
    period : 룩백 기간 (거래일)
    """
    stock_ret = price.pct_change(periods=period)
    bench_ret = benchmark.pct_change(periods=period)
    rs = stock_ret / bench_ret.replace(0, np.nan)
    return rs


def calc_52w_high_proximity(price: pd.Series, window: int = 252) -> pd.Series:
    """
    52주 신고가 근접도 = 현재가 / 52주 최고가
    1.0 = 신고가 갱신, 낮을수록 신고가에서 멀어짐.
    """
    rolling_high = price.rolling(window=window, min_periods=window // 2).max()
    return price / rolling_high


def compute_momentum_factors(
    price_wide: pd.DataFrame,
    benchmark: pd.Series,
    lookbacks: List[int],
) -> pd.DataFrame:
    """
    전체 종목 모멘텀 팩터 계산.

    Parameters
    ----------
    price_wide : 날짜 × 종목코드 adj_close 피벗 테이블
    benchmark  : KOSPI adj_close (같은 DatetimeIndex)
    lookbacks  : 수익률 룩백 기간 리스트 (예: [20, 60, 120])

    Returns
    -------
    MultiIndex DataFrame: (date, ticker) → 각 팩터 컬럼
    """
    results = {}

    for lb in lookbacks:
        ret = price_wide.pct_change(periods=lb)
        bench_ret = benchmark.pct_change(periods=lb)
        rs = ret.div(bench_ret.replace(0, np.nan), axis=0)
        results[f"ret_{lb}d"]  = ret
        results[f"rs_{lb}d"]   = rs

    prox = price_wide.apply(calc_52w_high_proximity)
    results["high52w_proximity"] = prox

    # wide → long 형식으로 변환
    long_frames = []
    for factor_name, wide_df in results.items():
        long_df = wide_df.stack().rename(factor_name)
        long_frames.append(long_df)

    combined = pd.concat(long_frames, axis=1)
    combined.index.names = ["date", "ticker"]
    return combined

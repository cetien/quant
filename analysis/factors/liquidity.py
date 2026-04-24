"""
analysis/factors/liquidity.py
유동성 팩터 계산: 거래대금, 거래량 증가율
"""
import pandas as pd
import numpy as np


def compute_liquidity_factors(
    price_wide: pd.DataFrame,       # adj_close 피벗
    amount_wide: pd.DataFrame,      # 거래대금 피벗 (pykrx 제공)
    window: int = 20,
) -> pd.DataFrame:
    """
    유동성 팩터 계산.

    Parameters
    ----------
    price_wide  : 날짜 × 종목코드 adj_close
    amount_wide : 날짜 × 종목코드 거래대금 (amount)
    window      : 평균 기준 기간 (거래일)

    Returns
    -------
    long format: (date, ticker) → [amount_mean, amount_surge_ratio]
    """
    # N일 평균 거래대금
    amount_mean = amount_wide.rolling(window=window, min_periods=window // 2).mean()
    # 거래대금 급증률 = 당일 / N일 평균
    amount_surge = amount_wide / amount_mean.replace(0, np.nan)

    mean_long  = amount_mean.stack().rename("amount_mean")
    surge_long = amount_surge.stack().rename("amount_surge_ratio")

    combined = pd.concat([mean_long, surge_long], axis=1)
    combined.index.names = ["date", "ticker"]
    return combined

"""
analysis/factors/quality.py
퀄리티 팩터 계산: ROE, 영업이익률, 부채비율
"""
import pandas as pd
import numpy as np


def compute_quality_factors(fundamentals: pd.DataFrame) -> pd.DataFrame:
    """
    announce_date 기준 최신 재무 데이터에서 퀄리티 팩터 계산.

    Parameters
    ----------
    fundamentals : [ticker, announce_date, roe, revenue, operating_income, debt_ratio]

    Returns
    -------
    long format: (announce_date, ticker) → [roe, op_margin, debt_ratio]
    """
    df = fundamentals.copy()
    df["announce_date"] = pd.to_datetime(df["announce_date"])

    # 영업이익률 = 영업이익 / 매출액
    df["op_margin"] = df["operating_income"] / df["revenue"].replace(0, np.nan)

    result = df.set_index(["announce_date", "ticker"])[["roe", "op_margin", "debt_ratio"]]
    return result

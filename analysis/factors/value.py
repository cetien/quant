"""
analysis/factors/value.py
밸류 팩터 계산
- PER, PBR, PSR
- announce_date 기준 join (Look-ahead bias 원천 차단)
"""
import pandas as pd
import numpy as np


def calc_value_factors(
    fundamentals: pd.DataFrame,
    price_wide: pd.DataFrame,
) -> pd.DataFrame:
    """
    밸류 팩터 계산.

    Parameters
    ----------
    fundamentals : [ticker, announce_date, per, pbr, revenue, ...] 테이블
                   ★ report_date 아닌 announce_date 기준으로 join해야 함
    price_wide   : 날짜 × 종목코드 adj_close 피벗

    Returns
    -------
    long format: (date, ticker) → [per, pbr, per_rank, pbr_rank]
    """
    # announce_date 기준으로 가장 최근 분기 재무 데이터 사용
    # (미래 공시 데이터 참조 방지)
    fund = fundamentals.copy()
    fund["announce_date"] = pd.to_datetime(fund["announce_date"])
    fund = fund.sort_values(["ticker", "announce_date"])

    # 각 거래일에 대해 해당일 이전 가장 최근 announce_date 데이터 사용
    dates = price_wide.index
    tickers = price_wide.columns

    per_map  = {}
    pbr_map  = {}

    for dt in dates:
        available = fund[fund["announce_date"] <= dt]
        if available.empty:
            continue
        latest = available.sort_values("announce_date").groupby("ticker").last()
        per_map[dt]  = latest["per"].reindex(tickers)
        pbr_map[dt]  = latest["pbr"].reindex(tickers)

    per_wide  = pd.DataFrame(per_map).T
    pbr_wide  = pd.DataFrame(pbr_map).T

    # PER/PBR 낮을수록 좋음 → 부호 반전하여 Z-score
    per_long  = per_wide.stack().rename("per")
    pbr_long  = pbr_wide.stack().rename("pbr")

    combined = pd.concat([per_long, pbr_long], axis=1)
    combined.index.names = ["date", "ticker"]
    return combined

"""
analysis/selector.py
종목 필터링, 섹터 내 대장주 판별, 오늘의 리더보드 생성
"""
from typing import List, Optional

import pandas as pd
import numpy as np

from common.logger import get_logger

log = get_logger(__name__)


class StockSelector:
    """
    팩터 스코어 기반 종목 선택 및 리더보드 생성.
    """

    def __init__(self, score_df: pd.DataFrame) -> None:
        """
        score_df: MultiIndex (date, ticker) → 팩터 컬럼 + final_score
        """
        self.score_df = score_df

    def filter_universe(
        self,
        min_amount_mean: Optional[float] = None,
        sectors: Optional[List[str]] = None,
        sector_map: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        1차 유니버스 필터링 (유동성, 섹터).
        min_amount_mean: 최소 평균 거래대금 (원)
        """
        df = self.score_df.copy()

        if min_amount_mean and "amount_mean" in df.columns:
            df = df[df["amount_mean"] >= min_amount_mean]

        if sectors and sector_map is not None:
            tickers = df.index.get_level_values("ticker")
            in_sector = tickers.map(sector_map).isin(sectors)
            df = df[in_sector.values]

        return df

    def get_top_n(
        self,
        as_of_date: str,
        n: int = 10,
        sector_neutral: bool = False,
        sector_map: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        특정 날짜 기준 final_score 상위 N 종목.

        as_of_date 이후 데이터는 참조하지 않음 (Look-ahead bias 방지).
        """
        dt = pd.Timestamp(as_of_date)
        available = self.score_df[self.score_df.index.get_level_values("date") <= dt]

        if available.empty:
            log.warning(f"as_of_date={as_of_date}: 데이터 없음.")
            return pd.DataFrame()

        # 가장 최근 날짜 기준
        latest_date = available.index.get_level_values("date").max()
        day_scores  = available.xs(latest_date, level="date")["final_score"].dropna()

        if sector_neutral and sector_map is not None:
            # 섹터별로 균등하게 N개 배분
            day_scores = day_scores.to_frame("final_score")
            day_scores["sector"] = day_scores.index.map(sector_map)
            n_sectors = day_scores["sector"].nunique()
            per_sector = max(1, n // n_sectors)
            top = (
                day_scores.groupby("sector")
                .apply(lambda g: g.nlargest(per_sector, "final_score"))
                .droplevel(0)
            )
        else:
            top = day_scores.nlargest(n).to_frame("final_score")

        return top.sort_values("final_score", ascending=False)

    def compute_relative_strength_rank(
        self,
        as_of_date: str,
        rs_col: str = "rs_60d",
    ) -> pd.Series:
        """
        특정 날짜 기준 RS 백분위 순위 (0~1).
        1.0에 가까울수록 강한 종목.
        """
        dt = pd.Timestamp(as_of_date)
        day_scores = self.score_df[
            self.score_df.index.get_level_values("date") == dt
        ]
        if day_scores.empty or rs_col not in day_scores.columns:
            return pd.Series(dtype=float)
        return day_scores[rs_col].rank(pct=True)

    def identify_sector_leaders(
        self,
        as_of_date: str,
        sector_map: pd.Series,
        percentile: float = 0.9,
    ) -> pd.DataFrame:
        """
        섹터 내 final_score 상위 퍼센타일 종목 (대장주 후보).
        반환: [ticker, sector, final_score, sector_rank_pct]
        """
        dt = pd.Timestamp(as_of_date)
        day = self.score_df[
            self.score_df.index.get_level_values("date") == dt
        ]["final_score"].dropna().to_frame()

        day["sector"] = day.index.get_level_values("ticker").map(sector_map)
        day["sector_rank_pct"] = day.groupby("sector")["final_score"].rank(pct=True)

        leaders = day[day["sector_rank_pct"] >= percentile].copy()
        leaders.index = leaders.index.get_level_values("ticker")
        return leaders.sort_values("sector_rank_pct", ascending=False)

    def build_today_leaderboard(
        self,
        as_of_date: str,
        sector_map: Optional[pd.Series] = None,
        top_n: int = 20,
    ) -> pd.DataFrame:
        """
        오늘의 리더보드: RS, 거래대금 급증률, 최종 스코어 종합.
        """
        top = self.get_top_n(as_of_date, n=top_n)
        if top.empty:
            return pd.DataFrame()

        dt = pd.Timestamp(as_of_date)
        day_data = self.score_df[
            self.score_df.index.get_level_values("date") == dt
        ].copy()
        day_data.index = day_data.index.get_level_values("ticker")

        leaderboard = top.join(
            day_data[["rs_60d", "amount_surge_ratio"]],
            how="left"
        )
        if sector_map is not None:
            leaderboard["sector"] = leaderboard.index.map(sector_map)

        leaderboard = leaderboard.reset_index().rename(columns={"index": "ticker"})
        leaderboard["rank"] = range(1, len(leaderboard) + 1)
        return leaderboard[["rank", "ticker"] +
                           [c for c in leaderboard.columns if c not in ["rank", "ticker"]]]

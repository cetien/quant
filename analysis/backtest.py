"""
analysis/backtest.py
팩터 기반 전략 백테스트 엔진
- T+1 실행 강제 (Look-ahead bias 방지 핵심)
- 생존 편향 방지: delisted_stocks 테이블 반영
- MDD, Sharpe, 연간 수익률 계산
"""
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np
import pandas as pd

from common.config import cfg
from common.logger import get_logger

log = get_logger(__name__)


@dataclass
class StrategyConfig:
    rebalance_freq: str  = "M"      # 'D', 'W', 'M'
    top_n: int           = 20
    fee_rate: float      = 0.003    # 편도 0.3%
    slippage_bps: float  = 10.0     # 10 basis points


class Backtester:
    """
    팩터 스코어 기반 백테스트 엔진.

    핵심 원칙:
    1. T일 스코어 → T+1일 시가 진입 (Look-ahead bias 방지)
    2. 상폐 종목은 상폐일 직전 거래일 종가로 강제 청산
    """

    def __init__(
        self,
        price_wide: pd.DataFrame,       # adj_close 피벗 (날짜 × 종목)
        score_df: pd.DataFrame,         # MultiIndex (date, ticker) → final_score
        config: Optional[StrategyConfig] = None,
        delisted: Optional[Dict[str, str]] = None,  # {ticker: delist_date}
    ) -> None:
        self.price_wide  = price_wide
        self.score_df    = score_df
        self.cfg         = config or StrategyConfig()
        self.delisted    = delisted or {}

    # ── 리밸런싱 일자 생성 ────────────────────────────────────────────────────

    def _rebalance_dates(self) -> pd.DatetimeIndex:
        dates = self.price_wide.index
        if self.cfg.rebalance_freq == "D":
            return dates
        elif self.cfg.rebalance_freq == "W":
            return dates[dates.dayofweek == 4]   # 금요일
        else:  # "M"
            return dates[dates.is_month_end]

    # ── 시그널 생성 (T+1 lag) ─────────────────────────────────────────────────

    def generate_signals(self) -> pd.DataFrame:
        """
        T일 스코어 기준 상위 N 종목 선택.
        실제 진입은 T+1일 → signal 인덱스를 1거래일 shift.
        """
        score_wide = self.score_df["final_score"].unstack("ticker")
        rebal_dates = self._rebalance_dates()

        signals = pd.DataFrame(False, index=self.price_wide.index,
                               columns=self.price_wide.columns)

        for dt in rebal_dates:
            if dt not in score_wide.index:
                continue
            scores = score_wide.loc[dt].dropna().nlargest(self.cfg.top_n)
            selected = scores.index.tolist()

            # T+1일 인덱스 찾기
            future_dates = self.price_wide.index[self.price_wide.index > dt]
            if future_dates.empty:
                continue
            entry_date = future_dates[0]

            # 다음 리밸런싱 전까지 유지
            next_rebal_dates = rebal_dates[rebal_dates > dt]
            exit_date = future_dates[next_rebal_dates[0] > future_dates].max() \
                if not next_rebal_dates.empty else self.price_wide.index[-1]

            hold_period = self.price_wide.index[
                (self.price_wide.index >= entry_date) & (self.price_wide.index <= exit_date)
            ]
            signals.loc[hold_period, selected] = True

        return signals

    # ── 포트폴리오 NAV 계산 ───────────────────────────────────────────────────

    def run_backtest(self) -> pd.Series:
        """
        신호 기반 등가중 포트폴리오 NAV(Net Asset Value) 시계열 계산.
        Returns: DatetimeIndex → NAV (시작값=1.0)
        """
        signals = self.generate_signals()
        price   = self.price_wide.reindex(columns=signals.columns)

        daily_ret = price.pct_change()

        # 생존 편향 방지: 상폐 종목 상폐일 이후 수익률 0 처리
        for ticker, ddate in self.delisted.items():
            if ticker in daily_ret.columns:
                daily_ret.loc[pd.Timestamp(ddate):, ticker] = 0.0

        # 포트폴리오 일간 수익률 (등가중)
        n_holdings = signals.sum(axis=1).replace(0, np.nan)
        port_ret   = (daily_ret * signals).sum(axis=1) / n_holdings

        # 거래 비용 (리밸런싱 일자에 적용)
        rebal_dates = self._rebalance_dates()
        cost = self.cfg.fee_rate * 2 + self.cfg.slippage_bps / 10000
        port_ret.loc[port_ret.index.isin(rebal_dates)] -= cost

        nav = (1 + port_ret.fillna(0)).cumprod()
        return nav

    # ── 성과 지표 계산 ────────────────────────────────────────────────────────

    def calculate_statistics(self, nav: pd.Series) -> Dict[str, float]:
        """MDD, Sharpe, CAGR 계산."""
        daily_ret = nav.pct_change().dropna()
        n_days    = len(daily_ret)

        cagr  = (nav.iloc[-1] / nav.iloc[0]) ** (252 / n_days) - 1
        sharpe = (daily_ret.mean() / (daily_ret.std() + 1e-8)) * np.sqrt(252)

        rolling_max = nav.cummax()
        drawdown    = (nav - rolling_max) / rolling_max
        mdd         = drawdown.min()

        return {
            "CAGR":   round(cagr * 100, 2),
            "Sharpe": round(sharpe, 3),
            "MDD":    round(mdd * 100, 2),
            "기간(일)": n_days,
        }

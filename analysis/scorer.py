"""
analysis/scorer.py
팩터 정규화(Z-score) + 가중합 최종 점수 계산
- 섹터 중립화: 섹터 내 상대 Z-score
- 부호 처리: PER/PBR/부채비율은 낮을수록 좋음 → 부호 반전
"""
from typing import Dict, Optional

import numpy as np
import pandas as pd

from common.config import cfg
from common.logger import get_logger

log = get_logger(__name__)

# 부호 반전이 필요한 팩터 (낮을수록 좋음)
INVERT_FACTORS = {"per", "pbr", "debt_ratio"}


def zscore_cross_section(df: pd.DataFrame) -> pd.DataFrame:
    """
    날짜별 Cross-sectional Z-score 정규화.
    각 날짜 시점에 전 종목의 평균/표준편차로 정규화.
    """
    return df.groupby(level="date").transform(
        lambda x: (x - x.mean()) / (x.std() + 1e-8)
    )


def zscore_within_sector(
    df: pd.DataFrame,
    sector_map: pd.Series,
) -> pd.DataFrame:
    """
    섹터 중립화: 섹터 내 Z-score.
    sector_map: Series(ticker → sector)

    섹터 쏠림 방지: 반도체 강세장에서 반도체 종목만 상위권 점령하는 현상 제거.
    """
    result = df.copy()
    result["sector"] = result.index.get_level_values("ticker").map(sector_map)

    def sector_zscore(group):
        for col in df.columns:
            if col in group.columns:
                mu  = group[col].mean()
                std = group[col].std()
                group[col] = (group[col] - mu) / (std + 1e-8)
        return group

    result = result.groupby(["date", "sector"], group_keys=False).apply(sector_zscore)
    return result.drop(columns=["sector"])


def compute_final_score(
    factor_df: pd.DataFrame,
    weights: Optional[Dict[str, float]] = None,
    sector_map: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    최종 팩터 점수 계산.

    Parameters
    ----------
    factor_df  : MultiIndex (date, ticker) × 팩터 컬럼들
    weights    : {팩터명: 가중치} (합계 1.0 권장)
    sector_map : 섹터 중립화 적용 시 필요

    Returns
    -------
    MultiIndex (date, ticker) → [각 팩터 z-score, final_score]
    """
    weights = weights or cfg.analysis.factor_weights
    df = factor_df.copy()

    # 부호 반전 (낮을수록 좋은 팩터)
    for col in INVERT_FACTORS:
        if col in df.columns:
            df[col] = -df[col]
            log.debug(f"부호 반전 적용: {col}")

    # Z-score 정규화
    if sector_map is not None:
        df = zscore_within_sector(df, sector_map)
    else:
        df = zscore_cross_section(df)

    # 가중합 최종 점수 계산
    # weights 키와 실제 컬럼 교집합만 사용
    available = [f for f in weights if f in df.columns]
    if not available:
        log.warning("가중치 키와 팩터 컬럼이 매칭되지 않음.")
        df["final_score"] = np.nan
        return df

    total_w = sum(weights[f] for f in available)
    df["final_score"] = sum(
        df[f] * (weights[f] / total_w) for f in available
    )

    return df

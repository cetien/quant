"""
common/config.py
전역 설정 관리 (dataclass 기반)
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


BASE_DIR = Path(__file__).resolve().parents[1]  # quant_project/

DATA_DIR    = BASE_DIR / "data"
RAW_DIR     = DATA_DIR / "raw"
DB_DIR      = DATA_DIR / "database"
EXPORT_DIR  = DATA_DIR / "exports"


@dataclass
class IngestionConfig:
    """수집 관련 설정"""
    # API 딜레이 (Rate Limit 방지)
    yfinance_delay_sec: float = 0.5
    pykrx_delay_sec: float   = 0.3
    retry_max: int           = 3
    retry_backoff: float     = 2.0  # exponential backoff 배수

    # 수집 기본 설정
    default_start_date: str = "2025-01-01"
    markets: List[str]      = field(default_factory=lambda: ["KOSPI", "KOSDAQ"])

    # 글로벌 매크로 티커 (yfinance)
    macro_tickers: List[str] = field(default_factory=lambda: [
        "^SOX",      # 필라델피아 반도체
        "^GSPC",     # S&P 500
        "^IXIC",     # NASDAQ
        "^KS11",     # KOSPI
        "^KQ11",     # KOSDAQ
        "KRW=X",     # USD/KRW
        "^TNX",      # 미국 10년 국채
    ])


@dataclass
class StorageConfig:
    """저장 관련 설정"""
    db_path: Path             = DB_DIR / "quant.duckdb"
    raw_prices_dir: Path      = RAW_DIR / "prices"
    raw_supply_dir: Path      = RAW_DIR / "supply"
    raw_macro_dir: Path       = RAW_DIR / "macro"
    parquet_compression: str  = "snappy"  # snappy / zstd


@dataclass
class AnalysisConfig:
    """팩터·분석 관련 설정"""
    # 모멘텀 팩터 룩백 (거래일 기준)
    momentum_lookbacks: List[int] = field(default_factory=lambda: [20, 60, 120])
    # 유동성 팩터 평균 윈도우
    liquidity_window: int = 20
    # 팩터 가중치 (합계 = 1.0)
    factor_weights: dict = field(default_factory=lambda: {
        "momentum":  0.40,
        "value":     0.20,
        "quality":   0.25,
        "liquidity": 0.15,
    })


@dataclass
class BacktestConfig:
    """백테스트 관련 설정"""
    rebalance_freq: str  = "M"    # D / W / M
    top_n: int           = 20     # 상위 N개 종목 보유
    fee_rate: float      = 0.003  # 매수·매도 각 0.3%
    slippage_bps: float  = 10.0   # 슬리피지 10bp


@dataclass
class AppConfig:
    """통합 설정 객체"""
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    storage:   StorageConfig   = field(default_factory=StorageConfig)
    analysis:  AnalysisConfig  = field(default_factory=AnalysisConfig)
    backtest:  BacktestConfig  = field(default_factory=BacktestConfig)


# 싱글턴 인스턴스 (전체 모듈에서 import하여 사용)
cfg = AppConfig()

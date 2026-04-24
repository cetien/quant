"""
main.py
파이프라인 오케스트레이터 (CLI 실행용)
Phase별 주석 해제하여 단계적으로 실행
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.logger import get_logger
from common.config import cfg
from storage.db_manager import DuckDBManager
from ingestion.price_collector import PriceCollector
from ingestion.supply_collector import SupplyCollector
from ingestion.macro_collector import MacroCollector

log = get_logger("main")


def main():
    log.info("=== Quant Pipeline START ===")

    # ── Phase 0: DB 초기화 ────────────────────────────────────────────────────
    db = DuckDBManager()
    db.init_schema()
    log.info("Phase 0 완료: 스키마 초기화")

    # ── Phase 1: 데이터 수집 ──────────────────────────────────────────────────
    # 1-1. 매크로 지표 (가장 빠름, 먼저 실행)
    # mc = MacroCollector(db)
    # mc.incremental_update_macro()
    # log.info("Phase 1-1 완료: 매크로 수집")

    # 1-2. 종목 마스터 (pykrx)
    # pc = PriceCollector(db)
    # pc.update_stock_master()
    # log.info("Phase 1-2 완료: 종목 마스터")

    # 1-3. 일봉 가격 (시간 소요 — 테스트 시 tickers 지정)
    # pc.incremental_update_daily_prices(tickers=["005930", "000660"])  # 테스트
    # pc.incremental_update_daily_prices()  # 전체
    # log.info("Phase 1-3 완료: 일봉 수집")

    # 1-4. 수급 데이터
    # sc = SupplyCollector(db)
    # sc.incremental_update_supply(tickers=["005930", "000660"])  # 테스트
    # log.info("Phase 1-4 완료: 수급 수집")

    # ── Phase 2: 팩터 계산 ────────────────────────────────────────────────────
    # from analysis.factors.momentum import compute_momentum_factors
    # from analysis.scorer import compute_final_score
    # price_wide = db.query("...").pivot(...)
    # benchmark  = price_wide["^KS11"] 또는 macro 테이블에서 로드
    # factor_df  = compute_momentum_factors(price_wide, benchmark, cfg.analysis.momentum_lookbacks)
    # score_df   = compute_final_score(factor_df)
    # log.info("Phase 2 완료: 팩터 계산")

    # ── Phase 3: UI 실행 ──────────────────────────────────────────────────────
    # streamlit run ui/app.py

    log.info("=== Quant Pipeline DONE ===")
    db.close()


if __name__ == "__main__":
    main()

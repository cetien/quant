"""
ui/pages/settings.py
데이터 수집 관리 (Phase 0~1 수동 실행 트리거)
"""
import streamlit as st

from storage.db_manager import DuckDBManager
from storage.schema import ALL_SCHEMAS
from ingestion.price_collector import PriceCollector
from ingestion.supply_collector import SupplyCollector
from ingestion.macro_collector import MacroCollector


def render():
    st.title("⚙️ 데이터 관리")

    db = DuckDBManager()

    # ── DB 상태 ──────────────────────────────────────────────────────────────
    st.subheader("DB 현황")
    col1, col2, col3 = st.columns(3)

    tables = ["stocks", "daily_prices", "supply", "macro_indicators"]
    for i, tbl in enumerate(tables):
        try:
            cnt = db.query(f"SELECT COUNT(*) AS n FROM {tbl}").iloc[0]["n"]
            label = tbl
        except Exception:
            cnt = "미생성"
            label = tbl
        with [col1, col2, col3][i % 3]:
            st.metric(label, cnt)

    last_price = db.get_last_date("daily_prices")
    last_macro = db.get_last_date("macro_indicators")
    st.caption(f"일봉 최신일: {last_price or '없음'}  |  매크로 최신일: {last_macro or '없음'}")

    st.markdown("---")

    # ── 스키마 초기화 ─────────────────────────────────────────────────────────
    st.subheader("Phase 0: 초기화")
    if st.button("🗄️ 스키마 초기화 (테이블 생성)"):
        db.init_schema()
        st.success("스키마 초기화 완료.")

    st.markdown("---")

    # ── 수집 실행 ────────────────────────────────────────────────────────────
    st.subheader("Phase 1: 데이터 수집")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("**① 종목 목록**")
        if st.button("종목 마스터 업데이트"):
            with st.spinner("pykrx에서 종목 목록 수집 중..."):
                pc = PriceCollector(db)
                pc.update_stock_master()
            st.success("종목 마스터 업데이트 완료.")

    with c2:
        st.markdown("**② 매크로 지표**")
        if st.button("매크로 증분 업데이트"):
            with st.spinner("yfinance에서 매크로 수집 중..."):
                mc = MacroCollector(db)
                mc.incremental_update_macro()
            st.success("매크로 업데이트 완료.")

    with c3:
        st.markdown("**③ 일봉 가격**")
        st.caption("⚠️ 2,770개 종목 전체 수집은 수 시간 소요")
        test_ticker = st.text_input("테스트 티커 (빈칸=전체)", placeholder="005930")
        if st.button("일봉 증분 업데이트"):
            tickers = [test_ticker.strip()] if test_ticker.strip() else None
            with st.spinner("일봉 수집 중..."):
                pc = PriceCollector(db)
                pc.incremental_update_daily_prices(tickers=tickers)
            st.success("일봉 업데이트 완료.")

    st.markdown("---")

    # ── 수급 수집 ─────────────────────────────────────────────────────────────
    st.subheader("수급 데이터 수집")
    st.caption("기관·외인 순매수 (pykrx 제공)")
    sup_ticker = st.text_input("수급 테스트 티커", placeholder="005930", key="sup")
    if st.button("수급 증분 업데이트"):
        tickers = [sup_ticker.strip()] if sup_ticker.strip() else None
        with st.spinner("수급 수집 중..."):
            sc = SupplyCollector(db)
            sc.incremental_update_supply(tickers=tickers)
        st.success("수급 업데이트 완료.")

    db.close()

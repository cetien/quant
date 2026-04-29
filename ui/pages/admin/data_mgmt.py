"""ui/pages/admin/data_mgmt.py — 데이터 수집 관리 (기존 settings.py Phase 0~1 부분)"""
import streamlit as st
from storage.db_manager import DuckDBManager
from ingestion.price_collector import PriceCollector
from ingestion.supply_collector import SupplyCollector
from ingestion.macro_collector import MacroCollector
from ui.components.stock_selector import stock_selector


def render(db: DuckDBManager) -> None:
    # DB 현황
    tables = ["stocks", "daily_prices", "supply", "macro_indicators",
              "sectors", "themes", "stock_theme_map"]
    cols = st.columns(4)
    for i, tbl in enumerate(tables):
        try:
            cnt = db.query(f"SELECT COUNT(*) AS n FROM {tbl}").iloc[0]["n"]
        except Exception:
            cnt = "미생성"
        cols[i % 4].metric(tbl, cnt)

    last_price = db.get_last_date("daily_prices")
    last_macro = db.get_last_date("macro_indicators")
    st.caption(f"일봉 최신일: {last_price or '없음'}  |  매크로 최신일: {last_macro or '없음'}")
    st.divider()

    # Phase 0
    st.markdown("**Phase 0 — 초기화**")
    if st.button("🗄️ 스키마 초기화 (테이블 생성)"):
        db.init_schema()
        st.success("스키마 초기화 완료.")
    st.divider()

    # Phase 1
    st.markdown("**Phase 1 — 데이터 수집**")
    c1, c2, c3 = st.columns(3)

    with c1:
        st.caption("① 종목 목록")
        if st.button("종목 마스터 업데이트", width="stretch"):
            try:
                with st.spinner("수집 중..."):
                    count = PriceCollector(db).update_stock_master()
                if count > 0:
                    st.success(f"완료: {count}개 종목 업데이트.")
                else:
                    st.warning("가져온 종목 데이터가 없습니다.")
            except Exception as e:
                st.error(f"종목 업데이트 실패: {e}")

    with c2:
        st.caption("② 매크로 지표")
        if st.button("매크로 증분 업데이트", width="stretch"):
            try:
                with st.spinner("수집 중..."):
                    MacroCollector(db).incremental_update_macro()
                st.success("완료.")
            except Exception as e:
                st.error(f"매크로 수집 실패: {e}")

    with c3:
        st.caption("③ 일봉 가격")
        price_ticker = stock_selector(db, key="dm_price", label="종목 검색 (빈칸=전체)")
        if price_ticker:
            st.caption(f"선택: `{price_ticker}`")
        if st.button("일봉 증분 업데이트", width="stretch"):
            tickers = [price_ticker] if price_ticker else None
            try:
                with st.spinner("수집 중..."):
                    count = PriceCollector(db).incremental_update_daily_prices(tickers=tickers)
                if count > 0:
                    st.success(f"완료: {count}행 데이터 적재됨.")
                else:
                    st.info("이미 최신 상태이거나 수집된 데이터가 없습니다.")
            except Exception as e:
                st.error(f"일봉 수집 실패: {e}")

    st.divider()
    st.caption("수급 데이터")
    sup_ticker = stock_selector(db, key="dm_sup", label="종목 검색 (빈칸=전체)")
    if sup_ticker:
        st.caption(f"선택: `{sup_ticker}`")
    if st.button("수급 증분 업데이트"):
        tickers = [sup_ticker] if sup_ticker else None
        with st.spinner("수집 중..."):
            SupplyCollector(db).incremental_update_supply(tickers=tickers)
        st.success("완료.")

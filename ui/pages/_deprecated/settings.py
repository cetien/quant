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

    st.markdown("---")

    # ── 테마 관리 ─────────────────────────────────────────────────────────────
    st.subheader("테마 관리")

    tab_add, tab_map, tab_list = st.tabs(["➕ 테마 추가", "🔗 종목 매핑", "📋 테마 목록"])

    with tab_add:
        st.markdown("**새 테마 등록 / 수정**")
        theme_name = st.text_input("테마명", placeholder="예: AI 전력 인프라")
        theme_desc = st.text_area("설명 (선택)", height=80)
        theme_active = st.checkbox("활성 테마", value=True)
        if st.button("저장", key="save_theme"):
            if not theme_name.strip():
                st.warning("테마명을 입력하세요.")
            else:
                tid = db.upsert_theme(
                    name=theme_name.strip(),
                    description=theme_desc.strip(),
                    is_active=theme_active,
                )
                st.success(f"저장 완료 (theme_id={tid})")

    with tab_map:
        st.markdown("**종목 ↔ 테마 매핑**")
        themes_df = db.query("SELECT theme_id, name FROM themes WHERE is_active = TRUE ORDER BY name")
        if themes_df.empty:
            st.info("활성 테마가 없습니다. 먼저 테마를 추가하세요.")
        else:
            theme_options = dict(zip(themes_df["name"], themes_df["theme_id"]))
            sel_theme_name = st.selectbox("테마 선택", list(theme_options.keys()))
            sel_theme_id   = theme_options[sel_theme_name]

            map_ticker = st.text_input("종목 티커", placeholder="005930", key="map_ticker")
            map_weight = st.number_input("weight (중요도)", min_value=0.1, value=1.0, step=0.1)
            map_source = st.selectbox("분류 방식", ["manual", "rule", "nlp"])

            col_add, col_del = st.columns(2)
            with col_add:
                if st.button("매핑 추가"):
                    t = map_ticker.strip()
                    if not t:
                        st.warning("티커를 입력하세요.")
                    else:
                        try:
                            db.con.execute(
                                """INSERT INTO stock_theme_map
                                   (ticker, theme_id, weight, source, valid_from)
                                   VALUES (?, ?, ?, ?, CURRENT_DATE)""",
                                [t, sel_theme_id, map_weight, map_source],
                            )
                            st.success(f"{t} → {sel_theme_name} 매핑 추가 완료")
                        except Exception as e:
                            st.error(f"오류: {e}")

            with col_del:
                if st.button("매핑 비활성화"):
                    t = map_ticker.strip()
                    if not t:
                        st.warning("티커를 입력하세요.")
                    else:
                        db.deactivate_theme_mapping(t, sel_theme_id)
                        st.success(f"{t} → {sel_theme_name} 비활성화 완료")

            st.markdown("**현재 매핑 종목**")
            mapped = db.query(
                f"SELECT ticker, name, weight, source, valid_from "
                f"FROM v_active_theme_map WHERE theme_id = {sel_theme_id} "
                f"ORDER BY weight DESC"
            )
            st.dataframe(mapped, use_container_width=True, hide_index=True)

    with tab_list:
        all_themes = db.query(
            "SELECT theme_id, name, description, is_active, created_at, updated_at "
            "FROM themes ORDER BY theme_id"
        )
        st.dataframe(all_themes, use_container_width=True, hide_index=True)

    db.close()

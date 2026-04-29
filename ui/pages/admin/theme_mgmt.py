"""ui/pages/admin/theme_mgmt.py — 테마 관리 (기존 settings.py 테마 섹션 이전)"""
import streamlit as st

import ui.state as state
from ingestion.price_collector import PriceCollector
from storage.db_manager import DuckDBManager
from ui.components.stock_selector import stock_selector


def render(db: DuckDBManager) -> None:
    tab_add, tab_map, tab_list = st.tabs(["➕ 테마 추가", "🔗 종목 매핑", "📋 테마 목록"])

    with tab_add:
        name = st.text_input("테마명", placeholder="예: AI 전력 인프라")
        desc = st.text_area("설명 (선택)", height=80)
        rating = st.number_input("Rating", min_value=0, max_value=10, value=5)
        active = st.checkbox("활성 테마", value=True)
        if st.button("저장", key="tm_save"):
            if not name.strip():
                st.warning("테마명을 입력하세요.")
            else:
                tid = db.upsert_theme(name.strip(), desc.strip(), active, rating)
                st.success(f"저장 완료 (theme_id={tid})")

    with tab_map:
        themes = db.query("SELECT theme_id, name FROM themes WHERE is_active=TRUE ORDER BY name")
        if themes.empty:
            st.info("활성 테마 없음.")
        else:
            opts = dict(zip(themes["name"], themes["theme_id"]))
            edit_group_id = state.get("edit_group_id")
            edit_group_type = state.get("edit_group_type")
            preferred_theme_id = None
            if edit_group_type == "theme" and edit_group_id:
                try:
                    preferred_theme_id = int(str(edit_group_id).replace("thm_", ""))
                except ValueError:
                    preferred_theme_id = None

            theme_names = list(opts.keys())
            default_index = 0
            if preferred_theme_id is not None:
                for idx, theme_name in enumerate(theme_names):
                    if opts[theme_name] == preferred_theme_id:
                        default_index = idx
                        break

            sel_name = st.selectbox("테마 선택", theme_names, index=default_index, key="tm_sel")
            sel_id = opts[sel_name]

            ticker = stock_selector(db, key="theme_mgmt_map", label="종목 검색")
            if ticker:
                st.caption(f"선택된 티커: `{ticker}`")

            weight = st.number_input("weight", min_value=0.1, value=1.0, step=0.1)
            source = st.selectbox("분류 방식", ["manual", "rule", "nlp"])

            ca, cd = st.columns(2)
            with ca:
                if st.button("매핑 추가", width="stretch"):
                    if not ticker:
                        st.warning("종목을 선택하세요.")
                    else:
                        try:
                            db.con.execute(
                                "INSERT INTO stock_theme_map "
                                "(ticker,theme_id,weight,source,valid_from) "
                                "VALUES (?,?,?,?,CURRENT_DATE)",
                                [ticker, sel_id, weight, source],
                            )
                            st.success(f"{ticker} → {sel_name} 추가 완료")
                        except Exception as exc:
                            st.error(f"오류: {exc}")
            with cd:
                if st.button("매핑 비활성화", width="stretch"):
                    if not ticker:
                        st.warning("종목을 선택하세요.")
                    else:
                        db.deactivate_theme_mapping(ticker, sel_id)
                        st.success(f"{ticker} → {sel_name} 비활성화")

            mapped = db.query(
                f"SELECT ticker, name, weight, source, valid_from "
                f"FROM v_active_theme_map WHERE theme_id={sel_id} ORDER BY weight DESC"
            )
            st.dataframe(mapped, width="stretch", hide_index=True)

            st.divider()
            st.markdown("**📥 가격 데이터 수집**")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("⬆ 증분 업데이트 (이 테마 전체)", width="stretch", key="tm_incr"):
                    tickers = mapped["ticker"].tolist() if not mapped.empty else []
                    if not tickers:
                        st.warning("매핑된 종목이 없습니다.")
                    else:
                        with st.spinner(f"{len(tickers)}개 종목 증분 수집 중..."):
                            loaded = PriceCollector(db).incremental_update_daily_prices(tickers=tickers)
                        st.success(f"완료: {loaded}건 적재")
            with c2:
                if st.button("⬇ 소급 수집 (이 테마 전체)", width="stretch", key="tm_backfill"):
                    tickers = mapped["ticker"].tolist() if not mapped.empty else []
                    if not tickers:
                        st.warning("매핑된 종목이 없습니다.")
                    else:
                        with st.spinner(f"{len(tickers)}개 종목 소급 수집 중..."):
                            loaded = PriceCollector(db).backfill_daily_prices(tickers=tickers)
                        st.success(f"완료: {loaded}건 적재")

    with tab_list:
        df = db.query(
            "SELECT theme_id, name, description, rating, is_active, created_at, updated_at "
            "FROM themes ORDER BY theme_id"
        )
        st.dataframe(df, width="stretch", hide_index=True)

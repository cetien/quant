"""ui/pages/admin/theme_mgmt.py — 테마 관리 (기존 settings.py 테마 섹션 이전)"""
import streamlit as st
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    tab_add, tab_map, tab_list = st.tabs(["➕ 테마 추가", "🔗 종목 매핑", "📋 테마 목록"])

    with tab_add:
        name  = st.text_input("테마명", placeholder="예: AI 전력 인프라")
        desc  = st.text_area("설명 (선택)", height=80)
        active = st.checkbox("활성 테마", value=True)
        if st.button("저장", key="tm_save"):
            if not name.strip():
                st.warning("테마명을 입력하세요.")
            else:
                tid = db.upsert_theme(name.strip(), desc.strip(), active)
                st.success(f"저장 완료 (theme_id={tid})")

    with tab_map:
        themes = db.query(
            "SELECT theme_id, name FROM themes WHERE is_active=TRUE ORDER BY name"
        )
        if themes.empty:
            st.info("활성 테마 없음.")
        else:
            opts = dict(zip(themes["name"], themes["theme_id"]))
            sel_name = st.selectbox("테마 선택", list(opts.keys()), key="tm_sel")
            sel_id   = opts[sel_name]
            ticker   = st.text_input("종목 티커", placeholder="005930", key="tm_ticker")
            weight   = st.number_input("weight", min_value=0.1, value=1.0, step=0.1)
            source   = st.selectbox("분류 방식", ["manual", "rule", "nlp"])

            ca, cd = st.columns(2)
            with ca:
                if st.button("매핑 추가", use_container_width=True):
                    t = ticker.strip()
                    if not t:
                        st.warning("티커를 입력하세요.")
                    else:
                        try:
                            db.con.execute(
                                "INSERT INTO stock_theme_map "
                                "(ticker,theme_id,weight,source,valid_from) "
                                "VALUES (?,?,?,?,CURRENT_DATE)",
                                [t, sel_id, weight, source],
                            )
                            st.success(f"{t} → {sel_name} 추가 완료")
                        except Exception as e:
                            st.error(f"오류: {e}")
            with cd:
                if st.button("매핑 비활성화", use_container_width=True):
                    t = ticker.strip()
                    if not t:
                        st.warning("티커를 입력하세요.")
                    else:
                        db.deactivate_theme_mapping(t, sel_id)
                        st.success(f"{t} → {sel_name} 비활성화")

            mapped = db.query(
                f"SELECT ticker, name, weight, source, valid_from "
                f"FROM v_active_theme_map WHERE theme_id={sel_id} ORDER BY weight DESC"
            )
            st.dataframe(mapped, width="stretch", hide_index=True)

    with tab_list:
        df = db.query(
            "SELECT theme_id, name, description, is_active, created_at, updated_at "
            "FROM themes ORDER BY theme_id"
        )
        st.dataframe(df, width="stretch", hide_index=True)

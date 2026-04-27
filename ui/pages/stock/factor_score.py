"""ui/pages/stock/factor_score.py — 팩터 스코어 뷰 (Phase B: scorer 연동 후 활성화)"""
import streamlit as st
from storage.db_manager import DuckDBManager
import ui.state as state


def render(db: DuckDBManager) -> None:
    ticker = state.get("selected_ticker")
    if not ticker:
        st.info("사이드바에서 종목을 선택하세요.")
        return

    st.caption("팩터 스코어 데이터는 scorer.py 실행 후 표시됩니다.")

    # 섹터/테마 정보는 즉시 표시 가능
    sector_info = db.query(f"""
        SELECT sec.name AS 섹터, ssm.weight AS 섹터비중
        FROM stock_sector_map ssm
        JOIN sectors sec ON sec.id = ssm.sector_id
        WHERE ssm.ticker = '{ticker}'
        ORDER BY ssm.weight DESC
    """)
    theme_info = db.query(f"""
        SELECT theme_name AS 테마, weight AS 중요도, source AS 분류, valid_from AS 편입일
        FROM v_active_theme_map
        WHERE ticker = '{ticker}'
        ORDER BY weight DESC
    """)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**소속 섹터**")
        if sector_info.empty:
            st.caption("섹터 미등록")
        else:
            st.dataframe(sector_info, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**소속 테마**")
        if theme_info.empty:
            st.caption("테마 미등록")
        else:
            st.dataframe(theme_info, use_container_width=True, hide_index=True)

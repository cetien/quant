"""ui/pages/market/theme_ranking.py — 테마 랭킹 (Phase B)"""
import streamlit as st
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    themes = db.query(
        "SELECT theme_id, name, description FROM themes WHERE is_active = TRUE ORDER BY name"
    )
    if themes.empty:
        st.info("등록된 테마 없음 — [관리 > 테마 관리]에서 추가하세요.")
        return

    st.caption("테마별 평균 수익률 랭킹 — 팩터 스코어 적재 후 활성화됩니다.")

    for _, row in themes.iterrows():
        with st.expander(f"**{row['name']}**"):
            mapped = db.query(f"""
                SELECT ticker, name, weight
                FROM v_active_theme_map
                WHERE theme_id = {row['theme_id']}
                ORDER BY weight DESC
                LIMIT 10
            """)
            if mapped.empty:
                st.caption("매핑된 종목 없음")
            else:
                st.dataframe(mapped, width="stretch", hide_index=True)

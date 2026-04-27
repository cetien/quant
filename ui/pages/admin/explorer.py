"""ui/pages/admin/explorer.py — 데이터 탐색 (기존 explorer.py 이전)"""
import streamlit as st
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    tables_df = db.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1"
    )
    if tables_df.empty:
        st.warning("조회 가능한 테이블 없음.")
        return

    table_list = tables_df["table_name"].tolist()
    c1, c2 = st.columns([2, 1])
    selected = c1.selectbox("테이블 / 뷰", table_list)
    limit    = c2.number_input("표시 행 수", 10, 5000, 200)

    if selected:
        cnt = db.row_count(selected)
        st.caption(f"전체 행 수: {cnt:,}")
        df = db.query(f"SELECT * FROM {selected} LIMIT {limit}")
        st.dataframe(df, width="stretch", hide_index=True)

    st.divider()
    st.markdown("**SQL 직접 실행**")
    sql = st.text_area("쿼리", f"SELECT * FROM {selected} LIMIT 10" if selected else "",
                       height=100)
    if st.button("실행"):
        try:
            res = db.query(sql)
            st.dataframe(res, width="stretch", hide_index=True)
        except Exception as e:
            st.error(f"오류: {e}")

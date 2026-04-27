"""ui/pages/admin/settings.py — 애플리케이션 설정 (Phase C)"""
import streamlit as st
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    st.caption("애플리케이션 환경 설정 — Phase C에서 구현 예정.")

    st.markdown("**DB 경로**")
    from common.config import StorageConfig
    try:
        cfg = StorageConfig()
        st.code(str(cfg.db_path))
    except Exception:
        st.code("common/config.py 확인 필요")

    st.divider()
    st.markdown("**위험 작업**")
    with st.expander("⚠️ 테이블 초기화"):
        st.warning("선택한 테이블의 모든 데이터가 삭제됩니다.")
        tables = ["daily_prices", "supply", "macro_indicators", "fundamentals"]
        tbl = st.selectbox("대상 테이블", tables)
        if st.button("DELETE ALL", type="primary"):
            db.con.execute(f"DELETE FROM {tbl}")
            st.success(f"{tbl} 초기화 완료.")

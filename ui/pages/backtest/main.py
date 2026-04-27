"""ui/pages/backtest/main.py — 백테스트 UI (기존 backtest_ui.py 이전)"""
import streamlit as st
import ui.state as state
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    st.info("팩터 스코어 데이터가 DB에 적재된 후 실행 가능합니다. (Phase C)")

    with st.expander("전략 설정", expanded=True):
        c1, c2 = st.columns(2)
        rebal = c1.selectbox("리밸런싱 주기", ["D", "W", "M"],
                             index=["D", "W", "M"].index(state.get("bt_rebal") or "M"),
                             key="bt_rebal_sel")
        top_n = c1.number_input("보유 종목 수", 5, 100,
                                int(state.get("bt_top_n") or 20), key="bt_topn_sel")
        fee   = c2.number_input("거래비용 (%)", 0.0, 1.0,
                                float(state.get("bt_fee") or 0.3), 0.05,
                                key="bt_fee_sel") / 100
        slip  = c2.number_input("슬리피지 (bps)", 0, 50,
                                int(state.get("bt_slip") or 10), key="bt_slip_sel")

    import pandas as pd
    c1, c2 = st.columns(2)
    start = c1.date_input("시작일", pd.Timestamp("2020-01-01"))
    end   = c2.date_input("종료일", pd.Timestamp.today())

    if st.button("▶ 백테스트 실행", type="primary"):
        st.warning("Phase C 기능 — scorer.py 실행 후 활성화됩니다.")

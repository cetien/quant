"""
ui/pages/backtest_ui.py
전략 백테스트 UI
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from storage.db_manager import DuckDBManager
from analysis.backtest import Backtester, StrategyConfig


def render():
    st.title("🧪 백테스트")
    st.info("Phase 4 기능 — 팩터 스코어 데이터가 먼저 생성되어야 실행 가능합니다.")

    with st.expander("전략 설정", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            rebal = st.selectbox("리밸런싱 주기", ["D", "W", "M"], index=2)
            top_n = st.number_input("보유 종목 수", 5, 50, 20)
        with col2:
            fee   = st.number_input("거래비용 (%)", 0.0, 1.0, 0.3, 0.05) / 100
            slip  = st.number_input("슬리피지 (bps)", 0, 50, 10)

    start_date = st.date_input("백테스트 시작일", value=pd.Timestamp("2020-01-01"))
    end_date   = st.date_input("백테스트 종료일",  value=pd.Timestamp.today())

    if st.button("▶ 백테스트 실행", type="primary"):
        st.warning("팩터 스코어가 DB에 저장된 후 실행 가능합니다. (개발 진행 중)")
        # TODO: score_df를 DB에서 로드 후 Backtester 실행
        # config = StrategyConfig(rebalance_freq=rebal, top_n=top_n, fee_rate=fee, slippage_bps=slip)
        # bt = Backtester(price_wide, score_df, config, delisted)
        # nav = bt.run_backtest()
        # stats = bt.calculate_statistics(nav)
        # st.metric("CAGR", f"{stats['CAGR']}%")
        # st.metric("Sharpe", stats['Sharpe'])
        # st.metric("MDD", f"{stats['MDD']}%")

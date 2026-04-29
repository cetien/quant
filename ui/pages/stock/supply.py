"""ui/pages/stock/supply.py — 수급 분석"""
import streamlit as st
import plotly.graph_objects as go
from storage.db_manager import DuckDBManager
import ui.state as state


def render(db: DuckDBManager) -> None:
    ticker = state.get("selected_ticker")
    period = state.get("stock_period") or "1Y"
    if not ticker:
        st.info("사이드바에서 종목을 선택하세요.")
        return

    days = {"3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}[period]

    df = db.query(f"""
        SELECT date, inst_net_buy, foreign_net_buy,
               inst_net_amount, foreign_net_amount
        FROM supply
        WHERE ticker = '{ticker}'
          AND date >= CURRENT_DATE - INTERVAL {days} DAY
        ORDER BY date
    """)

    if df.empty:
        st.info(f"{ticker} 수급 데이터 없음 — [관리 > 데이터 수집]에서 수급 수집을 실행하세요.")
        return

    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["date"], y=df["inst_net_buy"],
                         name="기관 순매수(주)", marker_color="#378ADD"))
    fig.add_trace(go.Bar(x=df["date"], y=df["foreign_net_buy"],
                         name="외인 순매수(주)", marker_color="#D85A30"))
    fig.update_layout(barmode="group", height=360,
                      margin=dict(l=0, r=0, t=20, b=0),
                      legend=dict(orientation="h", y=1.08))
    st.plotly_chart(fig, width="stretch")

    # 누적 수급
    df["inst_cum"]    = df["inst_net_buy"].cumsum()
    df["foreign_cum"] = df["foreign_net_buy"].cumsum()
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df["date"], y=df["inst_cum"],
                              name="기관 누적", line=dict(color="#378ADD")))
    fig2.add_trace(go.Scatter(x=df["date"], y=df["foreign_cum"],
                              name="외인 누적", line=dict(color="#D85A30")))
    fig2.update_layout(height=280, margin=dict(l=0, r=0, t=20, b=0),
                       legend=dict(orientation="h", y=1.08))
    st.caption("누적 순매수")
    st.plotly_chart(fig2, width="stretch")

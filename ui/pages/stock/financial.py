"""ui/pages/stock/financial.py — 재무 요약"""
import streamlit as st
import plotly.graph_objects as go
from storage.db_manager import DuckDBManager
import ui.state as state


def render(db: DuckDBManager) -> None:
    ticker = state.get("selected_ticker")
    if not ticker:
        st.info("사이드바에서 종목을 선택하세요.")
        return

    df = db.query(f"""
        SELECT fiscal_quarter, announce_date,
               eps, per, pbr, roe,
               revenue, operating_income, debt_ratio
        FROM fundamentals
        WHERE ticker = '{ticker}'
        ORDER BY report_date DESC
        LIMIT 12
    """)

    if df.empty:
        st.info(f"{ticker} 재무 데이터 없음.")
        return

    # 밸류에이션 카드
    latest = df.iloc[0]
    cols = st.columns(4)
    for col, (label, key) in zip(cols, [
        ("PER", "per"), ("PBR", "pbr"), ("ROE(%)", "roe"), ("EPS", "eps")
    ]):
        val = latest.get(key)
        col.metric(label, f"{val:.2f}" if val is not None else "-")

    st.divider()

    # 매출·영업이익 차트
    df_plot = df.sort_values("fiscal_quarter")
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df_plot["fiscal_quarter"],
                         y=df_plot["revenue"] / 1e9,
                         name="매출(십억)", marker_color="#378ADD"))
    fig.add_trace(go.Bar(x=df_plot["fiscal_quarter"],
                         y=df_plot["operating_income"] / 1e9,
                         name="영업이익(십억)", marker_color="#1D9E75"))
    fig.update_layout(barmode="group", height=320,
                      margin=dict(l=0, r=0, t=20, b=0),
                      legend=dict(orientation="h", y=1.08))
    st.plotly_chart(fig, width="stretch")

    st.dataframe(df, width="stretch", hide_index=True)

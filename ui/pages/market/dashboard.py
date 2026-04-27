"""ui/pages/market/dashboard.py — 매크로 대시보드"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    n_macro = db.row_count("macro_indicators")
    if n_macro == 0:
        st.info("매크로 데이터 없음 — [관리 > 데이터 수집]에서 매크로 수집을 실행하세요.")
        return

    macro_df = db.query("""
        SELECT indicator_code, date, value, change_rate
        FROM macro_indicators
        WHERE date = (SELECT MAX(date) FROM macro_indicators)
        ORDER BY indicator_code
    """)
    if macro_df.empty:
        return

    # 지표 카드
    cols = st.columns(min(len(macro_df), 4))
    for i, row in macro_df.iterrows():
        delta_str = f"{row['change_rate']:.2f}%" if pd.notna(row["change_rate"]) else None
        cols[i % 4].metric(row["indicator_code"], f"{row['value']:,.2f}", delta_str)

    st.divider()

    # 추세선
    indicators = macro_df["indicator_code"].tolist()
    c1, c2 = st.columns([3, 1])
    selected = c1.multiselect("지표 선택", indicators, default=indicators[:3])
    period = c2.selectbox("기간", ["1M", "3M", "6M", "1Y", "3Y"], index=3,
                          key="dash_period")
    period_days = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095}[period]

    if not selected:
        return

    in_clause = ", ".join(f"'{i}'" for i in selected)
    hist = db.query(f"""
        SELECT indicator_code, date, value FROM macro_indicators
        WHERE indicator_code IN ({in_clause})
          AND date >= CURRENT_DATE - INTERVAL {period_days} DAY
        ORDER BY date
    """)
    if hist.empty:
        st.info("해당 기간 데이터 없음.")
        return

    fig = make_subplots(rows=len(selected), cols=1, shared_xaxes=True,
                        subplot_titles=selected, vertical_spacing=0.06)
    for idx, code in enumerate(selected, 1):
        sub = hist[hist["indicator_code"] == code]
        fig.add_trace(go.Scatter(x=sub["date"], y=sub["value"],
                                 name=code, mode="lines"), row=idx, col=1)
    fig.update_layout(height=200 * len(selected), showlegend=False,
                      margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)

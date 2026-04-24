"""
ui/pages/deep_dive.py
개별 종목 심층 분석: 캔들차트 + 매크로 상관계수
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from storage.db_manager import DuckDBManager


def render():
    st.title("🔬 Deep Dive")

    db = DuckDBManager()

    # ── 종목 선택 ────────────────────────────────────────────────────────────
    stocks = db.query("SELECT ticker, name FROM stocks WHERE is_active = TRUE ORDER BY ticker")
    if stocks.empty:
        st.warning("종목 데이터 없음. 먼저 수집을 실행하세요.")
        return

    options = [f"{r['ticker']} {r['name']}" for _, r in stocks.iterrows()]
    selected = st.selectbox("종목 선택", options)
    ticker = selected.split()[0]

    period = st.selectbox("기간", ["3M", "6M", "1Y", "3Y", "5Y"], index=2)
    period_map = {"3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}
    days = period_map[period]

    # ── 가격 데이터 ──────────────────────────────────────────────────────────
    price_df = db.query(f"""
        SELECT date, open, high, low, close, adj_close, volume, amount
        FROM daily_prices
        WHERE ticker = '{ticker}'
          AND date >= CURRENT_DATE - INTERVAL {days} DAY
        ORDER BY date
    """)

    if price_df.empty:
        st.warning(f"{ticker} 가격 데이터 없음.")
        return

    # ── 캔들차트 ─────────────────────────────────────────────────────────────
    st.subheader(f"캔들차트: {selected}")
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.7, 0.3],
        subplot_titles=["가격", "거래량"]
    )

    fig.add_trace(go.Candlestick(
        x=price_df["date"],
        open=price_df["open"], high=price_df["high"],
        low=price_df["low"],  close=price_df["close"],
        name="OHLC",
    ), row=1, col=1)

    # 이동평균선
    for window, color in [(20, "orange"), (60, "blue"), (120, "purple")]:
        if len(price_df) >= window:
            ma = price_df["adj_close"].rolling(window).mean()
            fig.add_trace(go.Scatter(
                x=price_df["date"], y=ma,
                name=f"MA{window}", line=dict(color=color, width=1)
            ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=price_df["date"], y=price_df["volume"], name="거래량",
        marker_color="lightblue"
    ), row=2, col=1)

    fig.update_layout(xaxis_rangeslider_visible=False, height=600)
    st.plotly_chart(fig, use_container_width=True)

    # ── 매크로 상관계수 ───────────────────────────────────────────────────────
    st.subheader("매크로 상관계수 (60일 수익률 기준)")

    macro_df = db.query(f"""
        SELECT indicator_code, date, value
        FROM macro_indicators
        WHERE date >= CURRENT_DATE - INTERVAL {days} DAY
        ORDER BY date
    """)

    if not macro_df.empty and len(price_df) > 0:
        stock_ret = price_df.set_index("date")["adj_close"].pct_change()

        corr_results = []
        for code in macro_df["indicator_code"].unique():
            macro_sub = macro_df[macro_df["indicator_code"] == code].set_index("date")["value"]
            macro_ret = macro_sub.pct_change()

            combined = pd.concat([stock_ret, macro_ret], axis=1).dropna()
            combined.columns = ["stock", "macro"]
            if len(combined) > 10:
                corr = combined.corr().iloc[0, 1]
                corr_results.append({"지표": code, "상관계수": round(corr, 3)})

        if corr_results:
            corr_df = pd.DataFrame(corr_results).sort_values("상관계수", key=abs, ascending=False)
            st.bar_chart(corr_df.set_index("지표")["상관계수"])
            st.dataframe(corr_df, use_container_width=True)

    db.close()

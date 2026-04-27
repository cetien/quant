"""ui/pages/stock/chart.py — 캔들차트 + 이동평균 + 매크로 상관계수 (deep_dive 이전)"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from storage.db_manager import DuckDBManager
import ui.state as state


def render(db: DuckDBManager) -> None:
    ticker = state.get("selected_ticker")
    period = state.get("stock_period") or "1Y"
    if not ticker:
        st.info("사이드바에서 종목을 선택하세요.")
        return

    days = {"3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}[period]

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

    stock_name = db.query(f"SELECT name FROM stocks WHERE ticker='{ticker}'")
    title = f"{ticker}  {stock_name.iloc[0,0] if not stock_name.empty else ''}"

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.72, 0.28],
                        subplot_titles=[title, "거래량"])
    fig.add_trace(go.Candlestick(
        x=price_df["date"], open=price_df["open"], high=price_df["high"],
        low=price_df["low"], close=price_df["close"], name="OHLC",
    ), row=1, col=1)

    for w, color in [(20, "#EF9F27"), (60, "#378ADD"), (120, "#7F77DD")]:
        if len(price_df) >= w:
            ma = price_df["adj_close"].rolling(w).mean()
            fig.add_trace(go.Scatter(x=price_df["date"], y=ma,
                                     name=f"MA{w}", line=dict(color=color, width=1)),
                          row=1, col=1)

    colors = ["#D85A30" if r["close"] >= r["open"] else "#378ADD"
              for _, r in price_df.iterrows()]
    fig.add_trace(go.Bar(x=price_df["date"], y=price_df["volume"],
                         name="거래량", marker_color=colors), row=2, col=1)
    fig.update_layout(xaxis_rangeslider_visible=False, height=560,
                      margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)

    # 매크로 상관계수
    with st.expander("매크로 상관계수"):
        macro_df = db.query(f"""
            SELECT indicator_code, date, value FROM macro_indicators
            WHERE date >= CURRENT_DATE - INTERVAL {days} DAY ORDER BY date
        """)
        if macro_df.empty:
            st.caption("매크로 데이터 없음.")
            return
        stock_ret = price_df.set_index("date")["adj_close"].pct_change()
        rows = []
        for code in macro_df["indicator_code"].unique():
            sub = macro_df[macro_df["indicator_code"] == code].set_index("date")["value"]
            combined = pd.concat([stock_ret, sub.pct_change()], axis=1).dropna()
            combined.columns = ["s", "m"]
            if len(combined) > 10:
                rows.append({"지표": code, "상관계수": round(combined.corr().iloc[0, 1], 3)})
        if rows:
            cdf = pd.DataFrame(rows).sort_values("상관계수", key=abs, ascending=False)
            st.bar_chart(cdf.set_index("지표")["상관계수"])

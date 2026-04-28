"""ui/pages/stock/chart.py — 캔들차트 + 이동평균 + 매크로 상관계수 (deep_dive 이전)"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from storage.db_manager import DuckDBManager
import ui.state as state


_PERIOD_DAYS = {"3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}

_NAVER_URL = "https://finance.naver.com/item/main.naver?code={ticker}"


def _render_action_bar(db: DuckDBManager, ticker: str) -> None:
    """
    차트 상단 액션 바.
    👍 like   : stocks.rating += 1
    👎 dislike: stocks.rating -= 1  (하한 0)
    🔗 url    : 네이버 금융 새 탭으로 열기
    현재 rating 표시 포함.
    """
    try:
        row = db.query(f"SELECT name, rating FROM stocks WHERE ticker = '{ticker}'")
        name   = row.iloc[0]["name"]   if not row.empty else ticker
        rating = int(row.iloc[0]["rating"]) if not row.empty else 0
    except Exception:
        name, rating = ticker, 0

    naver_url = _NAVER_URL.format(ticker=ticker)

    st.markdown(
        f"<span style='font-size:1.05rem;font-weight:600'>{name}</span>"
        f"&nbsp;<code style='font-size:0.85rem'>{ticker}</code>"
        f"&nbsp;&nbsp;⭐ <b>{rating}</b>",
        unsafe_allow_html=True,
    )

    c1, c2, c3, _ = st.columns([1, 1, 1, 6])
    with c1:
        if st.button("👍", key=f"like_{ticker}", help="Like (+1 rating)"):
            db.execute(
                "UPDATE stocks SET rating = rating + 1 WHERE ticker = ?", [ticker]
            )
            st.rerun()
    with c2:
        if st.button("👎", key=f"dislike_{ticker}", help="Dislike (−1 rating, min 0)"):
            db.execute(
                "UPDATE stocks SET rating = MAX(0, rating - 1) WHERE ticker = ?", [ticker]
            )
            st.rerun()
    with c3:
        # Streamlit에서 새 탭 열기: link_button 사용 (1.31+)
        st.link_button("🔗", url=naver_url, help="네이버 금융에서 보기")


def _render_macro_chart(db: DuckDBManager, code: str) -> None:
    """매크로 지표 라인차트. sidebar에서 Global Index > 지표 클릭 시 표시."""
    st.markdown(f"### 📊 {code}")

    c1, c2 = st.columns([3, 1])
    period = c2.selectbox("기간", list(_PERIOD_DAYS), index=2, key="macro_chart_period")
    days = _PERIOD_DAYS[period]

    # 지표 전환 버튼
    if c1.button("← 지표 목록으로", key="macro_back_btn"):
        state.set("selected_macro", None)
        st.rerun()

    df = db.query(f"""
        SELECT date, value, change_rate
        FROM macro_indicators
        WHERE indicator_code = '{code}'
          AND date >= CURRENT_DATE - INTERVAL {days} DAY
        ORDER BY date
    """)
    if df.empty:
        st.warning(f"{code} 데이터 없음.")
        return

    latest = df.iloc[-1]
    cr = latest["change_rate"]
    delta_str = f"{cr:+.2f}%" if cr is not None and pd.notna(cr) else None
    st.metric(code, f"{latest['value']:,.2f}", delta_str)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["value"],
        mode="lines", name=code,
        line=dict(width=2, color="#378ADD"),
        fill="tozeroy", fillcolor="rgba(55,138,221,0.08)",
    ))
    fig.update_layout(
        height=420,
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis_rangeslider_visible=False,
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # 전 기간 등락률 테이블
    with st.expander("기간별 등락률"):
        rows = []
        for label, d in _PERIOD_DAYS.items():
            sub = db.query(f"""
                SELECT value FROM macro_indicators
                WHERE indicator_code = '{code}'
                  AND date <= CURRENT_DATE - INTERVAL {d} DAY
                ORDER BY date DESC LIMIT 1
            """)
            if not sub.empty and sub.iloc[0, 0]:
                past_val = sub.iloc[0, 0]
                ret = (latest["value"] - past_val) / past_val * 100
                rows.append({"기간": label, "등락률": f"{ret:+.2f}%"})
        if rows:
            st.table(pd.DataFrame(rows).set_index("기간"))


def render(db: DuckDBManager) -> None:
    # ── 매크로 지표 선택 시 라인차트 분기 ────────────────────────────────────
    macro_code = state.get("selected_macro")
    if macro_code:
        _render_macro_chart(db, macro_code)
        return

    # ── 종목 차트 ─────────────────────────────────────────────────────────────
    ticker = state.get("selected_ticker")
    period = state.get("stock_period") or "1Y"
    if not ticker:
        st.info("사이드바에서 종목을 선택하세요.")
        return

    # ── sidebar gridStock 클릭 시 incremental_update 트리거 ──────────────────
    trigger = state.get("trigger_price_update")
    if trigger == ticker:
        state.set("trigger_price_update", None)
        with st.spinner(f"{ticker} 가격 데이터 업데이트 중…"):
            try:
                from ingestion.price_collector import PriceCollector
                PriceCollector(db).incremental_update_daily_prices(tickers=[ticker])
            except Exception as e:
                st.warning(f"가격 업데이트 실패: {e}")

    # ── 상단 액션 버튼 ────────────────────────────────────────────────────────
    _render_action_bar(db, ticker)

    days = _PERIOD_DAYS.get(period, 365)

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

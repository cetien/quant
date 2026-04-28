"""ui/pages/stock/chart.py — 캔들차트 + 이동평균 + 매크로 상관계수"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from storage.db_manager import DuckDBManager
from ui.components.stock_selector import stock_selector
import ui.state as state

_PERIOD_DAYS = {"3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}
_NAVER_URL   = "https://finance.naver.com/item/main.naver?code={ticker}"


# ── History 패널 (left) ───────────────────────────────────────────────────────

def _render_history(db: DuckDBManager) -> None:
    st.caption("🕐 최근 조회")
    hist = db.get_history()
    if hist.empty:
        st.caption("─ 없음 ─")
        return

    cur_ticker = state.get("selected_ticker")
    cur_macro  = state.get("selected_macro")

    for _, row in hist.iterrows():
        ticker    = row["ticker"]
        kind      = row["kind"]
        name      = row["name"]
        is_pinned = bool(row["is_pinned"])
        is_cur    = (kind == "stock" and ticker == cur_ticker) or \
                    (kind == "macro" and ticker == cur_macro)

        col_pin, col_btn = st.columns([1, 4])
        with col_pin:
            new_pin = st.checkbox(
                "📌", value=is_pinned,
                key=f"pin_{kind}_{ticker}",
                label_visibility="collapsed",
                help="고정 (삭제 버튼에서 제외)",
            )
            if new_pin != is_pinned:
                db.set_history_pin(ticker, kind, new_pin)
                st.rerun()
        with col_btn:
            # 버튼 클릭 시 ticker를 사용하여 상태를 업데이트하도록 보장
            button_label = f"{'▶ ' if is_cur else ''}{name}"
            if st.button(
                button_label,
                key=f"hist_{kind}_{ticker}",
                use_container_width=True,
                help=ticker,  # 툴팁에 티커 표시
                type="primary" if is_cur else "secondary",
            ):
                if kind == "stock":
                    state.set("selected_ticker", ticker) # 종목명 대신 티커(ID) 저장
                    state.set("selected_macro",  None)
                else:
                    state.set("selected_macro",  ticker) # 매크로 지표 코드 저장
                    state.set("selected_ticker", None)
                state.set("active_tab", "종목 분석")
                st.rerun()

    st.divider()
    if st.button("🗑 삭제", use_container_width=True,
                 help="고정되지 않은 항목을 모두 제거합니다."):
        removed = db.delete_unpinned_history()
        st.toast(f"{removed}건 삭제됨")
        st.rerun()


# ── 액션 바 ──────────────────────────────────────────────────────────────────

def _render_action_bar(db: DuckDBManager, ticker: str) -> None:
    # 종목 검색 및 이동 익스팬더 추가
    with st.expander("🔍 종목 검색 및 이동"):
        new_ticker = stock_selector(db, key="chart_action_search", label="")
        if new_ticker and new_ticker != ticker:
            state.set("selected_ticker", new_ticker)
            state.set("selected_macro", None)
            st.rerun()

    try:
        row    = db.query(f"SELECT name, rating FROM stocks WHERE ticker = '{ticker}'")
        name   = row.iloc[0]["name"]   if not row.empty else ticker
        rating = int(row.iloc[0]["rating"]) if not row.empty else 0
    except Exception:
        name, rating = ticker, 0

    st.markdown(
        f"<span style='font-size:1.05rem;font-weight:600'>{name}</span>"
        f"&nbsp;<code style='font-size:0.85rem'>{ticker}</code>"
        f"&nbsp;&nbsp;⭐ <b>{rating}</b>",
        unsafe_allow_html=True,
    )
    c1, c2, c3, _ = st.columns([1, 1, 1, 6])
    with c1:
        if st.button("👍", key=f"like_{ticker}", help="Like (+1 rating)"):
            db.execute("UPDATE stocks SET rating = rating + 1 WHERE ticker = ?", [ticker])
            st.rerun()
    with c2:
        if st.button("👎", key=f"dislike_{ticker}", help="Dislike (−1 rating, min 0)"):
            db.execute("UPDATE stocks SET rating = MAX(0, rating - 1) WHERE ticker = ?", [ticker])
            st.rerun()
    with c3:
        st.link_button("🔗", url=_NAVER_URL.format(ticker=ticker), help="네이버 금융에서 보기")


# ── 매크로 차트 ───────────────────────────────────────────────────────────────

def _render_macro_chart(db: DuckDBManager, code: str) -> None:
    st.markdown(f"### 📊 {code}")
    c1, c2 = st.columns([3, 1])
    period = c2.selectbox("기간", list(_PERIOD_DAYS), index=2, key="macro_chart_period")
    days   = _PERIOD_DAYS[period]
    if c1.button("← 지표 목록으로", key="macro_back_btn"):
        state.set("selected_macro", None)
        st.rerun()

    df = db.query(f"""
        SELECT date, value, change_rate FROM macro_indicators
        WHERE indicator_code = '{code}'
          AND date >= CURRENT_DATE - INTERVAL {days} DAY
        ORDER BY date
    """)
    if df.empty:
        st.warning(f"{code} 데이터 없음.")
        return

    latest    = df.iloc[-1]
    cr        = latest["change_rate"]
    delta_str = f"{cr:+.2f}%" if cr is not None and pd.notna(cr) else None
    st.metric(code, f"{latest['value']:,.2f}", delta_str)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["value"], mode="lines", name=code,
        line=dict(width=2, color="#378ADD"),
        fill="tozeroy", fillcolor="rgba(55,138,221,0.08)",
    ))
    fig.update_layout(height=420, margin=dict(l=0, r=0, t=20, b=0),
                      xaxis_rangeslider_visible=False, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

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
                ret = (latest["value"] - sub.iloc[0, 0]) / sub.iloc[0, 0] * 100
                rows.append({"기간": label, "등락률": f"{ret:+.2f}%"})
        if rows:
            st.table(pd.DataFrame(rows).set_index("기간"))


# ── 종목 캔들차트 ─────────────────────────────────────────────────────────────

def _render_stock_chart(db: DuckDBManager, ticker: str) -> None:
    period   = state.get("stock_period") or "1Y"
    days     = _PERIOD_DAYS.get(period, 365)
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

    stock_info = db.query(f"SELECT name FROM stocks WHERE ticker='{ticker}'")
    name = stock_info.iloc[0, 0] if not stock_info.empty else ticker
    title = f"{ticker}  {name}"

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

    # ── 테마/섹터 정보 추가 ──────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("##### 📂 분류 정보 (Theme / Sector)")
    
    c1, c2 = st.columns(2)
    with c1:
        sector_info = db.query(f"""
            SELECT sec.id, sec.name AS 섹터, ssm.weight AS 비중
            FROM stock_sector_map ssm
            JOIN sectors sec ON sec.id = ssm.sector_id
            WHERE ssm.ticker = '{ticker}'
            ORDER BY ssm.weight DESC
        """)
        st.caption("소속 섹터 (클릭 시 사이드바 이동)")
        if sector_info.empty:
            st.info("등록된 섹터 정보가 없습니다.")
        else:
            sel_sector = st.dataframe(
                sector_info[["섹터", "비중"]], use_container_width=True, hide_index=True,
                on_select="rerun", selection_mode="single-row", key=f"ch_sec_{ticker}"
            )
            if sel_sector['selection']['rows']:
                idx = sel_sector['selection']['rows'][0]
                sec_id = sector_info.iloc[idx]['id']
                state.set("selected_group_id", f"sec_{sec_id}")
                st.rerun()
            
    with c2:
        theme_info = db.query(f"""
            SELECT theme_id, theme_name AS 테마, weight AS 비중, source AS 출처
            FROM v_active_theme_map
            WHERE ticker = '{ticker}'
            ORDER BY weight DESC
        """)
        st.caption("소속 테마 (클릭 시 사이드바 이동)")
        if theme_info.empty:
            st.info("등록된 테마 정보가 없습니다.")
        else:
            sel_theme = st.dataframe(
                theme_info[["테마", "비중", "출처"]], use_container_width=True, hide_index=True,
                on_select="rerun", selection_mode="single-row", key=f"ch_thm_{ticker}"
            )
            if sel_theme['selection']['rows']:
                idx = sel_theme['selection']['rows'][0]
                thm_id = theme_info.iloc[idx]['theme_id']
                state.set("selected_group_id", f"thm_{thm_id}")
                st.rerun()

    # ── 관련 리포트 정보 추가 ────────────────────────────────────────────────
    from ui.pages.market.view_report import render_report_grid, handle_report_selection

    st.markdown("---")
    st.markdown(f"##### 📋 {name} 관련 리포트")

    report_query = f"""
        SELECT id, date, title, writer, filepath
        FROM pdf_reports
        WHERE ticker = '{ticker}'
        ORDER BY date DESC
    """
    try:
        df_rep = db.query(report_query)
        if df_rep.empty:
            st.info("조회된 리포트가 없습니다.")
        else:
            df_rep["date"] = pd.to_datetime(df_rep["date"]).dt.strftime("%Y-%m-%d")
            resp = render_report_grid(df_rep, key=f"stock_rep_{ticker}", height=300)
            handle_report_selection(db, df_rep, resp.get("selected_rows"))
    except Exception as e:
        st.error(f"리포트 로드 실패: {e}")


# ── 공개 API ──────────────────────────────────────────────────────────────────

def render(db: DuckDBManager) -> None:
    macro_code = state.get("selected_macro")
    ticker     = state.get("selected_ticker")

    # 1. 히스토리 업데이트를 렌더링 전에 수행하여 UI 반영 지연 방지
    if macro_code:
        db.upsert_history(macro_code, macro_code, kind="macro")
    elif ticker:
        try:
            row  = db.query(f"SELECT name FROM stocks WHERE ticker = '{ticker}'")
            name = row.iloc[0]["name"] if not row.empty else ticker
        except Exception:
            name = ticker
        db.upsert_history(ticker, name, kind="stock")

    left, right = st.columns([2, 7])

    with left:
        _render_history(db)

    with right:
        if macro_code:
            _render_macro_chart(db, macro_code)
            return

        if not ticker:
            st.info("사이드바에서 종목을 선택하세요.")
            return

        trigger = state.get("trigger_price_update")
        if trigger == ticker:
            state.set("trigger_price_update", None)
            with st.spinner(f"{ticker} 가격 데이터 업데이트 중…"):
                try:
                    from ingestion.price_collector import PriceCollector
                    PriceCollector(db).incremental_update_daily_prices(tickers=[ticker])
                except Exception as e:
                    st.warning(f"가격 업데이트 실패: {e}")

        _render_action_bar(db, ticker)
        _render_stock_chart(db, ticker)

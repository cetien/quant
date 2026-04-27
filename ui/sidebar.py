"""
ui/sidebar.py
모든 탭에서 공유하는 사이드바.
- 맥락 필터: 현재 탭에 따라 내용이 달라짐
- 워치리스트: 항상 노출
- DB 상태 미니 위젯: 하단 고정
"""
from __future__ import annotations

from datetime import datetime, timedelta

import streamlit as st

from storage.db_manager import DuckDBManager
import ui.state as state


_DB_CACHE_SEC = 60  # DB 상태 캐시 TTL


def _refresh_db_status(db: DuckDBManager) -> dict:
    """DB 상태를 조회하고 세션에 캐시. 60초 이내 재조회 시 캐시 반환."""
    ts = state.get("db_status_ts")
    if ts and (datetime.now() - ts).seconds < _DB_CACHE_SEC:
        cached = state.get("db_status")
        if cached:
            return cached

    try:
        n_stocks    = db.row_count("stocks")
        n_prices    = db.row_count("daily_prices")
        last_price  = db.get_last_date("daily_prices")
        last_macro  = db.get_last_date("macro_indicators")
        status = {
            "n_stocks":   n_stocks,
            "n_prices":   n_prices,
            "last_price": last_price,
            "last_macro": last_macro,
            "ok":         n_stocks > 0 and last_price is not None,
        }
    except Exception:
        status = {"ok": False, "n_stocks": 0, "n_prices": 0,
                  "last_price": None, "last_macro": None}

    state.set("db_status", status)
    state.set("db_status_ts", datetime.now())
    return status


# ── 탭별 맥락 필터 ────────────────────────────────────────────────────────────

def _ctx_market() -> None:
    st.caption("시장 개요 옵션")
    st.selectbox("기간", ["1M", "3M", "6M", "1Y", "3Y"], index=3, key="mkt_period")


def _ctx_screener() -> None:
    st.caption("스크리너 필터")
    st.multiselect("시장", ["KOSPI", "KOSDAQ"],
                   default=["KOSPI", "KOSDAQ"], key="scr_market")
    st.number_input("최소 거래대금 (억)", min_value=0, value=50, step=10,
                    key="scr_min_amount")
    st.slider("최소 RS (60일)", 0.5, 2.0, 1.0, 0.05, key="scr_rs_min")
    st.number_input("상위 N개", 10, 200, 50, key="scr_top_n")


def _ctx_stock(db: DuckDBManager) -> None:
    st.caption("종목 선택")
    stocks = db.query(
        "SELECT ticker || ' ' || name AS label, ticker "
        "FROM stocks WHERE is_active = TRUE ORDER BY ticker"
    )
    if stocks.empty:
        st.info("종목 데이터 없음")
        return
    labels  = stocks["label"].tolist()
    tickers = stocks["ticker"].tolist()

    cur = state.get("selected_ticker")
    idx = tickers.index(cur) if cur in tickers else 0
    sel = st.selectbox("종목", labels, index=idx, key="sb_stock_select")
    state.set("selected_ticker", tickers[labels.index(sel)])
    st.selectbox("기간", ["3M", "6M", "1Y", "3Y", "5Y"],
                 index=2, key="stock_period")


def _ctx_backtest() -> None:
    st.caption("전략 파라미터")
    st.selectbox("리밸런싱", ["D", "W", "M"], index=2, key="bt_rebal")
    st.number_input("보유 종목 수", 5, 100, 20, key="bt_top_n")
    st.number_input("거래비용 (%)", 0.0, 1.0, 0.3, 0.05, key="bt_fee")
    st.number_input("슬리피지 (bps)", 0, 50, 10, key="bt_slip")


def _ctx_admin() -> None:
    st.caption("관리 메뉴")


# ── 워치리스트 ────────────────────────────────────────────────────────────────

def _watchlist_widget() -> None:
    st.markdown("---")
    watchlist = state.get("watchlist")

    with st.expander("★ 워치리스트", expanded=True):
        if not watchlist:
            st.caption("종목 없음")
        else:
            for ticker in watchlist:
                col_t, col_x = st.columns([4, 1])
                with col_t:
                    if st.button(ticker, key=f"wl_{ticker}", use_container_width=True):
                        state.navigate_to_stock(ticker)
                        st.rerun()
                with col_x:
                    if st.button("✕", key=f"wl_del_{ticker}"):
                        watchlist.remove(ticker)
                        state.set("watchlist", watchlist)
                        st.rerun()

        add_col, btn_col = st.columns([3, 1])
        with add_col:
            new_t = st.text_input("추가", placeholder="005930",
                                  label_visibility="collapsed", key="wl_input")
        with btn_col:
            if st.button("＋", key="wl_add"):
                t = new_t.strip()
                if t and t not in watchlist:
                    watchlist.append(t)
                    state.set("watchlist", watchlist)
                    st.rerun()


# ── DB 상태 미니 위젯 ─────────────────────────────────────────────────────────

def _db_status_widget(status: dict) -> None:
    st.markdown("---")
    if status["ok"]:
        st.caption(
            f"✅ DB 정상  \n"
            f"종목 {status['n_stocks']:,}개  \n"
            f"최신일 {status['last_price'] or '-'}"
        )
    else:
        st.caption("⚠️ DB 미수집 — [관리] 탭에서 초기화 필요")


# ── 공개 API ──────────────────────────────────────────────────────────────────

def render(active_tab: str, db: DuckDBManager) -> None:
    """
    app.py에서 with st.sidebar: 블록 안에서 호출.

    Parameters
    ----------
    active_tab : str   현재 선택된 상단 탭 이름
    db         : DuckDBManager
    """
    st.markdown("### 📈 Quant")

    ctx_map = {
        "시장 개요": _ctx_market,
        "스크리너":  lambda: _ctx_screener(),
        "종목 분석": lambda: _ctx_stock(db),
        "백테스트":  _ctx_backtest,
        "관리":      _ctx_admin,
    }
    ctx_fn = ctx_map.get(active_tab)
    if ctx_fn:
        ctx_fn()

    _watchlist_widget()

    status = _refresh_db_status(db)
    _db_status_widget(status)

"""
ui/pages/scanner.py
조건식 기반 종목 스크리너
"""
import streamlit as st
import pandas as pd

from storage.db_manager import DuckDBManager


def render():
    st.title("🔍 종목 스크리너")

    db = DuckDBManager()

    st.sidebar.subheader("필터 조건")

    # ── 조건 설정 ────────────────────────────────────────────────────────────
    min_amount = st.sidebar.number_input(
        "최소 평균 거래대금 (억원)", min_value=0, value=50, step=10
    ) * 1e8

    market_filter = st.sidebar.multiselect(
        "시장", ["KOSPI", "KOSDAQ"], default=["KOSPI", "KOSDAQ"]
    )

    ma_align = st.sidebar.checkbox("이동평균 정배열 (5>20>60>120일)", value=False)

    rs_min = st.sidebar.slider("최소 RS (60일, KOSPI 대비)", 0.5, 2.0, 1.0, 0.05)

    top_n = st.sidebar.number_input("상위 N개 표시", 10, 100, 30)

    if st.sidebar.button("🔍 검색 실행", type="primary"):
        with st.spinner("DuckDB 쿼리 실행 중..."):
            _run_screen(db, min_amount, market_filter, ma_align, rs_min, int(top_n))

    db.close()


def _run_screen(db, min_amount, markets, ma_align, rs_min, top_n):
    market_str = ", ".join([f"'{m}'" for m in markets])

    # 기본 쿼리: 최신일 기준 종목 스크리닝
    sql = f"""
        WITH latest AS (
            SELECT dp.ticker, dp.close, dp.adj_close, dp.volume, dp.amount,
                   s.name, s.market, s.sector
            FROM daily_prices dp
            JOIN stocks s ON dp.ticker = s.ticker
            WHERE dp.date = (SELECT MAX(date) FROM daily_prices)
              AND s.market IN ({market_str})
              AND s.is_active = TRUE
        ),
        agg AS (
            SELECT ticker,
                   AVG(amount) AS amount_mean_20d
            FROM daily_prices
            WHERE date >= CURRENT_DATE - INTERVAL 20 DAY
            GROUP BY ticker
        )
        SELECT l.*, a.amount_mean_20d
        FROM latest l
        JOIN agg a ON l.ticker = a.ticker
        WHERE a.amount_mean_20d >= {min_amount}
        ORDER BY a.amount_mean_20d DESC
        LIMIT {top_n}
    """

    try:
        result = db.query(sql)
    except Exception as e:
        st.error(f"쿼리 오류: {e}")
        return

    if result.empty:
        st.warning("조건에 맞는 종목이 없습니다.")
        return

    # 결과 표시
    result["amount_mean_20d(억)"] = (result["amount_mean_20d"] / 1e8).round(1)
    display_cols = ["ticker", "name", "market", "sector", "close", "amount_mean_20d(억)"]
    display_cols = [c for c in display_cols if c in result.columns]

    st.success(f"검색 완료: {len(result)}개 종목")
    st.dataframe(result[display_cols], width="stretch", height=600)

    # CSV 다운로드
    csv = result.to_csv(index=False, encoding="utf-8-sig")
    st.download_button("📥 CSV 다운로드", csv, "screener_result.csv", "text/csv")

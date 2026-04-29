"""ui/pages/screener/condition.py — 조건식 스크리너 (기존 scanner.py 이전)"""
import streamlit as st
import ui.state as state
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    # 사이드바에서 필터값 읽기
    markets   = state.get("scr_market") or ["KOSPI", "KOSDAQ"]
    min_amt   = (state.get("scr_min_amount") or 50) * 1e8
    rs_min    = state.get("scr_rs_min") or 1.0
    top_n     = int(state.get("scr_top_n") or 50)

    c1, c2 = st.columns([1, 1])
    ma_align = c1.checkbox("이동평균 정배열 (5>20>60>120일)")
    run = c2.button("🔍 검색 실행", type="primary", width="stretch")

    if not run:
        prev = state.get("screener_result")
        if prev is not None:
            st.caption("이전 검색 결과")
            st.dataframe(prev, width="stretch", hide_index=True)
        return

    market_str = ", ".join(f"'{m}'" for m in markets)
    sql = f"""
        WITH agg AS (
            SELECT ticker,
                   AVG(amount)  AS amount_20d,
                   AVG(close)   AS close_20d
            FROM daily_prices
            WHERE date >= CURRENT_DATE - INTERVAL 20 DAY
            GROUP BY ticker
        )
        SELECT s.ticker, s.name, s.market,
               sec.name AS sector,
               dp.close,
               ROUND(a.amount_20d / 1e8, 1) AS "거래대금(억·20일평균)"
        FROM stocks s
        JOIN daily_prices dp ON dp.ticker = s.ticker
            AND dp.date = (SELECT MAX(date) FROM daily_prices)
        JOIN agg a ON a.ticker = s.ticker
        LEFT JOIN v_stock_primary_sector sec ON sec.ticker = s.ticker
        WHERE s.market IN ({market_str})
          AND s.is_active = TRUE
          AND a.amount_20d >= {min_amt}
        ORDER BY a.amount_20d DESC
        LIMIT {top_n}
    """
    with st.spinner("검색 중..."):
        try:
            result = db.query(sql)
        except Exception as e:
            st.error(f"쿼리 오류: {e}")
            return

    if result.empty:
        st.warning("조건에 맞는 종목 없음.")
        return

    state.set("screener_result", result)
    st.success(f"{len(result)}개 종목")
    st.dataframe(result, width="stretch", hide_index=True)
    csv = result.to_csv(index=False, encoding="utf-8-sig")
    st.download_button("📥 CSV", csv, "screener.csv", "text/csv")

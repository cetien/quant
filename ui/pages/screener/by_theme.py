"""ui/pages/screener/by_theme.py — 테마별 스크리너"""
import streamlit as st
from storage.db_manager import DuckDBManager
import ui.state as state


def render(db: DuckDBManager) -> None:
    themes = db.query(
        "SELECT theme_id, name FROM themes WHERE is_active = TRUE ORDER BY name"
    )
    if themes.empty:
        st.info("등록된 테마 없음 — [관리 > 테마 관리]에서 추가하세요.")
        return

    opts = dict(zip(themes["name"], themes["theme_id"]))
    sel  = st.selectbox("테마 선택", list(opts.keys()))
    tid  = opts[sel]

    markets = state.get("scr_market") or ["KOSPI", "KOSDAQ"]
    market_str = ", ".join(f"'{m}'" for m in markets)

    sql = f"""
        SELECT v.ticker, v.name, v.market, v.theme_name,
               v.weight, v.confidence, v.source,
               dp.close,
               ROUND(a.amount_20d / 1e8, 1) AS "거래대금(억)"
        FROM v_active_theme_map v
        JOIN daily_prices dp ON dp.ticker = v.ticker
            AND dp.date = (SELECT MAX(date) FROM daily_prices)
        LEFT JOIN (
            SELECT ticker, AVG(amount) AS amount_20d
            FROM daily_prices
            WHERE date >= CURRENT_DATE - INTERVAL 20 DAY
            GROUP BY ticker
        ) a ON a.ticker = v.ticker
        WHERE v.theme_id = {tid}
          AND v.market IN ({market_str})
        ORDER BY v.weight DESC
    """
    with st.spinner("조회 중..."):
        try:
            df = db.query(sql)
        except Exception as e:
            st.error(f"오류: {e}")
            return

    if df.empty:
        st.info("해당 테마에 매핑된 종목 없음.")
        return

    st.success(f"{len(df)}개 종목")
    st.dataframe(df, width="stretch", hide_index=True)
    st.download_button("📥 CSV", df.to_csv(index=False, encoding="utf-8-sig"),
                       f"theme_{sel}.csv", "text/csv")

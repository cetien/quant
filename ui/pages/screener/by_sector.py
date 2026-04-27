"""ui/pages/screener/by_sector.py — 섹터별 스크리너"""
import streamlit as st
from storage.db_manager import DuckDBManager
import ui.state as state


def render(db: DuckDBManager) -> None:
    sectors = db.query("SELECT id, name FROM sectors ORDER BY name")
    if sectors.empty:
        st.info("섹터 데이터 없음 — tools/import_sectors.py로 KRX CSV를 먼저 입력하세요.")
        return

    opts = dict(zip(sectors["name"], sectors["id"]))
    sel  = st.selectbox("섹터 선택", list(opts.keys()))
    sid  = opts[sel]

    markets    = state.get("scr_market") or ["KOSPI", "KOSDAQ"]
    market_str = ", ".join(f"'{m}'" for m in markets)
    top_n      = int(state.get("scr_top_n") or 50)

    sql = f"""
        SELECT s.ticker, s.name, s.market,
               ssm.weight AS sector_weight,
               dp.close,
               ROUND(a.amount_20d / 1e8, 1) AS "거래대금(억)"
        FROM stock_sector_map ssm
        JOIN stocks s ON s.ticker = ssm.ticker
        JOIN daily_prices dp ON dp.ticker = s.ticker
            AND dp.date = (SELECT MAX(date) FROM daily_prices)
        LEFT JOIN (
            SELECT ticker, AVG(amount) AS amount_20d
            FROM daily_prices
            WHERE date >= CURRENT_DATE - INTERVAL 20 DAY
            GROUP BY ticker
        ) a ON a.ticker = s.ticker
        WHERE ssm.sector_id = {sid}
          AND s.market IN ({market_str})
          AND s.is_active = TRUE
        ORDER BY ssm.weight DESC, a.amount_20d DESC NULLS LAST
        LIMIT {top_n}
    """
    with st.spinner("조회 중..."):
        try:
            df = db.query(sql)
        except Exception as e:
            st.error(f"오류: {e}")
            return

    if df.empty:
        st.info("해당 섹터에 종목 없음.")
        return

    st.success(f"{len(df)}개 종목")
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.download_button("📥 CSV", df.to_csv(index=False, encoding="utf-8-sig"),
                       f"sector_{sel}.csv", "text/csv")

"""ui/pages/admin/sector_mgmt.py — 섹터 관리"""
import streamlit as st

import ui.state as state
from ingestion.price_collector import PriceCollector
from storage.db_manager import DuckDBManager


def render(db: DuckDBManager) -> None:
    sectors = db.query("SELECT id, name, rating FROM sectors ORDER BY name")

    if sectors.empty:
        st.info(
            "섹터 데이터 없음.  \n"
            "```\npython tools/import_sectors.py --csv <KRX_CSV_경로>\n```"
        )
        return

    st.caption(f"총 {len(sectors)}개 섹터 (KRX CSV 기준)")
    st.dataframe(sectors, width="stretch", hide_index=True)

    sec_opts = dict(zip(sectors["name"], sectors["id"]))
    sector_names = list(sec_opts.keys())
    edit_group_id = state.get("edit_group_id")
    edit_group_type = state.get("edit_group_type")
    preferred_sector_id = None
    if edit_group_type == "sector" and edit_group_id:
        try:
            preferred_sector_id = int(str(edit_group_id).replace("sec_", ""))
        except ValueError:
            preferred_sector_id = None

    default_index = 0
    if preferred_sector_id is not None:
        for idx, sector_name in enumerate(sector_names):
            if sec_opts[sector_name] == preferred_sector_id:
                default_index = idx
                break

    st.divider()
    st.markdown("**섹터 검색 우선순위(rating) 수정**")
    sel = st.selectbox("섹터 선택", sector_names, index=default_index, key="sec_sel")
    cur_rating = int(sectors.loc[sectors["name"] == sel, "rating"].iloc[0])
    new_rating = st.number_input(
        "rating (높을수록 검색 우선순위 높음)",
        min_value=0,
        value=cur_rating,
        step=1,
    )
    if st.button("저장", key="sec_save"):
        db.con.execute("UPDATE sectors SET rating=? WHERE id=?", [new_rating, sec_opts[sel]])
        st.success(f"'{sel}' rating → {new_rating} 저장 완료.")
        st.rerun()

    st.divider()
    st.markdown("**종목 섹터 매핑 조회**")
    sel2 = st.selectbox("섹터", sector_names, index=default_index, key="sec_map_sel")
    df = db.query(
        f"""
        SELECT s.ticker, s.name, s.market, ssm.weight
        FROM stock_sector_map ssm
        JOIN stocks s ON s.ticker = ssm.ticker
        WHERE ssm.sector_id = {sec_opts[sel2]}
        ORDER BY ssm.weight DESC
    """
    )
    st.caption(f"{len(df)}개 종목")
    st.dataframe(df, width="stretch", hide_index=True)

    st.divider()
    st.markdown("**📥 가격 데이터 수집**")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("⬆ 증분 업데이트 (이 섹터 전체)", width="stretch", key="sec_incr"):
            tickers = df["ticker"].tolist() if not df.empty else []
            if not tickers:
                st.warning("매핑된 종목이 없습니다.")
            else:
                with st.spinner(f"{len(tickers)}개 종목 증분 수집 중..."):
                    loaded = PriceCollector(db).incremental_update_daily_prices(tickers=tickers)
                st.success(f"완료: {loaded}건 적재")
    with c2:
        if st.button("⬇ 소급 수집 (이 섹터 전체)", width="stretch", key="sec_backfill"):
            tickers = df["ticker"].tolist() if not df.empty else []
            if not tickers:
                st.warning("매핑된 종목이 없습니다.")
            else:
                with st.spinner(f"{len(tickers)}개 종목 소급 수집 중..."):
                    loaded = PriceCollector(db).backfill_daily_prices(tickers=tickers)
                st.success(f"완료: {loaded}건 적재")

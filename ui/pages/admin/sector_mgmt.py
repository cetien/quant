"""ui/pages/admin/sector_mgmt.py — 섹터 관리"""
import streamlit as st
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

    st.divider()
    st.markdown("**섹터 검색 우선순위(rating) 수정**")
    sec_opts = dict(zip(sectors["name"], sectors["id"]))
    sel = st.selectbox("섹터 선택", list(sec_opts.keys()), key="sec_sel")
    cur_rating = int(sectors.loc[sectors["name"] == sel, "rating"].iloc[0])
    new_rating = st.number_input("rating (높을수록 검색 우선순위 높음)",
                                 min_value=0, value=cur_rating, step=1)
    if st.button("저장", key="sec_save"):
        db.con.execute("UPDATE sectors SET rating=? WHERE id=?",
                       [new_rating, sec_opts[sel]])
        st.success(f"'{sel}' rating → {new_rating} 저장 완료.")
        st.rerun()

    st.divider()
    st.markdown("**종목 섹터 매핑 조회**")
    sel2 = st.selectbox("섹터", list(sec_opts.keys()), key="sec_map_sel")
    df = db.query(f"""
        SELECT s.ticker, s.name, s.market, ssm.weight
        FROM stock_sector_map ssm
        JOIN stocks s ON s.ticker = ssm.ticker
        WHERE ssm.sector_id = {sec_opts[sel2]}
        ORDER BY ssm.weight DESC
    """)
    st.caption(f"{len(df)}개 종목")
    st.dataframe(df, width="stretch", hide_index=True)

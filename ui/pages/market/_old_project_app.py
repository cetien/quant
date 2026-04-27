import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st
import os
import platform
import subprocess

from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

# ---------------------------
# DB
# ---------------------------
DB_PATH = Path(__file__).resolve().parents[1] / "db" / "reports.db"
# 외부 마스터 DB 경로 (환경에 맞게 수정하세요)
MASTER_DB_PATH = Path(__file__).resolve().parents[2] / "otherProject" / "db" / "master_sectors.db"

@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    
    master_exists = MASTER_DB_PATH.exists()
    
    if master_exists:
        # 외부 DB를 'master'라는 별칭으로 연결
        conn.execute(f"ATTACH DATABASE '{MASTER_DB_PATH}' AS master")
        query = """
        SELECT 
            p.id, p.date, 
            IFNULL(ms.name, '미분류') as sector,
            c.name as company, 
            p.title, p.writer, p.filepath
        FROM pdf_reports p
        JOIN companies c ON p.company_id = c.id
        LEFT JOIN master.company_sectors mcs ON c.id = mcs.company_id
        LEFT JOIN master.sectors ms ON mcs.sector_id = ms.id
        ORDER BY p.date DESC
        """
    else:
        # 마스터 DB가 없을 경우의 폴백(Fallback) 쿼리
        query = """
        SELECT 
            p.id, p.date, 
            '미분류' as sector,
            c.name as company, 
            p.title, p.writer, p.filepath
        FROM pdf_reports p
        JOIN companies c ON p.company_id = c.id
        ORDER BY p.date DESC
        """

    df = pd.read_sql(query, conn)
    conn.close()

    # 안정성 처리
    df['date'] = df['date'].astype(str)
    df = df.fillna("")

    return df


def open_pdf(path):
    if not Path(path).exists():
        st.error(f"파일 없음: {path}")
        return

    if platform.system() == "Windows":
        os.startfile(path)
    elif platform.system() == "Darwin":
        subprocess.call(["open", path])
    else:
        subprocess.call(["xdg-open", path])


# ---------------------------
# UI
# ---------------------------
st.set_page_config(layout="wide")
st.title("📊 Report Analyzer")

df = load_data()

if df.empty:
    st.warning("데이터 없음")
    st.stop()

with st.sidebar:
    st.header("🏆 Ranking")

    st.subheader("Company Top 5")
    st.table(df['company'].value_counts().head(5))

    st.subheader("Sector Top 5")
    st.table(df['sector'].value_counts().head(5))

# Grid에 표시할 데이터 (filepath 제거)
df_view = df.drop(columns=["filepath"])

# ---------------------------
# JS renderer (title → 링크 스타일)
# ---------------------------
cell_renderer = JsCode("""
function(params) {
    return `<span style="
        color:#1f77b4;
        text-decoration: underline;
        cursor: pointer;
    ">${params.value}</span>`;
}
""")

# ---------------------------
# Grid 설정
# ---------------------------
gb = GridOptionsBuilder.from_dataframe(df_view)

gb.configure_default_column(
    resizable=True,
    sortable=True,
    groupable=True,
    filter=True
)

gb.configure_column("id", hide=True)
gb.configure_column("date", headerName="날짜", width=120)
gb.configure_column("sector", headerName="섹터", width=150)
gb.configure_column("company", headerName="종목", width=150)

gb.configure_column(
    "title",
    headerName="리포트 제목"
    #cellRenderer=cell_renderer
)

gb.configure_selection("single")

grid_options = gb.build()

# ---------------------------
# Grid 렌더링
# ---------------------------
response = AgGrid(
    df_view,
    gridOptions=grid_options,
    allow_unsafe_jscode=True,
    height=700,
    theme="streamlit",
    #enable_enterprise_modules=True,
    key="main_grid"
)

# ---------------------------
# 클릭 → PDF 실행 (중복 방지 포함)
# ---------------------------
selected = response.get("selected_rows", [])

if selected:
    report_id = selected[0]["id"]

    # 정렬/필터 시 중복 실행 방지
    if st.session_state.get("last_clicked") != report_id:
        st.session_state["last_clicked"] = report_id

        target_path = df[df["id"] == report_id]["filepath"].iloc[0]
        open_pdf(target_path)
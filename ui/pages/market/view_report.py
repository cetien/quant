"""ui/pages/market/view_report.py — 뷰 리포트"""
import os
import platform
import subprocess
from pathlib import Path

import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder

from storage.db_manager import DuckDBManager

def open_pdf(path: str):
    """시스템 기본 앱으로 PDF 열기"""
    if not Path(path).exists():
        st.error(f"파일을 찾을 수 없습니다: {path}")
        return
    if platform.system() == "Windows":
        os.startfile(path)
    elif platform.system() == "Darwin":
        subprocess.call(["open", path])
    else:
        subprocess.call(["xdg-open", path])

def render(db: DuckDBManager) -> None:
    st.subheader("📋 시장 뷰 리포트")

    # 1. DuckDB를 사용하여 리포트와 섹터 정보 조인 쿼리
    # pdf_reports 테이블이 DuckDB 내에 있다고 가정합니다.
    # 종목명(company)을 기준으로 stocks와 조인하여 primary sector를 가져옵니다.
    query = """
    SELECT 
        p.id, 
        p.date, 
        s.name as company,
        vps.sector as sector,
        p.title, 
        p.writer, 
        p.filepath
    FROM pdf_reports p
    LEFT JOIN stocks s ON p.ticker = s.ticker
    LEFT JOIN v_stock_primary_sector vps ON s.ticker = vps.ticker
    ORDER BY p.date DESC
    """
    
    try:
        df = db.query(query)
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        st.info("💡 'pdf_reports' 테이블이 DuckDB에 생성되어 있는지 확인하세요.")
        return

    if df.empty:
        st.warning("조회된 리포트 데이터가 없습니다.")
        return

    # 2. 상단 요약 통계
    col1, col2 = st.columns(2)
    with col1:
        st.caption("🏆 종목별 리포트 빈도 Top 5")
        st.dataframe(df['company'].value_counts().head(5), width="stretch", hide_index=False)
    with col2:
        st.caption("📂 섹터별 리포트 빈도 Top 5")
        st.dataframe(df['sector'].value_counts().head(5), width="stretch", hide_index=False)

    st.divider()

    # 3. AgGrid 설정
    df_view = df.drop(columns=["filepath"])
    gb = GridOptionsBuilder.from_dataframe(df_view)
    gb.configure_default_column(resizable=True, sortable=True, filter=True, groupable=True)
    gb.configure_column("id", hide=True)
    gb.configure_column("date", headerName="날짜", width=100)
    
    # '섹터'와 '종목' 컬럼에 그룹화 기능 활성화
    gb.configure_column("sector", headerName="섹터", width=150, enableRowGroup=True)
    gb.configure_column("company", headerName="종목", width=120, rowGroup=True, enableRowGroup=True)
    
    gb.configure_column("title", headerName="리포트 제목", flex=1)
    gb.configure_selection("single")
    
    gb.configure_grid_options(
        rowGroupPanelShow='always', # 헤더 상단에 'Group By' 패널(Drag columns here) 활성화
        groupDisplayType='multipleColumns', # 그룹화된 컬럼을 별도의 컬럼으로 표시
        enableRangeSelection=True, # 셀 범위 선택 활성화
    )
    grid_options = gb.build()

    response = AgGrid(
        df_view,
        gridOptions=grid_options,
        height=500,
        theme="streamlit",
        enable_enterprise_modules=True, # Group By 기능을 위해 필수 활성화
        key="report_grid"
    )

    # 4. 행 선택 시 PDF 열기
    selected = response.get("selected_rows")

    # AgGrid의 선택 상태가 None일 수 있으므로 안전하게 처리
    if selected is None:
        has_selection = False
    elif isinstance(selected, pd.DataFrame):
        has_selection = not selected.empty
    else:
        has_selection = len(selected) > 0

    if has_selection:
        # 선택된 행 데이터 추출
        selected_row = selected.iloc[0] if isinstance(selected, pd.DataFrame) else selected[0]
        report_id = selected_row["id"]

        try:
            target_path = df[df["id"] == int(report_id)]["filepath"].iloc[0]
            if st.button(f"📄 리포트 열기: {selected_row['title']}"):
                open_pdf(target_path)
        except (IndexError, KeyError):
            st.error(f"리포트(ID: {report_id})의 경로를 찾을 수 없습니다.")

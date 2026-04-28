"""ui/pages/market/view_report.py — 뷰 리포트"""
import os
import platform
import subprocess
from pathlib import Path

import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
from ui.pages.market._old_project_ingestion import ingest, DEFAULT_PDF_DIR, DEFAULT_LOG_PATH

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

    # 페이지 활성화 시 신규 리포트 자동 인제션 (세션당 1회 실행)
    if "report_ingested" not in st.session_state:
        with st.spinner("신규 리포트 파일을 스캔하고 DB를 업데이트 중입니다..."):
            try:
                ingest(DEFAULT_PDF_DIR, DEFAULT_LOG_PATH)
                st.session_state["report_ingested"] = True
            except Exception as e:
                st.error(f"리포트 자동 수집 중 오류 발생: {e}")

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

    # 날짜 형식을 YYYY-MM-DD 문자열로 변환 (AgGrid 표시 형식 고정)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

    # 필터 상태 관리
    f_col = st.session_state.get("report_filter_col")
    f_val = st.session_state.get("report_filter_val")

    # 2. 상단 요약 통계
    col1, col2 = st.columns(2)
    with col1:
        st.caption("🏆 종목별 리포트 빈도 Top 5 (클릭 시 필터링)")
        stock_counts = df['company'].value_counts().head(5).reset_index()
        stock_counts.columns = ['종목', '리포트수']
        # on_select를 통해 클릭 감지 (Streamlit 1.35+ 필요)
        sel_s = st.dataframe(
            stock_counts, use_container_width=True, hide_index=True,
            on_select="rerun", selection_mode="single-row"
        )
        if sel_s['selection']['rows']:
            idx = sel_s['selection']['rows'][0]
            st.session_state["report_filter_col"] = "company"
            st.session_state["report_filter_val"] = stock_counts.iloc[idx]['종목']
            st.rerun()

    with col2:
        st.caption("📂 섹터별 리포트 빈도 Top 5 (클릭 시 필터링)")
        sector_counts = df['sector'].value_counts().head(5).reset_index()
        sector_counts.columns = ['섹터', '리포트수']
        sel_sec = st.dataframe(
            sector_counts, use_container_width=True, hide_index=True,
            on_select="rerun", selection_mode="single-row"
        )
        if sel_sec['selection']['rows']:
            idx = sel_sec['selection']['rows'][0]
            st.session_state["report_filter_col"] = "sector"
            st.session_state["report_filter_val"] = sector_counts.iloc[idx]['섹터']
            st.rerun()

    # 필터가 적용된 경우 해제 버튼 표시
    if f_col and f_val:
        if st.button(f"🔄 전체 보기 (현재 필터: {f_val})", use_container_width=True):
            st.session_state["report_filter_col"] = None
            st.session_state["report_filter_val"] = None
            st.rerun()
        # 그리드용 데이터 필터링
        df = df[df[f_col] == f_val]

    st.divider()

    # 3. AgGrid 설정
    df_view = df.drop(columns=["filepath"])
    gb = GridOptionsBuilder.from_dataframe(df_view)
    gb.configure_default_column(resizable=True, sortable=True, filter=True, groupable=True)
    gb.configure_column("id", hide=True)
    gb.configure_column("date", headerName="날짜", width=100)
    
    # 필터링 대상에 따라 그룹화 우선순위 변경
    is_sector_mode = (f_col == "sector")
    gb.configure_column("sector", headerName="섹터", width=150, 
                        rowGroup=is_sector_mode, enableRowGroup=True)
    gb.configure_column("company", headerName="종목", width=120, 
                        rowGroup=not is_sector_mode, enableRowGroup=True)
    
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

        # 새로운 리포트가 선택되었을 때만 자동 실행 (무한 루프 방지)
        if st.session_state.get("last_opened_report_id") != report_id:
            st.session_state["last_opened_report_id"] = report_id
            try:
                target_path = df[df["id"] == report_id]["filepath"].iloc[0]
                open_pdf(target_path)
                st.toast(f"리포트 실행: {selected_row['title']}")
            except (IndexError, KeyError):
                st.error(f"리포트(ID: {report_id})의 경로를 찾을 수 없습니다.")

        # 5. 리포트 삭제 기능 (파일 시스템 및 DB에서 물리적 삭제)
        if st.button(f"🗑️ 리포트 삭제: {selected_row['title']}", type="secondary", use_container_width=True):
            try:
                target_path = Path(df[df["id"] == report_id]["filepath"].iloc[0])
                # 1) 실제 파일 삭제
                if target_path.exists():
                    target_path.unlink()
                # 2) DB 레코드 삭제
                db.execute("DELETE FROM pdf_reports WHERE id = ?", [int(report_id)])
                # 3) 상태 초기화 및 새로고침
                st.session_state.pop("last_opened_report_id", None)
                st.rerun()
            except Exception as e:
                st.error(f"삭제 중 오류 발생: {e}")

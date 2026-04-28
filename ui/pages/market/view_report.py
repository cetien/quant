"""ui/pages/market/view_report.py — 뷰 리포트"""
import os
import platform
import subprocess
from pathlib import Path

import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

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


def render_report_grid(df: pd.DataFrame, key: str, height: int = 500, 
                       f_col: str = None):
    """AgGrid 리포트 목록 그리드 렌더링 (공용)"""
    df_view = df.drop(columns=["filepath"]) if "filepath" in df.columns else df

    gb = GridOptionsBuilder.from_dataframe(df_view)
    gb.configure_default_column(resizable=True, sortable=True, filter=True, groupable=True)
    gb.configure_column("id", hide=True)
    gb.configure_column("date",    headerName="날짜",       width=100)
    gb.configure_column("writer",  headerName="작성자",     width=100)
    gb.configure_column("title",   headerName="리포트 제목", flex=1)

    # 그룹화 기준: 종목 필터 → company rowGroup / 섹터 필터 → sector rowGroup / 전체 → 없음
    company_group = (f_col == "company")
    sector_group  = (f_col == "sector")

    if "company" in df_view.columns:
        gb.configure_column(
            "company", headerName="종목", width=120,
            rowGroup=company_group, hide=company_group, enableRowGroup=True,
        )
    if "sector" in df_view.columns:
        gb.configure_column(
            "sector", headerName="섹터", width=150,
            rowGroup=sector_group, hide=sector_group, enableRowGroup=True,
        )

    gb.configure_selection("single", use_checkbox=False)
    gb.configure_grid_options(
        rowGroupPanelShow="always",
        groupDisplayType="multipleColumns",
        enableRangeSelection=True,
        animateRows=True,
        groupDefaultExpanded=1 if (company_group or sector_group) else 0,
    )

    return AgGrid(
        df_view,
        gridOptions=gb.build(),
        height=height,
        theme="streamlit",
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        enable_enterprise_modules=True,
        key=key,
    )


def handle_report_selection(db: DuckDBManager, df: pd.DataFrame, selected_rows):
    """그리드 선택 이벤트 처리 (파일 열기/삭제)"""
    if selected_rows is None:
        has_selection = False
    elif isinstance(selected_rows, pd.DataFrame):
        has_selection = not selected_rows.empty
    else:
        has_selection = len(selected_rows) > 0

    if has_selection:
        selected_row = selected_rows.iloc[0] if isinstance(selected_rows, pd.DataFrame) else selected_rows[0]
        report_id = selected_row["id"]

        # 자동 열기
        if st.session_state.get("last_opened_report_id") != report_id:
            st.session_state["last_opened_report_id"] = report_id
            try:
                target_path = df[df["id"] == report_id]["filepath"].iloc[0]
                open_pdf(target_path)
                st.toast(f"리포트 실행: {selected_row['title']}")
            except (IndexError, KeyError):
                st.error(f"리포트(ID: {report_id})의 경로를 찾을 수 없습니다.")

        # 삭제 버튼
        if st.button(f"🗑️ 리포트 삭제: {selected_row['title']}", type="secondary",
                     use_container_width=True):
            try:
                target_path_row = df[df["id"] == report_id]
                if not target_path_row.empty:
                    target_path = Path(target_path_row["filepath"].iloc[0])
                    if target_path.exists():
                        target_path.unlink()
                    db.execute("DELETE FROM pdf_reports WHERE id = ?", [int(report_id)])
                    st.session_state.pop("last_opened_report_id", None)
                    st.rerun()
                else:
                    st.error("삭제할 리포트 경로를 찾을 수 없습니다.")
            except Exception as e:
                st.error(f"삭제 중 오류 발생: {e}")


def render(db: DuckDBManager) -> None:
    # 세션당 1회 자동 인제션
    if "report_ingested" not in st.session_state:
        with st.spinner("신규 리포트 파일을 스캔하고 DB를 업데이트 중입니다..."):
            try:
                ingest(DEFAULT_PDF_DIR, DEFAULT_LOG_PATH)
                st.session_state["report_ingested"] = True
            except Exception as e:
                st.error(f"리포트 자동 수집 중 오류 발생: {e}")

    # ── 데이터 로드 ───────────────────────────────────────────────────────────
    query = """
    SELECT
        p.id,
        p.date,
        s.name  AS company,
        vps.sector,
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

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # ── 제목 + 요약 통계 ──────────────────────────────────────────────────────
    total    = len(df)
    date_min = df["date"].min()
    date_max = df["date"].max()

    title_col, btn_col = st.columns([8, 1])
    with title_col:
        st.subheader(f"📋 시장 뷰 리포트 · {total}건  ({date_min} ~ {date_max})")
    with btn_col:
        if st.button("🔄", help="PDF 폴더를 다시 스캔하여 신규 리포트를 반영합니다.",
                     key="report_refresh_btn"):
            with st.spinner("스캔 중…"):
                try:
                    ingest(DEFAULT_PDF_DIR, DEFAULT_LOG_PATH)
                except Exception as e:
                    st.error(f"수집 오류: {e}")
            # 인제션 캐시 + 그리드 선택 상태 초기화 후 rerun
            st.session_state.pop("report_ingested", None)
            st.session_state.pop("last_opened_report_id", None)
            st.rerun()

    # ── 필터 상태 ─────────────────────────────────────────────────────────────
    f_col = st.session_state.get("report_filter_col")
    f_val = st.session_state.get("report_filter_val")


    # ── 상단 요약 통계 ────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.caption("🏆 종목별 리포트 빈도 Top 5")
        stock_counts = df["company"].value_counts().head(5).reset_index()
        stock_counts.columns = ["종목", "리포트수"]

        for _, row in stock_counts.iterrows():
            label = f"{row['종목']}  ({row['리포트수']}건)"
            is_active = (f_col == "company" and f_val == row["종목"])
            btn_type = "primary" if is_active else "secondary"
            if st.button(label, key=f"stk_top_{row['종목']}", use_container_width=True,
                         type=btn_type):
                if is_active:
                    # 이미 선택된 종목 재클릭 → 필터 해제
                    st.session_state["report_filter_col"] = None
                    st.session_state["report_filter_val"] = None
                else:
                    st.session_state["report_filter_col"] = "company"
                    st.session_state["report_filter_val"] = row["종목"]
                st.rerun()

    with col2:
        st.caption("📂 섹터별 리포트 빈도 Top 5")
        sector_counts = df["sector"].value_counts().head(5).reset_index()
        sector_counts.columns = ["섹터", "리포트수"]

        for _, row in sector_counts.iterrows():
            label = f"{row['섹터']}  ({row['리포트수']}건)"
            is_active = (f_col == "sector" and f_val == row["섹터"])
            btn_type = "primary" if is_active else "secondary"
            if st.button(label, key=f"sec_top_{row['섹터']}", use_container_width=True,
                         type=btn_type):
                if is_active:
                    st.session_state["report_filter_col"] = None
                    st.session_state["report_filter_val"] = None
                else:
                    st.session_state["report_filter_col"] = "sector"
                    st.session_state["report_filter_val"] = row["섹터"]
                st.rerun()

    # 필터 해제 버튼 (상단 버튼 재클릭으로도 해제 가능하나 명시적으로도 제공)
    if f_col and f_val:
        st.info(f"필터 적용 중: **{f_val}**")
        if st.button("🔄 전체 보기", use_container_width=True):
            st.session_state["report_filter_col"] = None
            st.session_state["report_filter_val"] = None
            st.rerun()

    st.divider()


    # ── AgGrid 그리드 ─────────────────────────────────────────────────────────
    # 필터 적용 시 해당 값으로 df 축소 (그룹 모드는 그리드 내부에서 처리)
    df_grid = df.copy()
    if f_col and f_val:
        df_grid = df_grid[df_grid[f_col] == f_val]

    response = render_report_grid(
        df_grid,
        key=f"report_grid_{f_col}_{f_val}",
        f_col=f_col
    )

    # ── 행 선택 시 PDF 열기 + 삭제 ───────────────────────────────────────────
    handle_report_selection(db, df_grid, response.get("selected_rows"))

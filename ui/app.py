"""ui/app.py
Streamlit 메인 진입점.
상단 탭(st.tabs) 기반 내비게이션 + 공통 사이드바.
실행: streamlit run ui/app.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

import ui.state as state
from storage.db_manager import DuckDBManager
from ui.sidebar import render as render_sidebar

TOP_TABS = ["시장 개요", "스크리너", "종목 분석", "백테스트", "관리"]
ADMIN_TABS = ["데이터 수집", "테마 관리", "섹터 관리", "데이터 탐색", "설정"]


def _move_first(items: list[str], target: str | None) -> list[str]:
    if not target or target not in items:
        return items[:]
    return [target] + [item for item in items if item != target]


st.set_page_config(
    page_title="Quant Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

state.init()


@st.cache_resource
def get_db() -> DuckDBManager:
    return DuckDBManager()


db = get_db()

with st.sidebar:
    render_sidebar(state.get("active_tab"), db)

tabs = {name: tab for name, tab in zip(TOP_TABS, st.tabs(TOP_TABS))}

with tabs["시장 개요"]:
    state.set("active_tab", "시장 개요")
    from ui.pages.market import dashboard, theme_ranking, view_report

    inner = st.tabs(["매크로 대시보드", "테마 랭킹", "Report View"])
    with inner[0]:
        dashboard.render(db)
    with inner[1]:
        theme_ranking.render(db)
    with inner[2]:
        view_report.render(db)

with tabs["스크리너"]:
    state.set("active_tab", "스크리너")
    from ui.pages.screener import by_sector, by_theme, condition

    inner = st.tabs(["조건식", "테마별", "섹터별"])
    with inner[0]:
        condition.render(db)
    with inner[1]:
        by_theme.render(db)
    with inner[2]:
        by_sector.render(db)

with tabs["종목 분석"]:
    state.set("active_tab", "종목 분석")
    from ui.pages.stock import chart, factor_score, financial, supply

    inner = st.tabs(["차트", "팩터 스코어", "수급", "재무"])
    with inner[0]:
        chart.render(db)
    with inner[1]:
        factor_score.render(db)
    with inner[2]:
        supply.render(db)
    with inner[3]:
        financial.render(db)

with tabs["백테스트"]:
    state.set("active_tab", "백테스트")
    from ui.pages.backtest import main as bt_main

    bt_main.render(db)

with tabs["관리"]:
    state.set("active_tab", "관리")
    from ui.pages.admin import data_mgmt, explorer, sector_mgmt, settings, theme_mgmt

    focus = state.get("admin_focus")
    focus_map = {"theme": "테마 관리", "sector": "섹터 관리"}
    ordered_admin_tabs = _move_first(ADMIN_TABS, focus_map.get(focus))
    inner_tabs = {
        name: tab for name, tab in zip(ordered_admin_tabs, st.tabs(ordered_admin_tabs))
    }

    if "데이터 수집" in inner_tabs:
        with inner_tabs["데이터 수집"]:
            data_mgmt.render(db)
    if "테마 관리" in inner_tabs:
        with inner_tabs["테마 관리"]:
            theme_mgmt.render(db)
    if "섹터 관리" in inner_tabs:
        with inner_tabs["섹터 관리"]:
            sector_mgmt.render(db)
    if "데이터 탐색" in inner_tabs:
        with inner_tabs["데이터 탐색"]:
            explorer.render(db)
    if "설정" in inner_tabs:
        with inner_tabs["설정"]:
            settings.render(db)

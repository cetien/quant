"""
ui/app.py
Streamlit 메인 진입점.
상단 탭(st.tabs) 기반 네비게이션 + 공통 사이드바.
실행: streamlit run ui/app.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

import ui.state as state
from ui.sidebar import render as render_sidebar
from storage.db_manager import DuckDBManager

st.set_page_config(
    page_title="Quant Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 세션 초기화 ───────────────────────────────────────────────────────────────
state.init()

# ── DB 연결 (앱 수명 동안 1회) ────────────────────────────────────────────────
@st.cache_resource
def get_db() -> DuckDBManager:
    return DuckDBManager()

db = get_db()

# ── 사이드바 ─────────────────────────────────────────────────────────────────
TAB_NAMES = ["📊 시장 개요", "🔍 스크리너", "🔬 종목 분석", "🧪 백테스트", "⚙️ 관리"]
_LABEL_MAP = {
    "📊 시장 개요": "시장 개요",
    "🔍 스크리너":  "스크리너",
    "🔬 종목 분석": "종목 분석",
    "🧪 백테스트":  "백테스트",
    "⚙️ 관리":      "관리",
}

with st.sidebar:
    render_sidebar(state.get("active_tab"), db)

# ── 상단 탭 ───────────────────────────────────────────────────────────────────
tabs = st.tabs(TAB_NAMES)

# 탭 인덱스를 session_state의 active_tab과 동기화하는 JS 없이
# 각 탭 블록 안에서 active_tab을 갱신한다 (Streamlit 탭은 자체 선택 상태 관리).

with tabs[0]:
    state.set("active_tab", "시장 개요")
    from ui.pages.market import dashboard, theme_ranking, view_report
    inner = st.tabs(["📊 매크로 대시보드", "🔥 테마 랭킹", "📋 Report View"])
    with inner[0]: 
        dashboard.render(db)
    with inner[1]: 
        theme_ranking.render(db)
    with inner[2]:
        view_report.render(db)

with tabs[1]:
    state.set("active_tab", "스크리너")
    from ui.pages.screener import condition, by_theme, by_sector
    inner = st.tabs(["조건식", "테마별", "섹터별"])
    with inner[0]: condition.render(db)
    with inner[1]: by_theme.render(db)
    with inner[2]: by_sector.render(db)

with tabs[2]:
    state.set("active_tab", "종목 분석")
    from ui.pages.stock import chart, factor_score, supply, financial
    inner = st.tabs(["차트", "팩터 스코어", "수급", "재무"])
    with inner[0]: chart.render(db)
    with inner[1]: factor_score.render(db)
    with inner[2]: supply.render(db)
    with inner[3]: financial.render(db)

with tabs[3]:
    state.set("active_tab", "백테스트")
    from ui.pages.backtest import main as bt_main
    bt_main.render(db)

with tabs[4]:
    state.set("active_tab", "관리")
    from ui.pages.admin import data_mgmt, theme_mgmt, sector_mgmt, explorer, settings
    inner = st.tabs(["📥 데이터 수집", "🎯 테마 관리", "🏭 섹터 관리", "📂 데이터 탐색", "🔧 설정"])
    with inner[0]: data_mgmt.render(db)
    with inner[1]: theme_mgmt.render(db)
    with inner[2]: sector_mgmt.render(db)
    with inner[3]: explorer.render(db)
    with inner[4]: settings.render(db)

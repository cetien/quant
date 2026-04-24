"""
ui/app.py
Streamlit 메인 진입점 및 페이지 라우팅
실행: streamlit run ui/app.py
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

st.set_page_config(
    page_title="Quant Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 사이드바 네비게이션 ───────────────────────────────────────────────────────

PAGES = {
    "📊 대시보드":      "ui/pages/dashboard.py",
    "🔍 스크리너":      "ui/pages/scanner.py",
    "🔬 Deep Dive":    "ui/pages/deep_dive.py",
    "🧪 백테스트":      "ui/pages/backtest_ui.py",
    "⚙️ 데이터 관리":   "ui/pages/settings.py",
}

with st.sidebar:
    st.title("📈 Quant Platform")
    st.markdown("---")
    page_name = st.radio("메뉴", list(PAGES.keys()))

# ── 페이지 라우팅 ────────────────────────────────────────────────────────────

if page_name == "📊 대시보드":
    from ui.pages import dashboard
    dashboard.render()

elif page_name == "🔍 스크리너":
    from ui.pages import scanner
    scanner.render()

elif page_name == "🔬 Deep Dive":
    from ui.pages import deep_dive
    deep_dive.render()

elif page_name == "🧪 백테스트":
    from ui.pages import backtest_ui
    backtest_ui.render()

elif page_name == "⚙️ 데이터 관리":
    from ui.pages import settings
    settings.render()

"""
ui/state.py
st.session_state 키 정의 및 초기화 헬퍼.
모든 페이지에서 이 모듈을 통해 상태에 접근하면 키 충돌을 방지한다.
"""
import streamlit as st


# ── 기본값 정의 ───────────────────────────────────────────────────────────────

_DEFAULTS: dict = {
    # 네비게이션
    "active_tab":           "시장 개요",   # 상단 탭 현재 위치
    # 워치리스트
    "watchlist":            [],            # List[ticker]
    # 종목 분석 컨텍스트
    "selected_ticker":      None,          # 사이드바에서 선택된 종목
    "stock_period":         "1Y",
    # 스크리너 마지막 결과 (탭 이동 후 복귀 시 유지)
    "screener_result":      None,
    # DB 상태 캐시 (사이드바 위젯용)
    "db_status":            None,
    "db_status_ts":         None,          # 마지막 조회 timestamp
    # side panel — gridGroup
    "selected_group_id":    None,          # 선택된 group_id (str)
    "edit_group_id":        None,          # 편집 대상 group_id
    "edit_group_type":      None,          # 'sector' | 'theme'
    # side panel — price update 트리거
    "trigger_price_update": None,          # ticker str; chart.py에서 감지 후 None으로 리셋
    # 매크로 지표 선택 (Global Index gridStock)
    "selected_macro":       None,          # indicator_code str; 설정 시 selected_ticker는 무시
}


def init() -> None:
    """앱 진입 시 1회 호출. 누락된 키만 기본값으로 채운다."""
    for key, default in _DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default


def get(key: str):
    return st.session_state.get(key, _DEFAULTS.get(key))


def set(key: str, value) -> None:
    st.session_state[key] = value


def navigate_to_stock(ticker: str) -> None:
    """워치리스트 등에서 종목 클릭 시 종목분석 탭으로 점프."""
    st.session_state["selected_ticker"] = ticker
    st.session_state["active_tab"] = "종목 분석"

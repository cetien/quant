"""
ui/components/stock_selector.py

모든 관리 화면에서 재사용하는 종목 검색 컴포넌트.

사용법
------
from ui.components.stock_selector import stock_selector

ticker = stock_selector(db, key="theme_mgmt_add")
if ticker:
    # ticker 확정된 상태
    do_something(ticker)
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
from storage.db_manager import DuckDBManager


# ── 종목 목록 캐시 (세션당 1회 로드) ─────────────────────────────────────────

def _load_stock_options(db: DuckDBManager) -> list[dict]:
    """
    stocks 테이블에서 활성 종목을 로드.
    반환: [{"label": "삼성전자 (005930)", "ticker": "005930", "name": "삼성전자"}, ...]
    세션 내 캐시 — 관리 탭에서 종목 추가/삭제 시 clear_stock_cache() 호출.
    """
    if "stock_selector_options" in st.session_state:
        return st.session_state["stock_selector_options"]

    try:
        df = db.query("""
            SELECT ticker, name
            FROM stocks
            WHERE is_active = TRUE
            ORDER BY name
        """)
    except Exception:
        df = pd.DataFrame(columns=["ticker", "name"])

    options = [
        {"label": f"{row['name']} ({row['ticker']})",
         "ticker": row["ticker"],
         "name":   row["name"]}
        for _, row in df.iterrows()
    ]
    st.session_state["stock_selector_options"] = options
    return options


def clear_stock_cache() -> None:
    """종목 마스터 변경 후 호출 — 다음 렌더링 시 재로드."""
    st.session_state.pop("stock_selector_options", None)


# ── 공개 API ──────────────────────────────────────────────────────────────────

def stock_selector(
    db: DuckDBManager,
    key: str,
    label: str = "종목 검색",
    placeholder: str = "종목명 또는 티커 입력…",
    help: str = "종목명 또는 티커 앞 글자로 검색합니다.",
) -> str | None:
    """
    종목 검색 selectbox.

    - 입력 문자열로 name / ticker 동시 필터링 (대소문자 무시)
    - 선택 시 ticker 반환, 미선택(placeholder) 시 None 반환

    Parameters
    ----------
    db          : DuckDBManager
    key         : str  위젯 고유 key (화면마다 다르게 지정)
    label       : str  위젯 레이블
    placeholder : str  검색창 안내 문구
    help        : str  툴팁

    Returns
    -------
    str | None  선택된 ticker 또는 None
    """
    options = _load_stock_options(db)

    def _search(q: str, opts: list[dict]) -> list[dict]:
        if not q:
            return opts
        q = q.lower()
        return [o for o in opts if q in o["name"].lower() or q in o["ticker"].lower()]

    # 검색어 입력
    query = st.text_input(
        label,
        key=f"{key}_query",
        placeholder=placeholder,
        help=help,
        label_visibility="collapsed" if label == "" else "visible",
    )

    filtered = _search(query, options)

    if not filtered:
        st.caption("검색 결과 없음")
        return None

    # 결과가 1개면 자동 확정
    if len(filtered) == 1:
        st.caption(f"✅ {filtered[0]['label']} 자동 선택")
        return filtered[0]["ticker"]

    labels = [o["label"] for o in filtered]
    chosen = st.selectbox(
        "종목 선택",
        options=["— 선택 —"] + labels,
        key=f"{key}_select",
        label_visibility="collapsed",
    )

    if chosen == "— 선택 —":
        return None

    matched = next((o for o in filtered if o["label"] == chosen), None)
    return matched["ticker"] if matched else None

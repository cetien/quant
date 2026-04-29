"""
ui/sidebar.py  (v3 — AgGrid)
사이드 패널:
  ┌─────────────────────────────┐
  │  global index 요약          │
  ├─────────────────────────────┤
  │  slider for gridGroup       │
  │  gridGroup  (AgGrid)        │
  │  [✏ 편집] 버튼              │
  ├─────────────────────────────┤
  │  metric selectbox           │
  │  gridStock  (AgGrid)        │
  └─────────────────────────────┘
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

import ui.state as state
from ingestion.price_collector import PriceCollector
from storage.db_manager import DuckDBManager

_DB_CACHE_SEC = 60

STOCK_METRIC_OPTIONS: list[str] = [
    "1개월 상승률",
    "3개월 상승률",
    "6개월 상승률",
    "PER",
    "PBR",
    "ROE",
]

_METRIC_COL_MAP: dict[str, str] = {
    "1개월 상승률": "ret_1m",
    "3개월 상승률": "ret_3m",
    "6개월 상승률": "ret_6m",
    "PER": "per",
    "PBR": "pbr",
    "ROE": "roe",
}

_METRIC_IS_RATE = {"ret_1m", "ret_3m", "ret_6m"}
_AGGRID_THEME = "streamlit"

_ROW_STYLE_JS = JsCode("""
function(params) {
    if (params.node.isSelected()) {
        return { background: '#1f4e8c', color: '#ffffff', fontWeight: 'bold' };
    }
    return {};
}
""")

_RATE_CELL_STYLE_JS = JsCode("""
function(params) {
    if (params.value == null || params.value === '') return {};
    var v = parseFloat(params.value);
    if (v > 0) return { color: '#22c55e', fontWeight: 'bold' };
    if (v < 0) return { color: '#ef4444', fontWeight: 'bold' };
    return {};
}
""")


def _refresh_db_status(db: DuckDBManager) -> dict:
    ts = state.get("db_status_ts")
    if ts and (datetime.now() - ts).seconds < _DB_CACHE_SEC:
        cached = state.get("db_status")
        if cached:
            return cached
    try:
        status = {
            "n_stocks": db.row_count("stocks"),
            "n_prices": db.row_count("daily_prices"),
            "last_price": db.get_last_date("daily_prices"),
            "last_macro": db.get_last_date("macro_indicators"),
            "ok": True,
        }
        status["ok"] = status["n_stocks"] > 0 and status["last_price"] is not None
    except Exception:
        status = {
            "ok": False,
            "n_stocks": 0,
            "n_prices": 0,
            "last_price": None,
            "last_macro": None,
        }
    state.set("db_status", status)
    state.set("db_status_ts", datetime.now())
    return status


def _render_global_index(db: DuckDBManager) -> None:
    codes = ["USD_KRW", "SOX", "WTI", "KOSPI", "KOSDAQ"]
    try:
        df = db.query(
            f"""
            SELECT indicator_code AS code, value, change_rate
            FROM macro_indicators
            WHERE indicator_code IN ({', '.join(f"'{code}'" for code in codes)})
              AND date = (
                  SELECT MAX(date) FROM macro_indicators
                  WHERE indicator_code = macro_indicators.indicator_code
              )
        """
        )
    except Exception:
        df = pd.DataFrame()

    st.markdown("**🌐 Global Index**")
    if df.empty:
        st.caption("매크로 데이터 없음")
        return

    for _, row in df.iterrows():
        change_rate = row.get("change_rate")
        arrow = "▲" if (change_rate or 0) > 0 else ("▼" if (change_rate or 0) < 0 else "─")
        color = "green" if (change_rate or 0) > 0 else ("red" if (change_rate or 0) < 0 else "gray")
        change_rate_text = f"{change_rate:+.2f}%" if change_rate is not None else ""
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;font-size:0.8rem'>"
            f"<span>{row['code']}</span>"
            f"<span><b>{row['value']:,.2f}</b>&nbsp;"
            f"<span style='color:{color}'>{arrow}&nbsp;{change_rate_text}</span></span>"
            f"</div>",
            unsafe_allow_html=True,
        )


def _load_groups(db: DuckDBManager, min_rating: int) -> pd.DataFrame:
    rows: list[dict] = [
        {
            "group_id": "global",
            "type": "global",
            "그룹명": "🌐 Global Index",
            "종목수": "-",
            "Rating": 10,
        },
    ]
    try:
        df_sector = db.query(
            f"""
            SELECT
                'sec_' || s.id AS group_id,
                'sector' AS type,
                s.name AS 그룹명,
                COUNT(ssm.ticker)::TEXT AS 종목수,
                s.rating AS Rating
            FROM sectors s
            LEFT JOIN stock_sector_map ssm ON ssm.sector_id = s.id
            WHERE s.rating >= {min_rating}
            GROUP BY s.id, s.name, s.rating
            ORDER BY s.rating DESC, s.name
        """
        )
        rows.extend(df_sector.to_dict("records"))
    except Exception:
        pass
    try:
        df_theme = db.query(
            f"""
            SELECT
                'thm_' || t.theme_id AS group_id,
                'theme' AS type,
                t.name AS 그룹명,
                COUNT(CASE WHEN stm.valid_to IS NULL THEN 1 END)::TEXT AS 종목수,
                t.rating AS Rating
            FROM themes t
            LEFT JOIN stock_theme_map stm ON stm.theme_id = t.theme_id
            WHERE t.is_active = TRUE AND t.rating >= {min_rating}
            GROUP BY t.theme_id, t.name, t.rating
            ORDER BY t.rating DESC, t.name
        """
        )
        rows.extend(df_theme.to_dict("records"))
    except Exception:
        pass
    return pd.DataFrame(rows)


def _render_group_grid(db: DuckDBManager, min_rating: int) -> Optional[dict]:
    df = _load_groups(db, min_rating)
    if df.empty:
        st.caption("그룹 없음")
        return None

    cur_gid = state.get("selected_group_id") or df.iloc[0]["group_id"]
    if cur_gid not in df["group_id"].values:
        cur_gid = df.iloc[0]["group_id"]

    disp = df[["그룹명", "종목수", "Rating"]].copy()

    gb = GridOptionsBuilder.from_dataframe(disp)
    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=False,
        suppressMovable=True,
        cellStyle={"fontSize": "12px"},
    )
    gb.configure_column("그룹명", width=170, minWidth=100)
    gb.configure_column("종목수", width=55, minWidth=40, headerName="수")
    gb.configure_column("Rating", width=60, minWidth=40, headerName="★")
    gb.configure_selection("single", use_checkbox=False)
    gb.configure_grid_options(
        rowStyle=_ROW_STYLE_JS,
        headerHeight=28,
        rowHeight=28,
        suppressHorizontalScroll=True,
    )
    opts = gb.build()

    pre_idx = df.index[df["group_id"] == cur_gid].tolist()
    pre_rows = disp.iloc[pre_idx].to_dict("records") if pre_idx else []

    resp = AgGrid(
        disp,
        gridOptions=opts,
        theme=_AGGRID_THEME,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        pre_selected_rows=pre_rows,
        fit_columns_on_grid_load=False,
        height=min(28 * len(disp) + 36, 300),
        key="aggrid_group",
    )

    sel = resp.selected_rows
    if sel is not None:
        sel_df = pd.DataFrame(sel) if not isinstance(sel, pd.DataFrame) else sel
        if not sel_df.empty and "그룹명" in sel_df.columns:
            sel_name = sel_df.iloc[0]["그룹명"]
            matched = df[df["그룹명"] == sel_name]
            if not matched.empty:
                new_gid = matched.iloc[0]["group_id"]
                if new_gid != cur_gid:
                    state.set("selected_group_id", new_gid)
                    st.rerun()
                cur_gid = new_gid

    return df[df["group_id"] == cur_gid].iloc[0].to_dict()


def _load_macro_indicators(db: DuckDBManager) -> pd.DataFrame:
    try:
        return db.query(
            """
            SELECT
                indicator_code AS 코드,
                value AS 현재값,
                change_rate AS 등락률
            FROM macro_indicators
            WHERE date = (SELECT MAX(date) FROM macro_indicators)
            ORDER BY indicator_code
        """
        )
    except Exception:
        return pd.DataFrame(columns=["코드", "현재값", "등락률"])


def _render_macro_grid(db: DuckDBManager) -> None:
    df = _load_macro_indicators(db)
    if df.empty:
        st.caption("매크로 데이터 없음")
        return

    cur_macro = state.get("selected_macro")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=False,
        suppressMovable=True,
        cellStyle={"fontSize": "12px"},
    )
    gb.configure_column("코드", width=120, minWidth=80)
    gb.configure_column(
        "현재값",
        width=95,
        minWidth=70,
        valueFormatter=JsCode(
            "function(p){ return p.value==null ? '-'"
            " : p.value.toLocaleString(undefined,{maximumFractionDigits:2}); }"
        ),
    )
    gb.configure_column(
        "등락률",
        width=80,
        minWidth=60,
        cellStyle=_RATE_CELL_STYLE_JS,
        valueFormatter=JsCode(
            "function(p){ return p.value==null ? '-'"
            " : (p.value>0?'+':'')+p.value.toFixed(2)+'%'; }"
        ),
    )
    gb.configure_selection("single", use_checkbox=False)
    gb.configure_grid_options(
        rowStyle=_ROW_STYLE_JS,
        headerHeight=28,
        rowHeight=26,
        suppressHorizontalScroll=True,
    )
    opts = gb.build()

    pre_idx = df.index[df["코드"] == cur_macro].tolist()
    pre_rows = df.iloc[pre_idx].to_dict("records") if pre_idx else []

    resp = AgGrid(
        df,
        gridOptions=opts,
        theme=_AGGRID_THEME,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        pre_selected_rows=pre_rows,
        fit_columns_on_grid_load=False,
        height=min(26 * len(df) + 34, 400),
        key="aggrid_macro",
    )

    sel = resp.selected_rows
    if sel is not None:
        sel_df = pd.DataFrame(sel) if not isinstance(sel, pd.DataFrame) else sel
        if not sel_df.empty and "코드" in sel_df.columns:
            new_macro = sel_df.iloc[0]["코드"]
            if new_macro and new_macro != cur_macro:
                state.set("selected_macro", new_macro)
                state.set("selected_ticker", None)
                state.set("active_tab", "종목 분석")
                st.rerun()


def _load_stocks_for_group(
    db: DuckDBManager, group: dict, metric_col: str, min_stock_rating: int
) -> pd.DataFrame:
    group_type = group.get("type", "global")
    is_rate = metric_col in _METRIC_IS_RATE
    metric_expr = (
        f"CASE WHEN sc.{metric_col} IS NOT NULL THEN ROUND(sc.{metric_col} * 100, 2) END"
        if is_rate
        else f"ROUND(sc.{metric_col}, 2)"
    )
    order_clause = (
        f"CASE WHEN sc.{metric_col} IS NULL THEN 1 ELSE 0 END,"
        f" sc.{metric_col} DESC NULLS LAST"
    )
    rating_filter = f"AND s.rating >= {min_stock_rating}"

    if group_type == "global":
        weight_col = "1.0 AS 비중"
    elif group_type == "sector":
        weight_col = "ssm.weight AS 비중"
    else:
        weight_col = "stm.weight AS 비중"

    base_select = f"""
        s.ticker AS 티커,
        s.name AS 종목명,
        {metric_expr} AS 지표,
        s.rating AS rating,
        {weight_col},
        COALESCE(sec.sector, '') AS 섹터
    """

    if group_type == "global":
        sql = f"""
            SELECT {base_select}
            FROM stocks s
            LEFT JOIN stock_cache sc ON sc.ticker = s.ticker
            LEFT JOIN v_stock_primary_sector sec ON sec.ticker = s.ticker
            WHERE s.is_active = TRUE {rating_filter}
            ORDER BY {order_clause}
        """
    elif group_type == "sector":
        sector_id = group["group_id"].replace("sec_", "")
        sql = f"""
            SELECT {base_select}
            FROM stock_sector_map ssm
            JOIN stocks s ON s.ticker = ssm.ticker
            LEFT JOIN stock_cache sc ON sc.ticker = s.ticker
            LEFT JOIN v_stock_primary_sector sec ON sec.ticker = s.ticker
            WHERE ssm.sector_id = {sector_id} AND s.is_active = TRUE {rating_filter}
            ORDER BY {order_clause}
        """
    else:
        theme_id = group["group_id"].replace("thm_", "")
        sql = f"""
            SELECT {base_select}
            FROM stock_theme_map stm
            JOIN stocks s ON s.ticker = stm.ticker
            LEFT JOIN stock_cache sc ON sc.ticker = s.ticker
            LEFT JOIN v_stock_primary_sector sec ON sec.ticker = s.ticker
            WHERE stm.theme_id = {theme_id}
              AND (stm.valid_to IS NULL OR stm.valid_to > CURRENT_DATE)
              AND s.is_active = TRUE {rating_filter}
            ORDER BY {order_clause}
        """
    try:
        return db.query(sql)
    except Exception:
        return pd.DataFrame(columns=["티커", "종목명", "지표", "섹터"])


def _render_group_actions(db: DuckDBManager, group: dict, stocks_df: pd.DataFrame) -> None:
    if group.get("type") not in ("sector", "theme"):
        return

    name = group["그룹명"]
    count = len(stocks_df)
    col_label, col_update, col_edit = st.columns([5, 2, 1.5])
    with col_label:
        st.caption(f"선택 그룹: {name}: {count}종목")
    with col_update:
        if st.button("⬆ update all", key="grp_update_all_btn", width="stretch"):
            tickers = stocks_df["티커"].tolist() if not stocks_df.empty else []
            if not tickers:
                st.warning("업데이트할 종목이 없습니다.")
            else:
                with st.spinner(f"{len(tickers)}개 종목 증분 업데이트 중..."):
                    updated = PriceCollector(db).incremental_update_daily_prices(tickers=tickers)
                st.success(f"완료: {updated}건 적재")
                st.rerun()
    with col_edit:
        if st.button("✏ 편집", key="grp_edit_btn", width="stretch"):
            state.set("edit_group_id", group["group_id"])
            state.set("edit_group_type", group["type"])
            state.set("admin_focus", "theme" if group["type"] == "theme" else "sector")
            st.rerun()


def _render_stock_grid(
    db: DuckDBManager, group: dict, metric_label: str, min_stock_rating: int
) -> None:
    metric_col = _METRIC_COL_MAP[metric_label]
    is_rate = metric_col in _METRIC_IS_RATE
    df = _load_stocks_for_group(db, group, metric_col, min_stock_rating)

    if group.get("type") in ("sector", "theme"):
        _render_group_actions(db, group, df)

    if df.empty:
        st.caption("종목 없음")
        return

    disp = df[["종목명", "지표", "비중", "rating", "티커"]].copy()

    gb = GridOptionsBuilder.from_dataframe(disp)
    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=False,
        suppressMovable=True,
        cellStyle={"fontSize": "12px"},
    )
    gb.configure_column("종목명", width=100, minWidth=70)

    metric_header = metric_label.replace("상승률", "%")
    if is_rate:
        gb.configure_column(
            "지표",
            headerName=metric_header,
            width=75,
            minWidth=60,
            cellStyle=_RATE_CELL_STYLE_JS,
            valueFormatter=JsCode(
                "function(p){ return p.value==null ? '-' : p.value.toFixed(1)+'%'; }"
            ),
        )
    else:
        gb.configure_column(
            "지표",
            headerName=metric_header,
            width=75,
            minWidth=60,
            valueFormatter=JsCode(
                "function(p){ return p.value==null ? '-' : p.value; }"
            ),
        )
    gb.configure_column("비중", headerName="W", width=50, minWidth=40)
    gb.configure_column("rating", headerName="★", width=45, minWidth=30)
    gb.configure_column("티커", width=65, minWidth=50)
    gb.configure_selection("single", use_checkbox=False)
    gb.configure_grid_options(
        rowStyle=_ROW_STYLE_JS,
        headerHeight=28,
        rowHeight=26,
        suppressHorizontalScroll=True,
    )
    opts = gb.build()

    cur_ticker = state.get("selected_ticker")
    pre_idx = df.index[df["티커"] == cur_ticker].tolist()
    pre_rows = disp.iloc[pre_idx].to_dict("records") if pre_idx else []

    resp = AgGrid(
        disp,
        gridOptions=opts,
        theme=_AGGRID_THEME,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        pre_selected_rows=pre_rows,
        fit_columns_on_grid_load=False,
        height=min(26 * len(disp) + 34, 400),
        key="aggrid_stock",
    )

    sel = resp.selected_rows
    if sel is not None:
        sel_df = pd.DataFrame(sel) if not isinstance(sel, pd.DataFrame) else sel
        if not sel_df.empty and "티커" in sel_df.columns:
            new_ticker = sel_df.iloc[0]["티커"]
            if new_ticker and new_ticker != cur_ticker:
                state.set("selected_ticker", new_ticker)
                state.set("active_tab", "종목 분석")
                state.set("trigger_price_update", new_ticker)
                st.rerun()


def render(active_tab: str, db: DuckDBManager) -> None:
    _render_global_index(db)
    st.divider()

    min_rating = st.slider(
        "그룹 최소 Rating",
        min_value=0,
        max_value=10,
        value=1,
        step=1,
        key="grp_rating_slider",
        help="이 값 이상의 rating을 가진 섹터/테마만 표시. Global Index는 항상 표시.",
    )
    selected_group = _render_group_grid(db, min_rating)
    st.divider()

    if selected_group and selected_group.get("type") == "global":
        st.caption("📊 매크로 지표")
        _render_macro_grid(db)
    else:
        metric_label = st.selectbox(
            "종목 지표",
            options=STOCK_METRIC_OPTIONS,
            index=0,
            key="stock_metric_select",
        )
        min_stock_rating = st.slider(
            "종목 최소 Rating",
            min_value=0,
            max_value=10,
            value=1,
            step=1,
            key="stk_rating_slider",
            help="이 값 이상의 rating을 가진 종목만 표시.",
        )
        if selected_group:
            _render_stock_grid(db, selected_group, metric_label, min_stock_rating)
        else:
            st.caption("그룹을 선택하세요")

    st.divider()
    status = _refresh_db_status(db)
    if status["ok"]:
        st.caption(
            f"✅ DB 정상  \n"
            f"종목 {status['n_stocks']:,}개 | 최신 {status['last_price'] or '-'}"
        )
    else:
        st.caption("⚠️ DB 미수집 — [관리] 탭에서 초기화 필요")

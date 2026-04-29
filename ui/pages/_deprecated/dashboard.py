"""
ui/pages/dashboard.py
매크로 지표 요약 대시보드
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from storage.db_manager import DuckDBManager


def render():
    st.title("📊 글로벌 매크로 대시보드")

    db = DuckDBManager()
    try:
        # ── 데이터 수집 안내 배너 ─────────────────────────────────────────────────
        n_macro = db.row_count("macro_indicators")
        n_stocks = db.row_count("stocks")
        n_prices = db.row_count("daily_prices")

        col_s1, col_s2, col_s3 = st.columns(3)
        col_s1.metric("매크로 지표 (행)", f"{n_macro:,}")
        col_s2.metric("종목 수", f"{n_stocks:,}")
        col_s3.metric("일봉 데이터 (행)", f"{n_prices:,}")

        if n_macro == 0:
            st.warning(
                "📭 매크로 데이터가 없습니다.\n\n"
                "**[⚙️ 데이터 관리]** 메뉴 → **'매크로 증분 업데이트'** 버튼을 먼저 실행하세요."
            )
            return

        st.markdown("---")

        # ── 최신 매크로 지표 카드 ─────────────────────────────────────────────────
        macro_df = db.query("""
            SELECT indicator_code, date, value, change_rate
            FROM macro_indicators
            WHERE date = (SELECT MAX(date) FROM macro_indicators)
            ORDER BY indicator_code
        """)

        if macro_df.empty:
            st.info("표시할 최신 매크로 지표가 아직 없습니다. 데이터 수집 후 다시 확인하세요.")
            return

        latest_date = macro_df["date"].iloc[0]
        st.subheader(f"최신 지표 ({latest_date})")

        cols = st.columns(min(len(macro_df), 4))
        for i, row in macro_df.iterrows():
            with cols[i % 4]:
                delta_str = f"{row['change_rate']:.2f}%" if pd.notna(row["change_rate"]) else "N/A"
                st.metric(
                    label=row["indicator_code"],
                    value=f"{row['value']:,.2f}",
                    delta=delta_str,
                )

        st.markdown("---")

        # ── 추세선 차트 ──────────────────────────────────────────────────────────
        st.subheader("추세선")

        indicators = macro_df["indicator_code"].dropna().tolist()
        if not indicators:
            st.info("선택 가능한 매크로 지표가 없습니다.")
            return

        selected = st.multiselect("지표 선택", indicators, default=indicators[:3])
        period = st.selectbox("기간", ["1M", "3M", "6M", "1Y", "3Y"], index=3)
        period_days = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095}[period]

        if not selected:
            st.info("추세선을 보려면 하나 이상의 지표를 선택하세요.")
            return

        in_clause = ", ".join([f"'{i}'" for i in selected])
        hist = db.query(f"""
            SELECT indicator_code, date, value
            FROM macro_indicators
            WHERE indicator_code IN ({in_clause})
              AND date >= CURRENT_DATE - INTERVAL {period_days} DAY
            ORDER BY date
        """)

        if hist.empty:
            st.info(f"선택한 기간({period})에 표시할 추세 데이터가 없습니다.")
            return

        fig = make_subplots(
            rows=len(selected), cols=1,
            shared_xaxes=True,
            subplot_titles=selected,
            vertical_spacing=0.06,
        )
        for idx, code in enumerate(selected, 1):
            sub = hist[hist["indicator_code"] == code]
            if sub.empty:
                continue
            fig.add_trace(
                go.Scatter(x=sub["date"], y=sub["value"], name=code, mode="lines"),
                row=idx, col=1,
            )
        fig.update_layout(
            height=200 * len(selected),
            showlegend=False,
            margin=dict(l=0, r=0, t=30, b=0),
        )
        st.plotly_chart(fig, width="stretch")
    finally:
        db.close()

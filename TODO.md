# quant 프로젝트 TODO

---

## ✅ 1. 가상환경 + 패키지

```bash
cd D:\Trabajo\ai\quant
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

---

## ✅ 2. UI 실행

```bash
streamlit run ui/app.py
```

> \[관리 &gt; 데이터 수집\] 탭에서: 스키마 초기화 → 매크로 수집 → 종목 마스터 → 삼성전자(005930) 테스트 수집 순서로 진행.

---

## ✅ 3. DB 구조 변경 (v2)

### 변경 요약

- `stocks.sector TEXT` 제거 → `sectors` + `stock_sector_map` (N:M) 분리
- `themes` + `stock_theme_map` (N:M, 이력 관리) 추가
- 뷰 2개 추가: `v_stock_primary_sector`, `v_active_theme_map`
- `db_manager.py`: `get_sector_map()`, `upsert_theme()`, `deactivate_theme_mapping()` 추가
- `tools/import_sectors.py` 신규 생성 (KRX CSV 일괄 입력)

### 적용 절차

```bash
del data\database\quant.duckdb
python tools/import_sectors.py --csv <KRX_CSV_경로> --dry-run
python tools/import_sectors.py --csv <KRX_CSV_경로>
streamlit run ui/app.py
```

### 잔여 작업

- \[ \] `scorer.py` / `selector.py`: `sector_map` 인자 공급처를 `db.get_sector_map()` 호출로 교체
- \[ \] 테마: \[관리 &gt; 테마 관리\] 탭에서 수동 추가

---

## ✅ 4. UI 구조 개편 (Phase A)

### 변경 요약

- 사이드바 radio → 상단 `st.tabs` 기반 네비게이션으로 전환
- `ui/state.py` 신규: session_state 키 중앙 관리
- `ui/sidebar.py` 신규: 탭별 맥락 필터 + 워치리스트 + DB 상태 위젯
- `ui/app.py` 전면 재작성: 탑 탭 라우터

### 신규 파일 구조

```
ui/
├── app.py
├── state.py
├── sidebar.py
└── pages/
    ├── market/
    │   ├── dashboard.py
    │   └── theme_ranking.py
    ├── screener/
    │   ├── condition.py
    │   ├── by_theme.py
    │   └── by_sector.py
    ├── stock/
    │   ├── chart.py
    │   ├── factor_score.py
    │   ├── supply.py
    │   └── financial.py
    ├── backtest/
    │   └── main.py
    ├── admin/
    │   ├── data_mgmt.py
    │   ├── theme_mgmt.py
    │   ├── sector_mgmt.py
    │   ├── explorer.py
    │   └── settings.py
    └── _deprecated/
```

---

## ✅ 5. DB 구조 변경 (v3)

### 변경 요약

파일변경 내용`storage/schema.pyCREATE_STOCK_CACHE` DDL 추가, `ALL_SCHEMAS` 순서 삽입`storage/db_manager.pyrefresh_stock_cache()` 추가 — ret_1m/3m/6m + PER/PBR/ROE 1쿼리 계산`ingestion/price_collector.pyincremental_update_daily_prices` 완료 후 `refresh_stock_cache(tickers)` 자동 호출

### stock_cache 테이블 구조

```sql
CREATE TABLE IF NOT EXISTS stock_cache (
    ticker      TEXT    NOT NULL REFERENCES stocks(ticker),
    last_date   DATE    NOT NULL,
    ret_1m      DOUBLE,   -- 30일 수익률 (adj_close 기준)
    ret_3m      DOUBLE,   -- 90일 수익률
    ret_6m      DOUBLE,   -- 180일 수익률
    per         DOUBLE,
    pbr         DOUBLE,
    roe         DOUBLE,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker)
);
```

### 상승률 계산 기준

- 기준: `adj_close` (배당·분할 반영)
- 기간: 캘린더일 기준 (1M=30일, 3M=90일, 6M=180일)

---

## ✅ 6. UI 변경 — Side Panel (v2, AgGrid)

### 변경 요약

파일변경 내용`ui/sidebar.py`전면 재작성. 3섹션 구조 + AgGrid 적용`ui/state.pyselected_group_id`, `edit_group_id`, `edit_group_type`, `trigger_price_update`, `selected_macro` 키 추가`ui/pages/stock/chart.py_render_macro_chart()` 추가, `trigger_price_update` 감지 블록 추가

### Side Panel 구조

```
┌─────────────────────────────┐
│  🌐 Global Index 요약        │  macro_indicators 최신값 + 등락률
├─────────────────────────────┤
│  slider: 그룹 최소 Rating    │  range 0~10, default 1
│  gridGroup (AgGrid)          │  Global Index / sectors / themes
│  [✏ 편집] 버튼               │  sector·theme 선택 시만 표시
├─────────────────────────────┤
│  [Global Index 선택 시]      │
│    매크로 지표 AgGrid         │  코드 / 현재값 / 등락률
│                              │  → 클릭 시 chart.py 매크로 라인차트
│  [섹터·테마 선택 시]          │
│    selectbox: 종목 지표       │  1개월 상승률 / 3개월 / 6개월 / PER / PBR / ROE
│    slider: 종목 최소 Rating   │  range 0~10, default 0
│    gridStock (AgGrid)         │  종목명 / 지표 / 티커
│                              │  → 클릭 시 chart.py + price update 트리거
└─────────────────────────────┘
```

### AgGrid 공통 설정

- theme: `streamlit`
- 선택행 하이라이트: `rowStyle` JsCode (배경 `#1f4e8c`)
- 상승률 셀: 양수 초록(`#22c55e`) / 음수 빨강(`#ef4444`)
- `pre_selected_rows`로 rerun 후에도 선택 유지
- `domLayout: autoHeight`, 컬럼 폭 합계 ≤ 280px

---

## ✅ 7. stocks 테이블 — rating 필드 추가

### 변경 요약

파일변경 내용`storage/schema.pyCREATE_STOCKS` DDL에 `rating INTEGER NOT NULL DEFAULT 0` 추가`storage/db_manager.pymigrate()` 메서드 추가 + `_ensure_schema_initialized()`에서 자동 호출

### 마이그레이션 동작

- 기존 DB: `information_schema` 확인 후 컬럼 없으면 `ALTER TABLE stocks ADD COLUMN rating INTEGER NOT NULL DEFAULT 0` 실행
- 신규 DB: DDL에 포함 → ALTER 건너뜀
- 향후 컬럼 추가: `migrate()`의 `migrations` 리스트에 튜플 1개 추가

---

## ✅ 8. [chart.py](http://chart.py) — 액션 바 + gridStock rating 슬라이더

### 변경 요약

파일변경 내용`ui/pages/stock/chart.py_render_action_bar()` 추가 — 차트 상단에 종목명·rating·버튼 표시`ui/sidebar.py`gridStock 위 `종목 최소 Rating` 슬라이더 추가, query에 `s.rating >= N` 조건 반영

### 액션 바 구성

```
[종목명]  [티커]  ⭐ {rating}
[👍] [👎] [🔗]
```

버튼동작👍`UPDATE stocks SET rating = rating + 1 WHERE ticker = ?` → `st.rerun()`👎`UPDATE stocks SET rating = MAX(0, rating - 1) WHERE ticker = ?` → `st.rerun()`🔗`st.link_button` → `https://finance.naver.com/item/main.naver?code={ticker}` 새 탭

---

## 잔여 작업 (Phase B / C)

- \[ \] `scorer.py` / `selector.py`: `db.get_sector_map()` 호출로 교체
- \[ \] `stock/factor_score.py`: [scorer.py](http://scorer.py) 연동 후 팩터 점수 테이블 표시
- \[ \] `market/theme_ranking.py`: 테마별 평균 수익률 자동 계산 표시
- \[ \] 워치리스트 DB 영속화 (현재 session 한정)
- \[ \] 스크리너 결과에서 종목 클릭 → 종목 분석 탭 자동 이동
- \[ \] `admin/settings.py`: 설정 항목 구체화 (Phase C)
- \[ \] `✏ 편집` 버튼 → `admin/sector_mgmt.py` / `theme_mgmt.py` 탭 이동 로직 (`app.py` 처리 필요)

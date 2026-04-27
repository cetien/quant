# Quant Platform — DuckDB 기반 고성능 종목 분석 플랫폼

## 디렉토리 구조

```
quant/
├── main.py                    # 파이프라인 오케스트레이터 (CLI)
├── requirements.txt
│
├── ingestion/                 # 수집 레이어
│   ├── price_collector.py     # pykrx 일봉 OHLCV + 거래대금
│   ├── supply_collector.py    # 기관·외인 순매수 (pykrx)
│   └── macro_collector.py     # SOX·환율·금리 (yfinance)
│
├── storage/                   # 저장 레이어
│   ├── schema.py              # DDL + CHECK 제약 (뷰 포함)
│   ├── db_manager.py          # DuckDB 연결·upsert(append-only)·섹터/테마 메서드
│   └── parquet_store.py       # Cold 데이터 Parquet 파티셔닝
│
├── analysis/                  # 분석 레이어
│   ├── factors/
│   │   ├── momentum.py        # RS, N일 수익률, 신고가 근접도
│   │   ├── value.py           # PER, PBR (announce_date 기준)
│   │   ├── quality.py         # ROE, 영업이익률, 부채비율
│   │   └── liquidity.py       # 거래대금 평균·급증률
│   ├── scorer.py              # Z-score + 섹터 중립화 + 가중합
│   ├── selector.py            # 상위 N 종목·대장주·리더보드
│   └── backtest.py            # T+1 lag·MDD·Sharpe·생존편향 처리
│
├── ui/                        # Streamlit UI 레이어
│   ├── app.py                 # 메인 진입점
│   └── pages/
│       ├── dashboard.py       # 매크로 대시보드
│       ├── scanner.py         # 조건식 스크리너
│       ├── deep_dive.py       # 캔들차트 + 상관계수
│       ├── backtest_ui.py     # 백테스트 설정·실행
│       └── settings.py        # 데이터 수집 관리 + 테마 관리 UI
│
├── tools/                     # 1회용 유틸 스크립트
│   └── import_sectors.py      # KRX CSV → sectors + stock_sector_map 일괄 입력
│
├── common/                    # 공통 유틸
│   ├── config.py              # dataclass 기반 설정
│   ├── logger.py              # 로거
│   └── calendar.py            # 거래일·Forward Fill·Lag
│
└── data/
    ├── raw/prices/            # Parquet Cold 데이터
    ├── raw/supply/
    ├── raw/macro/
    ├── database/quant.duckdb  # Hot 데이터
    └── exports/               # CSV·Excel 내보내기
```

## DB 스키마 구조

### 테이블 의존성 순서

```
sectors
  └─► stock_sector_map ◄─┐
stocks ──────────────────┘
  └─► daily_prices
  └─► supply
  └─► fundamentals
  └─► stock_theme_map ◄── themes

delisted_stocks  (독립)
macro_indicators (독립)
trading_calendar (독립)
```

### 테이블 목록

| 테이블 | 역할 | PK |
|---|---|---|
| `sectors` | 섹터 마스터 (KRX 기준) | id |
| `stocks` | 종목 마스터 | ticker |
| `stock_sector_map` | 종목↔섹터 N:M (weight=검색 우선순위) | (ticker, sector_id) |
| `themes` | 테마 마스터 (UI에서 수동 추가) | theme_id |
| `stock_theme_map` | 종목↔테마 N:M (이력 관리) | (ticker, theme_id, valid_from) |
| `daily_prices` | 일봉 OHLCV + 거래대금 | (ticker, date) |
| `supply` | 기관·외인 순매수 | (ticker, date) |
| `macro_indicators` | 글로벌 매크로 지표 | (indicator_code, date) |
| `fundamentals` | 분기 재무 (announce_date 기준) | (ticker, report_date, fiscal_quarter) |
| `trading_calendar` | 거래일 캘린더 | (market, date) |
| `delisted_stocks` | 상장폐지 종목 | ticker |

### 뷰

| 뷰 | 설명 |
|---|---|
| `v_stock_primary_sector` | 종목별 대표 섹터 (weight 최대 섹터 기준) |
| `v_active_theme_map` | 현재 유효 테마-종목 매핑 (valid_to IS NULL 또는 미래) |

## 시작 순서 (Phase별)

### Phase 0 — 환경 설정
```bash
cd D:\Trabajo\ai\quant
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Phase 0 — 섹터 데이터 입력 (KRX CSV 1회 실행)
```bash
# dry-run으로 컬럼 매핑 먼저 확인
python tools/import_sectors.py --csv <KRX_CSV_경로> --dry-run

# 실입력
python tools/import_sectors.py --csv <KRX_CSV_경로>
```
> KRX CSV 컬럼이 다를 경우 `tools/import_sectors.py` 상단의 `COLUMN_MAP` 수정

### Phase 1 — 데이터 수집 (UI에서 실행 권장)
```bash
streamlit run ui/app.py
```
→ [데이터 관리] 탭 → 스키마 초기화 → 매크로 수집 → 종목 마스터 → 일봉(테스트 티커부터)

### Phase 2 — 팩터 계산
`main.py`의 Phase 2 주석 해제 후 실행

### Phase 3 — UI 분석
```bash
streamlit run ui/app.py
```

## 설계 원칙

| 원칙 | 구현 |
|---|---|
| Look-ahead bias 방지 | `fundamentals` join 시 `announce_date` 기준, 백테스트 T+1 진입 |
| 생존 편향 방지 | `delisted_stocks` 테이블, 상폐 이후 수익률 0 처리 |
| 증분 업데이트 | `upsert_dataframe` → INSERT OR IGNORE (CREATE OR REPLACE 미사용) |
| 데이터 정밀도 | 컬럼 타입 명시 + CHECK 제약 (DuckDB STRICT 모드 없음) |
| 섹터 쏠림 방지 | `scorer.py` 섹터 중립화 Z-score (`db.get_sector_map()` 경유) |
| 부호 일관성 | PER·PBR·부채비율 부호 반전 후 Z-score |
| 섹터 관리 | TEXT 직접 저장 방식 폐기 → `stock_sector_map` FK 기반 N:M |
| 테마 관리 | UI에서 수동 추가, 이력은 `valid_from/valid_to`로 관리 |
| DuckDB 트리거 미지원 대응 | `themes.updated_at`은 `upsert_theme()` 내부에서 갱신 |

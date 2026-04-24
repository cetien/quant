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
│   ├── schema.py              # DDL + CHECK 제약 (STRICT 모드 대체)
│   ├── db_manager.py          # DuckDB 연결·upsert(append-only)
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
│       └── settings.py        # 데이터 수집 관리
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

## 시작 순서 (Phase별)

### Phase 0 — 환경 설정
```bash
cd D:\Trabajo\ai\quant
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Phase 0 — DB 초기화
```bash
python main.py
```
→ `main.py`의 `db.init_schema()` 실행 확인

### Phase 1 — 데이터 수집 (UI에서 실행 권장)
```bash
streamlit run ui/app.py
```
→ [데이터 관리] 탭 → 스키마 초기화 → 매크로 수집 → 종목 마스터 → 일봉(테스트 티커부터)

### Phase 2 — 팩터 계산
`main.py`의 Phase 2 주석 해제 후 실행

### Phase 3 — UI
```bash
streamlit run ui/app.py
```

## 설계 원칙

| 원칙 | 구현 |
|------|------|
| Look-ahead bias 방지 | fundamentals join 시 `announce_date` 기준, 백테스트 T+1 진입 |
| 생존 편향 방지 | `delisted_stocks` 테이블, 상폐 이후 수익률 0 처리 |
| 증분 업데이트 | `upsert_dataframe` → INSERT OR IGNORE (CREATE OR REPLACE 미사용) |
| 데이터 정밀도 | 컬럼 타입 명시 + CHECK 제약 (DuckDB STRICT 모드 없음) |
| 섹터 쏠림 방지 | `scorer.py` 섹터 중립화 Z-score |
| 부호 일관성 | PER·PBR·부채비율 부호 반전 후 Z-score |

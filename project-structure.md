# 디렉토리 전체 구조

```
quant/
│
├── main.py                        # 파이프라인 오케스트레이터
│
├── ingestion/                     # 수집 레이어
│   ├── price_collector.py         # pykrx + yfinance 일봉 OHLCV
│   ├── supply_collector.py        # 기관·외인 순매수 (pykrx)
│   └── macro_collector.py         # SOX·환율·금리 (yfinance)
│
├── storage/                       # 저장 레이어
│   ├── db_manager.py              # DuckDB 연결, CRUD, upsert(append-only), 섹터/테마 메서드
│   ├── schema.py                  # DDL + CHECK 제약 + 뷰 (ALL_SCHEMAS)
│   └── parquet_store.py           # Cold 데이터 Parquet 파티셔닝 저장
│
├── analysis/                      # 처리·분석 레이어
│   ├── factors/
│   │   ├── momentum.py            # RS, 수익률, 신고가 근접도
│   │   ├── value.py               # PER, PBR, PSR
│   │   ├── quality.py             # ROE, 영업이익률, 부채비율
│   │   └── liquidity.py           # 거래대금, 거래량 증가율
│   ├── scorer.py                  # Z-score 정규화 + 섹터 중립화 + 가중합 최종 점수
│   ├── selector.py                # 상위N·대장주·리더보드
│   └── backtest.py                # T+1 실행, MDD, Sharpe, 생존편향 처리
│
├── ui/                            # Streamlit UI 레이어
│   ├── app.py                     # 메인 진입점, 페이지 라우팅
│   ├── pages/
│   │   ├── dashboard.py           # 매크로 지표 요약 대시보드
│   │   ├── scanner.py             # 조건식 편집 + 검색 결과 목록
│   │   ├── deep_dive.py           # 캔들차트 + 상관계수 시각화
│   │   ├── backtest_ui.py         # 전략 검증, 성과 분석
│   │   └── settings.py            # 수집 관리 + 테마 관리 UI
│   └── components/
│       ├── candlestick.py         # Plotly 캔들차트 컴포넌트
│       ├── condition_editor.py    # 조건식 GUI 편집기
│       └── factor_table.py        # 팩터 스코어 테이블
│
├── tools/                         # 1회용 유틸 스크립트
│   └── import_sectors.py          # KRX CSV → sectors + stock_sector_map 일괄 입력
│
├── common/                        # 공통 유틸리티
│   ├── config.py                  # 경로, API 키, 상수 (dataclass 기반)
│   ├── calendar.py                # 거래일 관리, Forward Fill
│   └── logger.py                  # 로그 설정
│
└── data/
    ├── raw/                       # Parquet Cold 데이터
    │   ├── prices/{year}/{month}/
    │   ├── supply/{year}/{month}/
    │   └── macro/{year}/{month}/
    ├── database/
    │   └── quant.duckdb           # Hot 데이터 DuckDB 파일
    └── exports/                   # CSV·Excel 내보내기
```

## 생성 완료 현황

| 레이어 | 파일 | 핵심 내용 |
|---|---|---|
| **common** | `config.py` | dataclass 기반 통합 설정 (IngestionConfig·StorageConfig·AnalysisConfig·BacktestConfig) |
| | `logger.py` | 콘솔+파일 이중 로거 |
| | `calendar.py` | KRX 거래일·Forward Fill·Lag-n일 shift |
| **storage** | `schema.py` | DDL 11개 테이블 + 인덱스 2개 + 뷰 2개 (sectors/themes/매핑 테이블 추가) |
| | `db_manager.py` | append-only upsert, as_of_date 조회, Zero-Copy, get_sector_map(), upsert_theme(), deactivate_theme_mapping() |
| | `parquet_store.py` | 연/월 파티션 저장·DuckDB read_parquet 로드 |
| **ingestion** | `price_collector.py` | pykrx 일봉+거래대금, 증분 업데이트 (sector 컬럼 제거) |
| | `supply_collector.py` | 기관·외인 순매수 (pykrx) |
| | `macro_collector.py` | SOX·환율·금리 7개 지표 (yfinance) |
| **analysis/factors** | `momentum.py` | RS(수정된 수식), N일 수익률, 52주 신고가 근접도 |
| | `value.py` | PER·PBR (announce_date 기준 join) |
| | `quality.py` | ROE·영업이익률·부채비율 |
| | `liquidity.py` | 거래대금 평균·급증률 |
| **analysis** | `scorer.py` | Z-score·섹터 중립화·부호 반전·가중합 |
| | `selector.py` | 상위N·대장주·리더보드 |
| | `backtest.py` | T+1 진입·MDD·Sharpe·생존편향 처리 |
| **ui** | `app.py` | Streamlit 라우팅 |
| | `dashboard.py` | 매크로 카드+추세선 |
| | `scanner.py` | DuckDB 쿼리 기반 스크리너 |
| | `deep_dive.py` | 캔들차트+이동평균+상관계수 |
| | `backtest_ui.py` | 백테스트 UI 뼈대 |
| | `settings.py` | 수집 트리거 + 테마 관리 UI (추가/매핑/목록 탭) |
| **tools** | `import_sectors.py` | KRX CSV → sectors + stock_sector_map 일괄 입력 (1회용) |
| **루트** | `main.py` | Phase별 주석 해제 실행 |
| | `requirements.txt` | 전체 의존성 |
| | `README.md` | 실행 순서+설계 원칙+DB 스키마 |
| | `architecture.md` | 레이어 설계 원칙+DB 인터페이스 |

## DB 스키마 변경 이력

| 버전 | 변경 내용 |
|---|---|
| v1 | `stocks` 단일 테이블에 `sector TEXT` 직접 저장 |
| v2 | `sector TEXT` 제거 → `sectors` + `stock_sector_map` (N:M FK) + `themes` + `stock_theme_map` (이력 관리) 분리. 뷰 2개(`v_stock_primary_sector`, `v_active_theme_map`) 추가 |

> v2 적용 시 기존 DB 삭제 후 재생성 필요 (데이터 이전 불필요, 섹터는 KRX CSV로 재입력)

# Architecture

## 설계 원칙

### 1. 레이어 간 단방향 의존성
UI → analysis → storage ← ingestion 순서로 단방향 참조.
역방향 참조가 없으므로 각 레이어를 독립적으로 테스트할 수 있다.

### 2. factors/ 파일 단위 분리
새 팩터 추가 시 기존 파일을 건드리지 않는다.
`factors/` 안에 새 파일만 추가하고 `scorer.py`에 등록하면 끝이다.
단일 `processor.py`에 모든 팩터를 쌓는 구조는 팩터가 10개를 넘으면 관리가 어려워진다.

### 3. UI components/ 공통화
캔들차트, 팩터 테이블은 `scanner.py`와 `deep_dive.py` 양쪽에서 사용한다.
`components/`에 한 번 구현해두면 수정 시 한 곳만 고친다.

### 4. 섹터/테마 분리 관리
`stocks.sector` TEXT 직접 저장 방식을 폐기하고 별도 테이블로 분리한다.

- **섹터**: `sectors` + `stock_sector_map` (N:M, weight=검색 우선순위)
  - 초기 데이터는 KRX CSV에서 `tools/import_sectors.py`로 1회 일괄 입력
  - 대표 섹터 조회: `v_stock_primary_sector` 뷰 또는 `db.get_sector_map()`
- **테마**: `themes` + `stock_theme_map` (N:M, 이력 관리)
  - UI [데이터 관리] 탭에서 수동 추가
  - `valid_from / valid_to`로 테마 편입·제외 이력 보존
  - PK에 `valid_from` 포함 → 동일 종목의 재활성화 이력 허용

### 5. DuckDB 제약 대응
DuckDB는 UPDATE 트리거를 지원하지 않는다.
`themes.updated_at` 갱신은 `db_manager.upsert_theme()` 내부에서 처리한다.
FK는 DDL 선언만으로 활성화되며 별도 PRAGMA 불필요 (SQLite와 다름).

## UI
Streamlit을 1순위로 사용한다.
분석 코드와 같은 Python 파일 안에서 동작하며, 데이터 테이블·차트·필터 슬라이더가 10~20줄로 구현된다.
Dash는 커스터마이징이 필요할 때 차선으로 고려한다.

## DB 레이어 핵심 인터페이스

| 메서드 | 위치 | 역할 |
|---|---|---|
| `upsert_dataframe(df, table, pk_cols)` | `db_manager.py` | append-only upsert (증분 업데이트) |
| `query_as_of(sql, as_of_date)` | `db_manager.py` | Look-ahead bias 방지 조회 |
| `get_sector_map()` | `db_manager.py` | ticker→섹터명 Series 반환 (scorer/selector 인자용) |
| `upsert_theme(name, description, is_active)` | `db_manager.py` | 테마 UPSERT + updated_at 갱신 |
| `deactivate_theme_mapping(ticker, theme_id)` | `db_manager.py` | 종목-테마 매핑 비활성화 |
| `get_last_date(table, date_col)` | `db_manager.py` | 증분 업데이트 기준점 조회 |

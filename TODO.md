- [x]
# 1. 가상환경 + 패키지
cd D:\Trabajo\ai\quant
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

- [x]
# 2. UI 실행
streamlit run ui/app.py

- [x]
→ [관리 > 데이터 수집] 탭에서: 스키마 초기화 → 매크로 수집 → 종목 마스터 → 삼성전자(005930) 테스트 수집 순서로 진행.

---

- [x]
# 3. DB 구조 변경 (v2)

## 변경 요약
- `stocks.sector TEXT` 제거 → `sectors` + `stock_sector_map` (N:M) 분리
- `themes` + `stock_theme_map` (N:M, 이력 관리) 추가
- 뷰 2개 추가: `v_stock_primary_sector`, `v_active_theme_map`
- `db_manager.py`: `get_sector_map()`, `upsert_theme()`, `deactivate_theme_mapping()` 추가
- `tools/import_sectors.py` 신규 생성 (KRX CSV 일괄 입력)

## 적용 절차
```bash
del data\database\quant.duckdb
python tools/import_sectors.py --csv <KRX_CSV_경로> --dry-run
python tools/import_sectors.py --csv <KRX_CSV_경로>
streamlit run ui/app.py
```

## 잔여 작업
- [ ] `scorer.py` / `selector.py`: `sector_map` 인자 공급처를 `db.get_sector_map()` 호출로 교체
- [ ] 테마: [관리 > 테마 관리] 탭에서 수동 추가

---

- [x]
# 4. UI 구조 개편 (Phase A)

## 변경 요약
- 사이드바 radio → 상단 `st.tabs` 기반 네비게이션으로 전환
- `ui/state.py` 신규: session_state 키 중앙 관리
- `ui/sidebar.py` 신규: 탭별 맥락 필터 + 워치리스트 + DB 상태 위젯
- `ui/app.py` 전면 재작성: 탑 탭 라우터

## 신규 파일 구조
```
ui/
├── app.py              ← 탑 탭 라우터 (전면 재작성)
├── state.py            ← new: session_state 관리
├── sidebar.py          ← new: 공통 사이드바
└── pages/
    ├── market/         ← new
    │   ├── dashboard.py
    │   └── theme_ranking.py
    ├── screener/       ← new (scanner.py → 3개 탭으로 분리)
    │   ├── condition.py
    │   ├── by_theme.py
    │   └── by_sector.py
    ├── stock/          ← new (deep_dive.py → 4개 탭으로 분리)
    │   ├── chart.py
    │   ├── factor_score.py
    │   ├── supply.py
    │   └── financial.py
    ├── backtest/       ← new (backtest_ui.py 이전)
    │   └── main.py
    ├── admin/          ← new (settings.py + explorer.py 재편)
    │   ├── data_mgmt.py
    │   ├── theme_mgmt.py
    │   ├── sector_mgmt.py
    │   ├── explorer.py
    │   └── settings.py
    └── _deprecated/    ← 구 파일 보존 (삭제 전 참조용)
```

## 탭 구성
| 상단 탭 | 내부 탭 | 사이드바 |
|---|---|---|
| 📊 시장 개요 | 매크로 대시보드 / 테마 랭킹 | 기간 선택 |
| 🔍 스크리너 | 조건식 / 테마별 / 섹터별 | 시장·거래대금·RS·Top N 필터 |
| 🔬 종목 분석 | 차트 / 팩터스코어 / 수급 / 재무 | 종목 선택 + 기간 |
| 🧪 백테스트 | (단일) | 전략 파라미터 전체 |
| ⚙️ 관리 | 데이터수집 / 테마 / 섹터 / 탐색 / 설정 | - |

## 잔여 작업 (Phase B / C)
- [ ] `stock/factor_score.py`: scorer.py 연동 후 팩터 점수 테이블 표시
- [ ] `market/theme_ranking.py`: 테마별 평균 수익률 자동 계산 표시
- [ ] 워치리스트 DB 영속화 (현재 session 한정)
- [ ] 스크리너 결과에서 종목 클릭 → 종목 분석 탭 자동 이동
- [ ] `admin/settings.py`: 설정 항목 구체화 (Phase C)

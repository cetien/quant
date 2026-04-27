"""
tools/import_themes.py
CSV(ticker, name, theme_name, desc) 데이터를 읽어 themes, stocks, stock_theme_map 테이블에 적재합니다.
"""
import argparse
import sys
from pathlib import Path
import pandas as pd

# 프로젝트 루트 경로를 sys.path에 추가하여 storage 모듈 참조 허용
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storage.db_manager import DuckDBManager

def import_themes_from_csv(csv_path: Path):
    """CSV 데이터를 로드하여 DuckDB에 테마 및 종목 정보를 업서트합니다."""
    
    # 1. CSV 로드
    df = None
    for enc in ("cp949", "utf-8-sig", "utf-8"):
        try:
            df = pd.read_csv(csv_path, dtype={'ticker': str}, encoding=enc)
            break
        except (UnicodeDecodeError, LookupError):
            continue
        except Exception as e:
            print(f"[ERROR] CSV 파일을 읽는 중 오류 발생: {e}")
            return

    if df is None:
        print(f"[ERROR] CSV 파일을 읽을 수 없습니다. 지원하는 인코딩(CP949, UTF-8)이 아니거나 파일에 문제가 있습니다.")
        return

    # 데이터 기본 정제
    # 실제 샘플인 """069500""" 형태를 처리하기 위해 모든 따옴표 제거 후 6자리 패딩
    df['ticker'] = df['ticker'].astype(str).str.replace('"', '', regex=False).str.strip().str.zfill(6)
    df['name'] = df['name'].str.strip()
    df['theme_name'] = df['theme_name'].str.strip()
    df['desc'] = df['desc'].fillna("").str.strip()
    
    print(f"[INFO] 로드 완료: {len(df)}행")

    with DuckDBManager() as db:
        # 2. 테마(themes) 추가 및 ID 매핑 생성
        theme_to_id = {}
        unique_themes = df[['theme_name', 'desc']].drop_duplicates('theme_name')
        print(f"[INFO] 테마 처리 중... ({len(unique_themes)}개 고유 테마)")
        
        for _, row in unique_themes.iterrows():
            # db_manager의 upsert_theme을 사용하여 이름 기준 중복 체크 및 ID 반환
            # csv.theme_name -> themes.name / csv.desc -> themes.description 반영
            tid = db.upsert_theme(
                name=row['theme_name'], 
                description=row['desc']
            )
            theme_to_id[row['theme_name']] = tid

        # 3. 종목(stocks) 추가
        stocks_df = df[['ticker', 'name']].drop_duplicates('ticker').copy()
        stocks_df['market'] = 'KOSPI'  # 요구사항: 'KOSPI' 고정
        print(f"[INFO] 종목 정보 업데이트 중... ({len(stocks_df)}개 종목)")
        db.upsert_dataframe(stocks_df, "stocks", pk_cols=["ticker"])

        # 4. 종목-테마 매핑(stock_theme_map) 추가
        map_df = df.copy()
        map_df['theme_id'] = map_df['theme_name'].map(theme_to_id)
        # stock_theme_map의 PK는 (ticker, theme_id, valid_from)이므로 날짜 지정 필수
        map_df['valid_from'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        
        # 필수 컬럼 선택 및 중복 제거 (동일 파일 내 중복 방지)
        final_map = map_df[['ticker', 'theme_id', 'valid_from']].drop_duplicates()
        
        print(f"[INFO] 테마 매핑 데이터 업데이트 중... ({len(final_map)}개 매핑)")
        # upsert_dataframe을 통해 기존에 동일 날짜/테마로 등록된 경우 skip 처리
        db.upsert_dataframe(final_map, "stock_theme_map", pk_cols=["ticker", "theme_id", "valid_from"])

    print("[DONE] 테마 데이터 임포트가 완료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV 테마 데이터를 DuckDB로 임포트")
    parser.add_argument("--csv", type=str, default="data/theme_dump.csv", help="입력 CSV 파일 경로")
    args = parser.parse_args()
    
    csv_file = Path(args.csv)
    if not csv_file.exists():
        # 절대 경로가 아닐 경우 프로젝트 루트에서도 검색
        csv_file = ROOT / args.csv
        
    if not csv_file.exists():
        print(f"[ERROR] 파일을 찾을 수 없습니다: {args.csv}")
        sys.exit(1)
        
    import_themes_from_csv(csv_file)

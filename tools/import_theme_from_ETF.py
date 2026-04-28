"""
tools/import_theme_from_ETF.py
ETF 덤프 CSV(theme_id, ticker, weight) 데이터를 읽어 stock_theme_map 테이블에 적재합니다.
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

def import_theme_etf_csv(csv_path: Path):
    """ETF 테마 덤프 CSV 데이터를 로드하여 DuckDB의 stock_theme_map에 적재합니다."""
    
    # 1. CSV 로드
    df = None
    # ETF 덤프 형식: theme_id, ticker, weight
    for enc in ("utf-8-sig", "cp949", "utf-8"):
        try:
            df = pd.read_csv(csv_path, dtype={'ticker': str}, encoding=enc)
            break
        except Exception:
            continue

    if df is None or df.empty:
        print(f"[ERROR] CSV 파일을 읽을 수 없거나 데이터가 비어 있습니다: {csv_path}")
        return

    # 2. 데이터 정제
    # 첫 컬럼(theme_id)이 비어있는 행 스킵 (사용자 요청)
    initial_len = len(df)
    df = df.dropna(subset=['theme_id'])
    df = df[df['theme_id'].astype(str).str.strip() != ""]
    if initial_len > len(df):
        print(f"[INFO] theme_id가 비어있는 {initial_len - len(df)}개 행을 건너뛰었습니다.")

    # ticker: """403870""" 형태에서 따옴표 제거 및 6자리 패딩
    df['ticker'] = df['ticker'].astype(str).str.replace('"', '', regex=False).str.strip().str.zfill(6)
    
    # theme_id 확인 (정수형)
    df['theme_id'] = df['theme_id'].astype(int)
    
    # weight 확인 (실수형)
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(1.0)
    
    # 필수 메타데이터 추가
    # stock_theme_map PK: (ticker, theme_id, valid_from)
    df['valid_from'] = pd.Timestamp.now().strftime('%Y-%m-%d')
    df['source'] = 'manual' # 기본값 세팅

    print(f"[INFO] 로드 및 정제 완료: {len(df)}행")

    with DuckDBManager() as db:
        # 3. 데이터 적재
        # upsert_dataframe은 지정된 pk_cols 기준 중복이 없으면 INSERT 함
        target_cols = ['ticker', 'theme_id', 'weight', 'source', 'valid_from']
        final_df = df[target_cols].drop_duplicates(['ticker', 'theme_id', 'valid_from'])
        
        print(f"[INFO] '{csv_path.name}' 데이터를 stock_theme_map에 적재 중...")
        try:
            total_count = db.upsert_dataframe(final_df, "stock_theme_map", pk_cols=["ticker", "theme_id", "valid_from"])
            
            # 테마별 적재 결과 요약 출력 (사용자 요청 형식)
            summary = final_df.groupby('theme_id').size()
            for tid, count in summary.items():
                print(f"theme_id={tid}, {count} records.")

            print(f"[OK] 적재 완료. 현재 테이블 전체 행 수: {total_count}")
        except Exception as e:
            print(f"[ERROR] DB 적재 중 오류 발생: {e}")

    print("[DONE] 작업이 완료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF 덤프 CSV 데이터를 stock_theme_map으로 임포트")
    parser.add_argument("--csv", type=str, default="data/theme_from_etf_dump.csv", help="입력 CSV 파일 경로")
    args = parser.parse_args()
    
    csv_file = Path(args.csv)
    if not csv_file.exists():
        csv_file = ROOT / args.csv
        
    if not csv_file.exists():
        print(f"[ERROR] 파일을 찾을 수 없습니다: {args.csv}")
        sys.exit(1)
        
    import_theme_etf_csv(csv_file)
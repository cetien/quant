"""
tools/import_sectors.py
KRX CSV에서 sectors + stock_sector_map 일괄 입력 (1회용).

사용법:
    python tools/import_sectors.py --csv <KRX_CSV_경로> [--dry-run]

KRX CSV 컬럼 가정 (실제 파일 확인 후 COLUMN_MAP 수정):
    단축코드, 한글 종목명, 시장구분명, 업종명
    → ticker, name, market, sector_name

실행 순서:
    1. sectors 테이블에 고유 섹터 삽입 (id 자동 부여)
    2. stocks 테이블에 종목 upsert (ticker 기준)
    3. stock_sector_map에 매핑 삽입 (weight=1.0, 단일 섹터 가정)
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from storage.db_manager import DuckDBManager  # noqa: E402

# ── KRX CSV 컬럼 → 내부 컬럼명 매핑 (파일에 따라 수정) ────────────────────
COLUMN_MAP = {
    "ticker":    "ticker",
    "name":  "name",
    "market":   "market",
    "sector":      "sector_name",
}

MARKET_NORMALIZE = {
    "유가증권시장": "KOSPI",
    "코스닥":      "KOSDAQ",
    "KOSPI":       "KOSPI",
    "KOSDAQ":      "KOSDAQ",
}


def load_krx_csv(path: Path) -> pd.DataFrame:
    for enc in ("cp949", "utf-8-sig", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=enc, dtype=str)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"CSV 인코딩 판별 실패: {path}")

    df.columns = df.columns.str.strip()

    missing = [k for k in COLUMN_MAP if k not in df.columns]
    if missing:
        print(f"[ERROR] CSV에서 컬럼을 찾을 수 없음: {missing}")
        print(f"  실제 컬럼: {list(df.columns)}")
        print("  COLUMN_MAP을 수정하세요.")
        sys.exit(1)

    df = df.rename(columns=COLUMN_MAP)[list(COLUMN_MAP.values())]
    df = df.dropna(subset=["ticker", "name", "sector_name"])
    df["ticker"]      = df["ticker"].str.strip().str.zfill(6)
    df["market"]      = df["market"].str.strip().map(MARKET_NORMALIZE)
    df["sector_name"] = df["sector_name"].str.strip()
    df = df[df["market"].isin(["KOSPI", "KOSDAQ"])]
    print(f"[INFO] 로드 완료: {len(df)}행 (KOSPI/KOSDAQ 필터 후)")
    return df.reset_index(drop=True)


def import_sectors(df: pd.DataFrame, db: DuckDBManager, dry_run: bool) -> None:
    # ── 1. sectors 삽입 ──────────────────────────────────────────────────────
    unique_sectors = sorted(df["sector_name"].unique())
    print(f"[INFO] 고유 섹터 수: {len(unique_sectors)}")

    if not dry_run:
        existing = set(
            r[0] for r in db.con.execute("SELECT name FROM sectors").fetchall()
        )
        new_id = db.con.execute(
            "SELECT COALESCE(MAX(id), 0) FROM sectors"
        ).fetchone()[0]
        for sec_name in unique_sectors:
            if sec_name not in existing:
                new_id += 1
                db.con.execute(
                    "INSERT INTO sectors (id, name, rating) VALUES (?, ?, 0)",
                    [new_id, sec_name],
                )
        print("[OK] sectors 삽입 완료.")
    else:
        print(f"[DRY-RUN] sectors 삽입 대상 (최초 5개): {unique_sectors[:5]}")

    # ── 섹터명 → id 매핑 로드 ────────────────────────────────────────────────
    sec_df  = db.query("SELECT id, name FROM sectors")
    sec_map = dict(zip(sec_df["name"], sec_df["id"]))

    # ── 2. stocks upsert ─────────────────────────────────────────────────────
    stocks_df = df[["ticker", "name", "market"]].drop_duplicates("ticker")
    if not dry_run:
        db.upsert_dataframe(stocks_df, "stocks", pk_cols=["ticker"])
        print(f"[OK] stocks upsert 완료: {len(stocks_df)}행")
    else:
        print(f"[DRY-RUN] stocks upsert 대상: {len(stocks_df)}행")

    # ── 3. stock_sector_map 삽입 ─────────────────────────────────────────────
    df["sector_id"] = df["sector_name"].map(sec_map)
    df["weight"]    = 1.0
    map_df = df[["ticker", "sector_id", "weight"]].dropna(subset=["sector_id"])
    map_df = map_df.copy()
    map_df["sector_id"] = map_df["sector_id"].astype(int)

    if not dry_run:
        db.upsert_dataframe(map_df, "stock_sector_map", pk_cols=["ticker", "sector_id"])
        print(f"[OK] stock_sector_map 삽입 완료: {len(map_df)}행")
    else:
        print(f"[DRY-RUN] stock_sector_map 삽입 대상: {len(map_df)}행")
        print(map_df.head(10).to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="KRX CSV → sectors / stock_sector_map 일괄 입력"
    )
    parser.add_argument("--csv",      required=True,      help="KRX CSV 파일 경로")
    parser.add_argument("--dry-run",  action="store_true", help="DB 미변경, 결과만 출력")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[ERROR] 파일 없음: {csv_path}")
        sys.exit(1)

    df = load_krx_csv(csv_path)

    with DuckDBManager() as db:
        import_sectors(df, db, dry_run=args.dry_run)

    print("[DONE]")


if __name__ == "__main__":
    main()

"""
storage/parquet_store.py
Cold 데이터 Parquet 파티셔닝 저장/로드
경로 규칙: data/raw/{category}/{year}/{month}/data.parquet
"""
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common.config import cfg
from common.logger import get_logger

log = get_logger(__name__)


def _partition_path(base_dir: Path, date: pd.Timestamp) -> Path:
    return base_dir / str(date.year) / f"{date.month:02d}"


def save_prices(df: pd.DataFrame, category: str = "prices") -> None:
    """
    일봉 데이터를 연/월 파티션으로 저장.
    date 컬럼 필수. 기존 파티션은 덮어쓰지 않고 append.
    """
    if "date" not in df.columns:
        raise ValueError("DataFrame must have 'date' column.")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    base = cfg.storage.raw_prices_dir if category == "prices" else \
           cfg.storage.raw_supply_dir if category == "supply" else \
           cfg.storage.raw_macro_dir

    for (year, month), group in df.groupby([df["date"].dt.year, df["date"].dt.month]):
        out_dir = base / str(year) / f"{month:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        if out_path.exists():
            existing = pq.read_table(out_path).to_pandas()
            combined = pd.concat([existing, group], ignore_index=True)
            pk = ["ticker", "date"] if "ticker" in combined.columns else ["date"]
            combined = combined.drop_duplicates(subset=pk, keep="last")
        else:
            combined = group

        table = pa.Table.from_pandas(combined)
        pq.write_table(
            table, out_path,
            compression=cfg.storage.parquet_compression
        )
        log.info(f"Parquet saved: {out_path} ({len(combined)} rows)")


def load_prices(
    category: str = "prices",
    start: Optional[str] = None,
    end: Optional[str] = None,
    tickers: Optional[list] = None,
) -> pd.DataFrame:
    """
    Parquet Cold 데이터 로드. DuckDB read_parquet() 직접 쿼리 대안.
    start/end: 'YYYY-MM-DD' 문자열
    """
    import duckdb

    base = cfg.storage.raw_prices_dir if category == "prices" else \
           cfg.storage.raw_supply_dir if category == "supply" else \
           cfg.storage.raw_macro_dir

    pattern = str(base / "**" / "*.parquet")
    con = duckdb.connect()

    where_clauses = []
    if start:
        where_clauses.append(f"date >= '{start}'")
    if end:
        where_clauses.append(f"date <= '{end}'")
    if tickers:
        tickers_str = ", ".join([f"'{t}'" for t in tickers])
        where_clauses.append(f"ticker IN ({tickers_str})")

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f"SELECT * FROM read_parquet('{pattern}', union_by_name=true) {where}"

    df = con.execute(sql).df()
    con.close()
    log.info(f"Parquet loaded: {category}, {len(df)} rows")
    return df

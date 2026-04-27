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
import duckdb

from common.config import cfg
from common.logger import get_logger

log = get_logger(__name__)


def _partition_path(base_dir: Path, date: pd.Timestamp) -> Path:
    return base_dir / str(date.year) / f"{date.month:02d}"


def _get_base_dir(category: str) -> Path:
    mapping = {
        "prices": cfg.storage.raw_prices_dir,
        "supply": cfg.storage.raw_supply_dir,
        "macro": cfg.storage.raw_macro_dir
    }
    return mapping.get(category, cfg.storage.raw_prices_dir)


def save_prices(df: pd.DataFrame, category: str = "prices") -> None:
    """
    일봉 데이터를 연/월 파티션으로 저장.
    date 컬럼 필수. 기존 파티션은 중복 제거 후 merge.

    수정 이력:
    - v1 버그: DuckDB SQL 안에서 pandas 변수명 'group'을 직접 참조
              → 'group'이 SQL 예약어로 파싱되어 Parser Error 발생
    - v2 수정: pandas DataFrame을 DuckDB에 register() 후 명시적 alias로 참조
    """
    if "date" not in df.columns:
        raise ValueError("DataFrame must have 'date' column.")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    base = _get_base_dir(category)

    for (year, month), partition_df in df.groupby([df["date"].dt.year, df["date"].dt.month]):
        out_dir = base / str(year) / f"{month:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        if out_path.exists():
            pk_cols = "ticker, date" if "ticker" in partition_df.columns else "date"
            con = duckdb.connect()

            # ★ 핵심 수정: pandas DataFrame을 register()로 등록 후 alias 참조
            # 'group'(예약어) 변수명 대신 'new_data'로 명시적 등록
            con.register("new_data", partition_df)

            query = f"""
                SELECT * FROM (
                    SELECT * FROM read_parquet('{out_path}')
                    UNION ALL BY NAME
                    SELECT * FROM new_data
                )
                QUALIFY ROW_NUMBER() OVER(PARTITION BY {pk_cols} ORDER BY date DESC) = 1
            """
            combined = con.execute(query).df()
            con.unregister("new_data")
            con.close()
        else:
            combined = partition_df

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
    base = _get_base_dir(category)

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

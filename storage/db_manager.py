"""
storage/db_manager.py
DuckDB 연결 관리 및 CRUD 인터페이스
- 연결 시 자동으로 init_schema() 실행 (테이블 없음 오류 방지)
- append-only upsert (증분 업데이트와 충돌 없음)
- Look-ahead bias 방지를 위한 as_of_date 기반 조회
"""
from pathlib import Path
import threading
from typing import Optional

import duckdb
import pandas as pd

from common.config import cfg
from common.logger import get_logger
from storage.schema import ALL_SCHEMAS


class DuckDBManager:
    """
    DuckDB 연결, 스키마 초기화, 데이터 적재/조회 담당.

    Usage:
        db = DuckDBManager()          # 연결 + 스키마 자동 초기화
        df = db.query("SELECT * FROM stocks LIMIT 5")
        db.close()
    """

    _schema_init_lock = threading.Lock()
    _schema_initialized_paths: set[str] = set()

    def __init__(self, db_path: Optional[Path] = None, auto_init: bool = True) -> None:
        self.logger  = get_logger(self.__class__.__name__)
        self.db_path = db_path or cfg.storage.db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self.logger.info(f"DuckDB connected: {self.db_path}")

        # ★ 핵심 수정: 연결 즉시 스키마 자동 초기화 (IF NOT EXISTS → 멱등성 보장)
        if auto_init:
            self._ensure_schema_initialized()

    # ── 연결 관리 ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        self.con.close()
        self.logger.info("DuckDB connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ── 스키마 초기화 ─────────────────────────────────────────────────────────

    def init_schema(self) -> None:
        """모든 테이블 DDL 실행 (IF NOT EXISTS → 멱등성 보장, 반복 호출 안전)."""
        for ddl in ALL_SCHEMAS:
            self.con.execute(ddl)
        self.logger.info("Schema initialized (or already exists).")

    def _ensure_schema_initialized(self) -> None:
        """
        같은 프로세스에서 동일 DB 파일의 자동 스키마 초기화는 한 번만 수행한다.
        Streamlit rerun 시 중복 DDL로 인한 catalog 충돌을 방지한다.
        """
        db_key = str(self.db_path.resolve())
        if db_key in self._schema_initialized_paths:
            return

        with self._schema_init_lock:
            if db_key in self._schema_initialized_paths:
                return
            self.init_schema()
            self._schema_initialized_paths.add(db_key)

    # ── 쿼리 유틸 ─────────────────────────────────────────────────────────────

    def execute(self, sql: str, params: Optional[list] = None) -> None:
        """DML/DDL 실행 (반환값 없음)."""
        if params:
            self.con.execute(sql, params)
        else:
            self.con.execute(sql)

    def query(self, sql: str, params: Optional[list] = None) -> pd.DataFrame:
        """SELECT 실행 → DataFrame 반환 (Zero-Copy Arrow 경유)."""
        if params:
            rel = self.con.execute(sql, params)
        else:
            rel = self.con.execute(sql)
        return rel.df()

    # ── 데이터 적재 (append-only) ─────────────────────────────────────────────

    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        pk_cols: list[str],
    ) -> int:
        """
        DataFrame을 테이블에 append-only upsert.
        - 이미 존재하는 PK는 SKIP (INSERT OR IGNORE 방식)
        - CREATE OR REPLACE TABLE 미사용 → 증분 업데이트 보장

        Returns
        -------
        int : 테이블 전체 행 수
        """
        if df.empty:
            self.logger.warning(f"upsert_dataframe: empty DataFrame for '{table}', skip.")
            return 0

        # DuckDB가 df를 직접 참조 (Zero-Copy)
        self.con.register("_tmp_df", df)
        insert_cols = list(df.columns)
        insert_col_sql = ", ".join(insert_cols)
        pk_condition = " AND ".join(
            [f"t.{c} = s.{c}" for c in pk_cols]
        )
        sql = f"""
            INSERT INTO {table} ({insert_col_sql})
            SELECT {insert_col_sql}
            FROM _tmp_df AS s
            WHERE NOT EXISTS (
                SELECT 1 FROM {table} AS t
                WHERE {pk_condition}
            )
        """
        self.con.execute(sql)
        self.con.unregister("_tmp_df")

        count = self.con.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        self.logger.info(f"upsert '{table}': {len(df)} rows received, total in table: {count}")
        return count

    # ── Look-ahead bias 안전 조회 ─────────────────────────────────────────────

    def query_as_of(self, sql: str, as_of_date: str) -> pd.DataFrame:
        """
        as_of_date 이전 데이터만 조회하는 래퍼.
        SQL 내 {as_of_date} 플레이스홀더를 치환.

        Usage:
            df = db.query_as_of(
                "SELECT * FROM fundamentals WHERE announce_date <= '{as_of_date}'",
                as_of_date="2024-06-30"
            )
        """
        safe_sql = sql.replace("{as_of_date}", as_of_date)
        return self.query(safe_sql)

    # ── 편의 메서드 ───────────────────────────────────────────────────────────

    def get_last_date(self, table: str, date_col: str = "date") -> Optional[str]:
        """테이블의 최신 날짜 반환 (증분 업데이트 기준점). 테이블이 비어있으면 None."""
        try:
            result = self.con.execute(
                f"SELECT MAX({date_col}) FROM {table}"
            ).fetchone()
            val = result[0] if result else None
            return str(val) if val else None
        except Exception:
            return None

    def table_exists(self, table: str) -> bool:
        result = self.con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_name = '{table}'"
        ).fetchone()
        return result[0] > 0

    def row_count(self, table: str) -> int:
        """테이블 행 수 반환. 테이블 없으면 0."""
        try:
            return self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            return 0

    # ── 섹터 / 테마 편의 메서드 ───────────────────────────────────────────────

    def get_sector_map(self) -> pd.Series:
        """
        종목 → 대표 섹터명 매핑 Series 반환 (weight 최대 섹터 기준).
        scorer.py / selector.py의 sector_map 인자로 직접 사용 가능.

        Returns: pd.Series  index=ticker, values=sector(TEXT)
        """
        df = self.query("SELECT ticker, sector FROM v_stock_primary_sector")
        if df.empty:
            return pd.Series(dtype=str)
        return df.set_index("ticker")["sector"]

    def upsert_theme(self, name: str, description: str = "", is_active: bool = True) -> int:
        """
        테마 삽입 또는 업데이트 (name 기준 UPSERT).
        updated_at 갱신은 애플리케이션 레이어에서 처리 (DuckDB 트리거 미지원).

        Returns: int theme_id
        """
        existing = self.con.execute(
            "SELECT theme_id FROM themes WHERE name = ?", [name]
        ).fetchone()
        if existing:
            theme_id = existing[0]
            self.con.execute(
                """UPDATE themes
                   SET description = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE theme_id = ?""",
                [description, is_active, theme_id],
            )
        else:
            max_id = self.con.execute(
                "SELECT COALESCE(MAX(theme_id), 0) FROM themes"
            ).fetchone()[0]
            theme_id = max_id + 1
            self.con.execute(
                "INSERT INTO themes (theme_id, name, description, is_active) VALUES (?, ?, ?, ?)",
                [theme_id, name, description, is_active],
            )
        self.logger.info(f"upsert_theme: theme_id={theme_id}, name='{name}'")
        return theme_id

    def deactivate_theme_mapping(self, ticker: str, theme_id: int) -> None:
        """
        종목-테마 매핑 비활성화 (valid_to = CURRENT_DATE).
        PK에 valid_from 포함이므로 valid_to IS NULL 조건으로 현재 유효 레코드만 대상.
        """
        self.con.execute(
            """UPDATE stock_theme_map
               SET valid_to = CURRENT_DATE
               WHERE ticker = ? AND theme_id = ? AND valid_to IS NULL""",
            [ticker, theme_id],
        )
        self.logger.info(f"deactivate_theme_mapping: ticker={ticker}, theme_id={theme_id}")

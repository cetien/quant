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
            self.migrate()          # ★ 스키마 초기화 직후 마이그레이션 실행
            self._schema_initialized_paths.add(db_key)

    def migrate(self) -> None:
        """
        기존 DB를 삭제하지 않고 컬럼을 추가하는 마이그레이션.
        - DuckDB는 ADD COLUMN IF NOT EXISTS 미지원 → information_schema로 존재 여부 확인 후 실행
        - 신규 컬럼은 항상 이 메서드에서 관리 (init_schema DDL과 이중 관리)
        """
        migrations: list[tuple[str, str, str]] = [
            # (table, column, ALTER TABLE SQL)
            (
                "stocks",
                "rating",
                "ALTER TABLE stocks ADD COLUMN rating INTEGER DEFAULT 5",
            ),
            # 기존에 이미 rating이 0으로 만들어진 경우를 위해 DEFAULT 값 변경 레이어 추가
            (
                "stocks",
                "rating_default_fix",  # 가상의 식별자
                "ALTER TABLE stocks ALTER rating SET DEFAULT 5",
            ),
            (
                "sectors",
                "sectors_rating_default_fix",
                "ALTER TABLE sectors ALTER rating SET DEFAULT 5",
            ),
            (
                "themes",
                "rating",
                "ALTER TABLE themes ADD COLUMN rating INTEGER DEFAULT 5",
            ),
        ]
        for table, column, ddl in migrations:
            # 1. 테이블 존재 여부 확인
            if not self.table_exists(table):
                self.logger.debug(f"migrate: '{table}' 테이블이 없어 건너뜁니다.")
                continue

            # 2. 실행 여부 판단
            if "_default_fix" in column:
                # 이미 기본값이 5인지 확인하거나, 매번 실행해도 무방한 DDL인 경우 시도
                try:
                    self.con.execute(ddl)
                    self.logger.info(f"migrate: {table} 테이블의 {column.split('_')[0]} 기본값을 5로 변경했습니다.")
                except Exception:
                    pass
                continue

            # 일반적인 컬럼 추가 로직
            table_info = self.con.execute(f"PRAGMA table_info('{table}')").fetchall()
            column_names = [col[1].lower() for col in table_info]
            exists = column.lower() in column_names

            if not exists:
                try:
                    self.con.execute(ddl)
                    # NOT NULL 제약조건이 필요한 경우 추가 실행 (선택 사항)
                    # self.con.execute(f"ALTER TABLE {table} ALTER {column} SET NOT NULL")
                    self.logger.info(f"migrate: '{table}' 테이블에 '{column}' 컬럼을 추가했습니다.")
                except Exception as e:
                    self.logger.error(f"migrate 실패 - {table}.{column}: {e}")

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
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE lower(table_name) = lower(?)", [table]
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

    def upsert_theme(self, name: str, description: str = "", is_active: bool = True, rating: int = 5) -> int:
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
                   SET description = ?, is_active = ?, rating = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE theme_id = ?""",
                [description, is_active, rating, theme_id],
            )
        else:
            max_id = self.con.execute(
                "SELECT COALESCE(MAX(theme_id), 0) FROM themes"
            ).fetchone()[0]
            theme_id = max_id + 1
            self.con.execute(
                "INSERT INTO themes (theme_id, name, description, is_active, rating) VALUES (?, ?, ?, ?, ?)",
                [theme_id, name, description, is_active, rating],
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

    # ── stock_cache 갱신 ──────────────────────────────────────────────────────

    def refresh_stock_cache(self, tickers: list[str] | None = None) -> int:
        """
        stock_cache 테이블을 갱신한다.

        - 상승률: adj_close 기준, 30/90/180 캘린더일 전 가격 대비
        - PER/PBR/ROE: fundamentals 최신 레코드 (announce_date 기준)
        - tickers=None 이면 stocks 테이블의 전체 활성 종목 대상

        Returns
        -------
        int : 갱신된 행 수
        """
        if tickers:
            ticker_in = ", ".join(f"'{t}'" for t in tickers)
            ticker_filter = f"AND ticker IN ({ticker_in})"
        else:
            ticker_filter = ""

        sql = f"""
        INSERT OR REPLACE INTO stock_cache
            (ticker, last_date, ret_1m, ret_3m, ret_6m, per, pbr, roe, updated_at)

        WITH latest AS (
            -- 종목별 최신 거래일 + 그날 adj_close
            SELECT
                ticker,
                MAX(date)                        AS last_date,
                LAST(adj_close ORDER BY date)    AS close_now
            FROM daily_prices
            WHERE 1=1 {ticker_filter}
            GROUP BY ticker
        ),
        past AS (
            -- 30/90/180일 전 가장 가까운 과거 adj_close
            SELECT
                p.ticker,
                MAX(CASE WHEN p.date <= (l.last_date - INTERVAL '30' DAY)
                         THEN p.adj_close END)   AS close_1m,
                MAX(CASE WHEN p.date <= (l.last_date - INTERVAL '90' DAY)
                         THEN p.adj_close END)   AS close_3m,
                MAX(CASE WHEN p.date <= (l.last_date - INTERVAL '180' DAY)
                         THEN p.adj_close END)   AS close_6m
            FROM daily_prices p
            JOIN latest l ON l.ticker = p.ticker
            WHERE p.date >= (l.last_date - INTERVAL '181' DAY)
            GROUP BY p.ticker
        ),
        fund AS (
            -- 종목별 최신 공시 기준 PER/PBR/ROE
            SELECT
                f.ticker, f.per, f.pbr, f.roe
            FROM fundamentals f
            JOIN (
                SELECT ticker, MAX(announce_date) AS max_ad
                FROM fundamentals
                WHERE announce_date IS NOT NULL
                GROUP BY ticker
            ) mx ON mx.ticker = f.ticker AND mx.max_ad = f.announce_date
        )

        SELECT
            l.ticker,
            l.last_date,
            CASE WHEN p.close_1m > 0 THEN (l.close_now - p.close_1m) / p.close_1m END AS ret_1m,
            CASE WHEN p.close_3m > 0 THEN (l.close_now - p.close_3m) / p.close_3m END AS ret_3m,
            CASE WHEN p.close_6m > 0 THEN (l.close_now - p.close_6m) / p.close_6m END AS ret_6m,
            f.per,
            f.pbr,
            f.roe,
            CURRENT_TIMESTAMP
        FROM latest l
        LEFT JOIN past   p ON p.ticker = l.ticker
        LEFT JOIN fund   f ON f.ticker = l.ticker
        """

        self.con.execute(sql)
        count = self.con.execute("SELECT COUNT(*) FROM stock_cache").fetchone()[0]
        self.logger.info(f"refresh_stock_cache 완료: {count}행")
        return count

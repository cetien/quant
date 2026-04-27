from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3] # d:/Trabajo/ai/quant
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from storage.db_manager import DuckDBManager

"""
파일명 규약: YYYYMMDD_종목명_리포트제목_작성자.pdf
예시: 20231027_삼성전자_3분기실적리뷰_홍길동.pdf
"""

DEFAULT_PDF_DIR = ROOT_DIR / "data" / "pdf"
DEFAULT_DB_PATH = ROOT_DIR / "db" / "reports.db"
DEFAULT_LOG_PATH = ROOT_DIR / "data" / "ingestion.log"


@dataclass(frozen=True)
class ParsedFilename:
    date: str
    ticker: str
    company_name: str
    title: str
    writer: str
    filepath: str
    file_hash: str


# ----------------------------
# Logger
# ----------------------------
def build_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ingestion")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


# ----------------------------
# Utils
# ----------------------------
def calculate_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with file_path.open("rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


# ----------------------------
# Filename Parser
# ----------------------------
def parse_filename(path: Path, db: DuckDBManager) -> ParsedFilename:
    if path.suffix.lower() != ".pdf":
        raise ValueError("not a pdf")

    parts = path.stem.split("_")
    if len(parts) < 4:
        raise ValueError("invalid filename format")

    # date
    try:
        date = datetime.strptime(parts[0], "%Y%m%d").date().isoformat()
    except ValueError:
        raise ValueError("invalid date")

    company_name = parts[1].strip()
    
    # stocks 테이블에서 종목명으로 ticker 조회
    ticker_df = db.query("SELECT ticker FROM stocks WHERE name = ?", [company_name])
    if ticker_df.empty:
        raise ValueError(f"unknown company: {company_name} (stocks 테이블에 없음)")
    
    ticker = ticker_df.iloc[0]['ticker']

    title = " ".join(parts[2:-1]).replace("-", " ").strip()
    writer = parts[-1].replace("-", " ").strip()

    return ParsedFilename(
        date=date,
        ticker=ticker,
        company_name=company_name,
        title=title,
        writer=writer,
        filepath=str(path.resolve()),
        file_hash=calculate_sha256(path),
    )

# ----------------------------
# DB Logic
# ----------------------------
def ensure_schema(db: DuckDBManager):
    """DuckDB에 리포트 테이블 생성"""
    db.execute("""
    CREATE TABLE IF NOT EXISTS pdf_reports (
        id INTEGER PRIMARY KEY,
        date DATE NOT NULL,
        ticker TEXT,
        title TEXT,
        writer TEXT,
        filepath TEXT UNIQUE,
        file_hash TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

def is_duplicate(db: DuckDBManager, parsed: ParsedFilename) -> bool:
    res = db.query(
        """
        SELECT 1 FROM pdf_reports
        WHERE filepath=? OR file_hash=?
        """,
        (parsed.filepath, parsed.file_hash),
    )
    return not res.empty

def insert_report(db: DuckDBManager, parsed: ParsedFilename):
    # ID 생성 (단순 MAX+1)
    max_id_df = db.query("SELECT COALESCE(MAX(id), 0) as max_id FROM pdf_reports")
    new_id = int(max_id_df.iloc[0]['max_id']) + 1

    db.execute(
        """
        INSERT INTO pdf_reports (
            id,
            date,
            ticker,
            title,
            writer,
            filepath,
            file_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_id,
            parsed.date,
            parsed.ticker,
            parsed.title,
            parsed.writer,
            parsed.filepath,
            parsed.file_hash,
        ),
    )


def log_error(logger, file_path: Path, e: Exception):
    logger.error("Failed: %s | %s", file_path.name, e)


# ----------------------------
# Main ingestion
# ----------------------------
def ingest(pdf_dir: Path, log_path: Path):
    logger = build_logger(log_path)
    
    inserted = 0
    skipped = 0
    failed = 0

    with DuckDBManager() as db:
        ensure_schema(db)
        
        for file in sorted(pdf_dir.glob("*.pdf")):
            try:
                parsed = parse_filename(file, db)

                if is_duplicate(db, parsed):
                    skipped += 1
                    logger.info("Skip duplicate: %s", file.name)
                    continue

                insert_report(db, parsed)
                inserted += 1
                logger.info("Inserted: %s", file.name)
            except Exception as e:
                failed += 1
                log_error(logger, file, e)

    logger.info(
        "DONE | inserted=%s skipped=%s failed=%s",
        inserted,
        skipped,
        failed,
    )


# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf-dir", type=Path, default=DEFAULT_PDF_DIR)
    parser.add_argument("--log-path", type=Path, default=DEFAULT_LOG_PATH)

    args = parser.parse_args()

    ingest(args.pdf_dir, args.log_path)


if __name__ == "__main__":
    main()
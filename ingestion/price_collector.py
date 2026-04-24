"""
ingestion/price_collector.py
일봉 OHLCV 수집: pykrx (KR 국내) + yfinance (글로벌 보완)
- 거래대금(amount): pykrx 제공 / yfinance 미제공
- 증분 업데이트: last_date 이후 구간만 수집
- Rate Limit 대응: tenacity 기반 exponential backoff
"""
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import time

from common.config import cfg
from common.logger import get_logger
from storage.db_manager import DuckDBManager
from storage.parquet_store import save_prices

log = get_logger(__name__)

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
    HAS_TENACITY = True
except ImportError:
    HAS_TENACITY = False
    log.warning("tenacity 미설치. 단순 재시도 로직 사용.")


def _retry_decorator():
    if HAS_TENACITY:
        return retry(
            stop=stop_after_attempt(cfg.ingestion.retry_max),
            wait=wait_exponential(
                multiplier=cfg.ingestion.retry_backoff, min=1, max=30
            ),
        )
    return lambda f: f  # no-op


class PriceCollector:
    """
    KR 일봉 OHLCV + 거래대금 수집 (pykrx 우선, yfinance 보완).
    """

    def __init__(self, db: DuckDBManager) -> None:
        self.db = db
        self._check_dependencies()

    def _check_dependencies(self):
        try:
            import pykrx  # noqa
        except ImportError:
            log.warning("pykrx 미설치: pip install pykrx  ← 거래대금·수급 수집 불가")
        try:
            import yfinance  # noqa
        except ImportError:
            log.warning("yfinance 미설치: pip install yfinance")

    # ── 종목 목록 조회 ────────────────────────────────────────────────────────

    def fetch_stock_list(self) -> pd.DataFrame:
        """
        pykrx에서 KOSPI + KOSDAQ 전체 종목 목록 수집.
        반환: [ticker, name, market]
        섹터/산업은 별도 소스(FinanceDataReader 등) 필요.
        """
        try:
            from pykrx import stock
            today = date.today().strftime("%Y%m%d")
            rows = []
            for market in ["KOSPI", "KOSDAQ"]:
                tickers = stock.get_market_ticker_list(today, market=market)
                for t in tickers:
                    name = stock.get_market_ticker_name(t)
                    rows.append({"ticker": t, "name": name, "market": market})
            df = pd.DataFrame(rows)
            log.info(f"종목 목록 수집: {len(df)}개")
            return df
        except Exception as e:
            log.error(f"fetch_stock_list 실패: {e}")
            return pd.DataFrame(columns=["ticker", "name", "market"])

    def update_stock_master(self) -> None:
        """stocks 테이블 최신 상태 동기화."""
        df = self.fetch_stock_list()
        if df.empty:
            return
        self.db.upsert_dataframe(df, "stocks", pk_cols=["ticker"])
        log.info("stocks 테이블 업데이트 완료.")

    # ── 일봉 수집 (pykrx) ────────────────────────────────────────────────────

    def fetch_daily_ohlcv_pykrx(
        self,
        ticker: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """단일 종목 일봉 수집 (거래대금 포함)."""
        try:
            from pykrx import stock
            raw = stock.get_market_ohlcv_by_date(
                start.replace("-", ""),
                end.replace("-", ""),
                ticker,
            )
            if raw.empty:
                return pd.DataFrame()

            raw = raw.reset_index()
            raw.columns = ["date", "open", "high", "low", "close", "volume", "amount", "changes"]
            raw["ticker"] = ticker
            raw["adj_close"] = raw["close"]  # pykrx 수정주가: 별도 API 필요
            raw["date"] = pd.to_datetime(raw["date"])
            return raw[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume", "amount"]]
        except Exception as e:
            log.error(f"pykrx 수집 실패 [{ticker}]: {e}")
            return pd.DataFrame()

    # ── 증분 업데이트 ─────────────────────────────────────────────────────────

    def incremental_update_daily_prices(
        self,
        tickers: Optional[List[str]] = None,
        end: Optional[str] = None,
    ) -> None:
        """
        전체 종목(또는 지정 종목) 일봉 증분 업데이트.
        last_date 이후 구간만 수집 → DB + Parquet 모두 적재.
        """
        if tickers is None:
            df_stocks = self.db.query("SELECT ticker FROM stocks WHERE is_active = TRUE")
            tickers = df_stocks["ticker"].tolist()

        end = end or date.today().strftime("%Y-%m-%d")
        last_date = self.db.get_last_date("daily_prices")
        start = (
            (pd.Timestamp(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            if last_date else cfg.ingestion.default_start_date
        )

        if start > end:
            log.info("이미 최신 상태. 수집 불필요.")
            return

        log.info(f"일봉 증분 수집: {len(tickers)}개 종목, {start} ~ {end}")
        all_rows = []

        for i, ticker in enumerate(tickers):
            df = self.fetch_daily_ohlcv_pykrx(ticker, start, end)
            if not df.empty:
                all_rows.append(df)

            # Rate Limit 방지
            time.sleep(cfg.ingestion.pykrx_delay_sec)

            if (i + 1) % 100 == 0:
                log.info(f"  진행: {i+1}/{len(tickers)}")

        if all_rows:
            combined = pd.concat(all_rows, ignore_index=True)
            self.db.upsert_dataframe(combined, "daily_prices", pk_cols=["ticker", "date"])
            save_prices(combined, category="prices")
            log.info(f"일봉 수집 완료: {len(combined)}행")
        else:
            log.warning("수집된 데이터 없음.")

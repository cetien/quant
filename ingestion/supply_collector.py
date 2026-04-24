"""
ingestion/supply_collector.py
기관·외인 순매수 수급 데이터 수집 (pykrx)
"""
from datetime import date, timedelta
from typing import List, Optional
import time

import pandas as pd

from common.config import cfg
from common.logger import get_logger
from storage.db_manager import DuckDBManager
from storage.parquet_store import save_prices

log = get_logger(__name__)


class SupplyCollector:
    """기관·외인 순매수 수집."""

    def __init__(self, db: DuckDBManager) -> None:
        self.db = db

    def fetch_supply_pykrx(
        self,
        ticker: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        try:
            from pykrx import stock
            raw = stock.get_market_net_purchases_of_equities_by_date(
                start.replace("-", ""),
                end.replace("-", ""),
                ticker,
            )
            if raw.empty:
                return pd.DataFrame()

            raw = raw.reset_index()
            # pykrx 컬럼명: 날짜, 기관합계, 외국인합계 등 (버전마다 다름)
            raw.columns = [c.strip() for c in raw.columns]
            df = pd.DataFrame()
            df["date"]   = pd.to_datetime(raw.iloc[:, 0])
            df["ticker"] = ticker

            # 컬럼 탐색 (pykrx 버전 호환)
            col_names = raw.columns.tolist()
            def find_col(keywords):
                for kw in keywords:
                    for c in col_names:
                        if kw in c:
                            return c
                return None

            inst_col   = find_col(["기관합계", "기관"])
            foreign_col = find_col(["외국인합계", "외국인"])

            df["inst_net_buy"]    = raw[inst_col].values   if inst_col    else None
            df["foreign_net_buy"] = raw[foreign_col].values if foreign_col else None
            df["inst_net_amount"]    = None
            df["foreign_net_amount"] = None

            return df[["ticker", "date", "inst_net_buy", "foreign_net_buy",
                        "inst_net_amount", "foreign_net_amount"]]
        except Exception as e:
            log.error(f"supply 수집 실패 [{ticker}]: {e}")
            return pd.DataFrame()

    def incremental_update_supply(
        self,
        tickers: Optional[List[str]] = None,
        end: Optional[str] = None,
    ) -> None:
        if tickers is None:
            df_stocks = self.db.query("SELECT ticker FROM stocks WHERE is_active = TRUE")
            tickers = df_stocks["ticker"].tolist()

        end = end or date.today().strftime("%Y-%m-%d")
        last_date = self.db.get_last_date("supply")
        start = (
            (pd.Timestamp(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            if last_date else cfg.ingestion.default_start_date
        )

        if start > end:
            log.info("수급 데이터 이미 최신.")
            return

        log.info(f"수급 증분 수집: {len(tickers)}개 종목, {start} ~ {end}")
        all_rows = []

        for i, ticker in enumerate(tickers):
            df = self.fetch_supply_pykrx(ticker, start, end)
            if not df.empty:
                all_rows.append(df)
            time.sleep(cfg.ingestion.pykrx_delay_sec)
            if (i + 1) % 100 == 0:
                log.info(f"  진행: {i+1}/{len(tickers)}")

        if all_rows:
            combined = pd.concat(all_rows, ignore_index=True)
            self.db.upsert_dataframe(combined, "supply", pk_cols=["ticker", "date"])
            save_prices(combined, category="supply")
            log.info(f"수급 수집 완료: {len(combined)}행")

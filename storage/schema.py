"""
storage/schema.py
DuckDB 테이블 DDL 정의.
- STRICT 모드 대신 컬럼 타입 명시 + CHECK 제약으로 데이터 정밀도 보장
- announce_date 컬럼으로 Look-ahead bias 원천 차단
- delisted_stocks 테이블로 생존 편향 방지
"""

# ── 기준 테이블 ──────────────────────────────────────────────────────────────

CREATE_STOCKS = """
CREATE TABLE IF NOT EXISTS stocks (
    ticker          TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    market          TEXT        NOT NULL CHECK (market IN ('KOSPI', 'KOSDAQ')),
    sector          TEXT,
    industry        TEXT,
    listed_date     DATE,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker)
);
"""

CREATE_DELISTED = """
CREATE TABLE IF NOT EXISTS delisted_stocks (
    ticker          TEXT        NOT NULL,
    name            TEXT,
    delist_date     DATE        NOT NULL,
    delist_reason   TEXT,
    PRIMARY KEY (ticker)
);
"""

# ── 가격 테이블 ──────────────────────────────────────────────────────────────

CREATE_DAILY_PRICES = """
CREATE TABLE IF NOT EXISTS daily_prices (
    ticker          TEXT        NOT NULL REFERENCES stocks(ticker),
    date            DATE        NOT NULL,
    open            DOUBLE      NOT NULL CHECK (open > 0),
    high            DOUBLE      NOT NULL CHECK (high >= open),
    low             DOUBLE      NOT NULL CHECK (low <= open),
    close           DOUBLE      NOT NULL CHECK (close > 0),
    adj_close       DOUBLE      NOT NULL CHECK (adj_close > 0),
    volume          BIGINT      NOT NULL CHECK (volume >= 0),
    amount          BIGINT               CHECK (amount >= 0),  -- pykrx 제공, yfinance 미제공
    ingested_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);
"""

# ── 수급 테이블 ──────────────────────────────────────────────────────────────

CREATE_SUPPLY = """
CREATE TABLE IF NOT EXISTS supply (
    ticker              TEXT    NOT NULL REFERENCES stocks(ticker),
    date                DATE    NOT NULL,
    inst_net_buy        BIGINT,         -- 기관 순매수 (주)
    foreign_net_buy     BIGINT,         -- 외인 순매수 (주)
    inst_net_amount     BIGINT,         -- 기관 순매수 (금액)
    foreign_net_amount  BIGINT,         -- 외인 순매수 (금액)
    ingested_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);
"""

# ── 매크로 지표 테이블 ───────────────────────────────────────────────────────

CREATE_MACRO = """
CREATE TABLE IF NOT EXISTS macro_indicators (
    indicator_code  TEXT        NOT NULL,   -- 예: 'USD_KRW', 'SOX', 'WTI'
    date            DATE        NOT NULL,
    value           DOUBLE      NOT NULL,
    change_rate     DOUBLE,                 -- 전일 대비 등락률
    ingested_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (indicator_code, date)
);
"""

# ── 재무 테이블 (Look-ahead bias 방지: announce_date 기준 join) ──────────────

CREATE_FUNDAMENTALS = """
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker          TEXT        NOT NULL REFERENCES stocks(ticker),
    report_date     DATE        NOT NULL,   -- 결산 기준일
    announce_date   DATE,                   -- 공시 실제 날짜 (join 기준)
    fiscal_quarter  TEXT,                   -- 예: '2024Q4'
    eps             DOUBLE,
    per             DOUBLE      CHECK (per > 0),
    pbr             DOUBLE      CHECK (pbr > 0),
    roe             DOUBLE,
    revenue         BIGINT,
    operating_income BIGINT,
    debt_ratio      DOUBLE,
    ingested_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, report_date, fiscal_quarter)
);
"""

# ── 거래일 캘린더 테이블 ─────────────────────────────────────────────────────

CREATE_TRADING_CALENDAR = """
CREATE TABLE IF NOT EXISTS trading_calendar (
    market          TEXT    NOT NULL,   -- 'KRX', 'NYSE' 등
    date            DATE    NOT NULL,
    is_open         BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (market, date)
);
"""

# ── 전체 DDL 실행 순서 ───────────────────────────────────────────────────────

ALL_SCHEMAS = [
    CREATE_STOCKS,
    CREATE_DELISTED,
    CREATE_DAILY_PRICES,
    CREATE_SUPPLY,
    CREATE_MACRO,
    CREATE_FUNDAMENTALS,
    CREATE_TRADING_CALENDAR,
]

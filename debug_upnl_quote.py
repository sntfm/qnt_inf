import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Configure pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 100)

# QuestDB connection settings
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

SOURCE_DEALS_TABLE = os.getenv("PNL_FLOW_DEALS_TABLE", "deals")
SOURCE_FEED_TABLE = os.getenv("PNL_FLOW_FEED_TABLE", "feed_kraken_1m")
MART_TABLE = os.getenv("PNL_FLOW_MART_TABLE", "mart_pnl_flow")
CONVMAP_TABLE = os.getenv("CONVMAP_TABLE", "map_decomposition_usd")

def _connect():
    """Create a SQLAlchemy engine for QuestDB's Postgres endpoint."""
    connection_string = f"postgresql://{QUESTDB_USER}:{QUESTDB_PASSWORD}@{QUESTDB_HOST}:{QUESTDB_PORT}/{QUESTDB_DB}"
    return create_engine(connection_string, connect_args={"connect_timeout": 30})

# Query a specific problematic instrument
date_str = "2025-10-26"
date_start = f"{date_str}T00:00:00.000000Z"
date_end = f"{date_str}T23:59:59.999999Z"

insert_sql = f"""
WITH
feed_all AS (
    SELECT DISTINCT
        ts,
        instrument,
        bid_px_0,
        ask_px_0
    FROM {SOURCE_FEED_TABLE}
    WHERE ts >= '{date_start}'
      AND ts <= '{date_end}'
      AND instrument IN ('Kraken.Spot.ETH/EUR_SPOT', 'Kraken.Spot.ETH/USDT_SPOT', 'Kraken.Spot.EUR/USD_SPOT')
),

deals_buy AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_buy,
        SUM(amt) AS amt_buy,
        COUNT(*) AS cnt_buy
    FROM {SOURCE_DEALS_TABLE}
    WHERE side = 'BUY'
      AND time >= '{date_start}'
      AND time <= '{date_end}'
      AND instrument = 'Kraken.Spot.ETH/EUR_SPOT'
    SAMPLE BY 1m ALIGN TO CALENDAR
),

deals_sell AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_sell,
        SUM(amt) AS amt_sell,
        COUNT(*) AS cnt_sell
    FROM {SOURCE_DEALS_TABLE}
    WHERE side = 'SELL'
      AND time >= '{date_start}'
      AND time <= '{date_end}'
      AND instrument = 'Kraken.Spot.ETH/EUR_SPOT'
    SAMPLE BY 1m ALIGN TO CALENDAR
),

rd AS (
    SELECT
        COALESCE(b.ts_1m, s.ts_1m) AS ts_1m,
        COALESCE(b.instrument, s.instrument) AS instrument,
        b.px_buy,
        s.px_sell,
        COALESCE(b.amt_buy, 0) AS amt_buy,
        COALESCE(s.amt_sell, 0) AS amt_sell,
        COALESCE(b.amt_buy, 0) - COALESCE(s.amt_sell, 0) AS amt_filled,
        LEAST(COALESCE(b.amt_buy, 0), COALESCE(s.amt_sell, 0)) AS amt_matched,
        COALESCE(b.cnt_buy, 0) + COALESCE(s.cnt_sell, 0) AS num_deals
    FROM deals_buy b
    FULL OUTER JOIN deals_sell s
        ON b.ts_1m = s.ts_1m
       AND b.instrument = s.instrument
),

convmap AS (
    SELECT DISTINCT
        instrument,
        is_major,
        instrument_base,
        instrument_quote,
        instrument_usd,
        inst_usd_is_inverted
    FROM {CONVMAP_TABLE}
    WHERE instrument = 'Kraken.Spot.ETH/EUR_SPOT'
),

base AS (
    SELECT
        f.ts AS ts,
        f.instrument,
        c.is_major,
        c.instrument_base,
        c.instrument_quote,
        c.instrument_usd,
        c.inst_usd_is_inverted,
        COALESCE(rd.amt_filled, 0) AS amt_filled,
        COALESCE(rd.amt_buy, 0) AS amt_buy,
        COALESCE(rd.amt_sell, 0) AS amt_sell,
        COALESCE(rd.amt_matched, 0) AS amt_matched,
        COALESCE(rd.px_buy, 0) AS px_buy,
        COALESCE(rd.px_sell, 0) AS px_sell,
        COALESCE(rd.num_deals, 0) AS num_deals,
        f.bid_px_0 AS px_bid_0,
        f.ask_px_0 AS px_ask_0,
        b.bid_px_0 AS px_bid_0_base,
        b.ask_px_0 AS px_ask_0_base,
        q.bid_px_0 AS px_bid_0_quote,
        q.ask_px_0 AS px_ask_0_quote,
        u.bid_px_0 AS px_bid_0_usd,
        u.ask_px_0 AS px_ask_0_usd
    FROM feed_all f
    LEFT JOIN rd
        ON f.ts = rd.ts_1m
       AND f.instrument = rd.instrument
    LEFT JOIN convmap c
        ON f.instrument = c.instrument
    LEFT JOIN feed_all u
        ON f.ts = u.ts
       AND c.instrument_usd = u.instrument
    LEFT JOIN feed_all b
        ON f.ts = b.ts
       AND c.instrument_base = b.instrument
    LEFT JOIN feed_all q
        ON f.ts = q.ts
       AND c.instrument_quote = q.instrument
    WHERE f.instrument = 'Kraken.Spot.ETH/EUR_SPOT'
)

SELECT * FROM base;
"""

engine = _connect()
df = pd.read_sql(insert_sql, engine)
engine.dispose()

print(f"Retrieved {len(df)} rows")
print("\n=== First few rows ===")
print(df.head(20))

# Now apply the same logic as in the main script
df['px_base'] = np.nan
df['px_quote'] = np.nan

amt = df['amt_filled'].fillna(0)
long = amt > 0
short = amt < 0
inv = df['inst_usd_is_inverted']

# Column PX
df['px'] = np.where(amt > 0, df['px_buy'], np.where(amt < 0, df['px_sell'], np.nan))

# Forward-fill base and quote instrument prices
base_bid = df['px_bid_0_base'].replace(0, np.nan).groupby(df['instrument']).ffill()
base_ask = df['px_ask_0_base'].replace(0, np.nan).groupby(df['instrument']).ffill()
quote_bid = df['px_bid_0_quote'].replace(0, np.nan).groupby(df['instrument']).ffill()
quote_ask = df['px_ask_0_quote'].replace(0, np.nan).groupby(df['instrument']).ffill()

df['px_bid_0_base'] = base_bid
df['px_ask_0_base'] = base_ask
df['px_bid_0_quote'] = quote_bid
df['px_ask_0_quote'] = quote_ask

ask_q = df['px_ask_0_quote']
bid_q = df['px_bid_0_quote']

# PX_BASE calculation
df.loc[long & ~inv, 'px_base']  = df['px_buy'] / bid_q
df.loc[long &  inv, 'px_base']  = df['px_buy'] * bid_q
df.loc[short & ~inv, 'px_base'] = df['px_sell'] / ask_q
df.loc[short &  inv, 'px_base'] = df['px_sell'] * ask_q

# PX_QUOTE calculation - THIS IS THE PROBLEM
df.loc[~inv, 'px_quote'] = df['px'] / df['px_base']
df.loc[inv, 'px_quote']  = df['px_base'] / df['px']

print("\n=== After px_quote calculation (rows with trades) ===")
print(df[df['amt_filled'] != 0][['ts', 'amt_filled', 'px', 'px_base', 'px_quote', 'px_bid_0_quote', 'px_ask_0_quote']].head(10))

print("\n=== After px_quote calculation (rows without trades) ===")
print(df[df['amt_filled'] == 0][['ts', 'amt_filled', 'px', 'px_base', 'px_quote', 'px_bid_0_quote', 'px_ask_0_quote']].head(10))

# Calculate cumulative amounts
df = df.sort_values(['instrument', 'ts'])
df['amt_filled'] = df['amt_filled'].fillna(0)
df['cum_amt'] = df.groupby('instrument')['amt_filled'].cumsum()

print("\n=== Cumulative amounts ===")
print(df[df['cum_amt'] != 0][['ts', 'amt_filled', 'cum_amt', 'px_quote', 'px_bid_0_quote']].head(20))

print("\n=== PROBLEM: px_quote is NaN/0 for rows without trades, even when position is open ===")

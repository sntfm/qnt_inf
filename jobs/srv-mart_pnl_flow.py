import os
import psycopg2
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# QuestDB connection settings
# ---------------------------------------------------------------------------
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
    """Create a new psycopg2 connection to QuestDB's Postgres endpoint."""
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DB,
        connect_timeout=30,
    )


def _update(date_str: str):
    print(f"Processing PnL flow data for {date_str}")

    date_start = f"{date_str}T00:00:00.000000Z"
    date_end = f"{date_str}T23:59:59.999999Z"

    insert_sql = f"""
WITH
-- Load all feed rows up to end of target day
feed_all AS (
    SELECT *
    FROM {SOURCE_FEED_TABLE}
    WHERE ts <= '{date_end}'
),

-- Bucket deals to 1min
deals_buy AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_buy,
        SUM(amt) AS amt_buy,
        COUNT(*) AS cnt_buy
    FROM {SOURCE_DEALS_TABLE}
    WHERE side = 'BUY'
      AND time <= '{date_end}'
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
      AND time <= '{date_end}'
    SAMPLE BY 1m ALIGN TO CALENDAR
),

-- Merge BUY/SELL buckets
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

-- Join FEED + DEALS + FX conversion
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
        rd.px_buy AS px_buy,
        rd.px_sell AS px_sell,
        COALESCE(rd.num_deals, 0) AS num_deals,
        -- Base instrument prices
        b.bid_px_0 AS px_bid_0_base,
        b.ask_px_0 AS px_ask_0_base,
        -- Quote instrument prices
        q.bid_px_0 AS px_bid_0_quote,
        q.ask_px_0 AS px_ask_0_quote,
        -- USD instrument prices
        u.bid_px_0 AS px_bid_0_usd,
        u.ask_px_0 AS px_ask_0_usd
    FROM feed_all f
    LEFT JOIN rd
        ON f.ts = rd.ts_1m
       AND f.instrument = rd.instrument
    LEFT JOIN {CONVMAP_TABLE} c
        ON f.instrument = c.instrument
    LEFT JOIN {SOURCE_FEED_TABLE} u
        ON f.ts = u.ts
       AND c.instrument_usd = u.instrument
    LEFT JOIN {SOURCE_FEED_TABLE} b
        ON f.ts = b.ts
       AND c.instrument_base = b.instrument
    LEFT JOIN {SOURCE_FEED_TABLE} q
        ON f.ts = q.ts
       AND c.instrument_quote = q.instrument
)

SELECT * FROM base;
    """
    # # print(insert_sql)
    # with _connect() as conn, conn.cursor() as cur:
    #     cur.execute(insert_sql)
    #     conn.commit()

    # print(f"Updated {MART_TABLE} for date {date_str}")
    
    with _connect() as conn:
        df = pd.read_sql(insert_sql, conn)

    print(f"Retrieved {len(df)} rows for date {date_str}")
######
    # Effective execution price in native
    df['px'] = np.where(
        df['amt_filled'].fillna(0) > 0, 
        df['px_buy'],
        np.where(
            df['amt_filled'].fillna(0) < 0,
            df['px_sell'],
            None
        )
    )


    # px_quote: Decomposed execution price for quote leg
    conditions_quote = [
        # Net long and non-inverted: px / base ask
        (df['amt_filled'].fillna(0) > 0) & (~df['inst_usd_is_inverted']),
        # Net long and inverted: px * base ask
        (df['amt_filled'].fillna(0) > 0) & (df['inst_usd_is_inverted']),
        # Net short and non-inverted: px / base ask
        (df['amt_filled'].fillna(0) < 0) & (~df['inst_usd_is_inverted']),
        # Net short and inverted: px * base ask
        (df['amt_filled'].fillna(0) < 0) & (df['inst_usd_is_inverted'])
    ]

    choices_quote = [
        df['px_buy'] / df['px_ask_0_base'].replace(0, np.nan),
        df['px_buy'] * df['px_ask_0_base'].replace(0, np.nan),
        df['px_sell'] / df['px_ask_0_base'].replace(0, np.nan),
        df['px_sell'] * df['px_ask_0_base'].replace(0, np.nan)
    ]

    df['px_quote'] = np.select(conditions_quote, choices_quote, default=None)

    # px_base: Decomposed execution price for base leg
    conditions_base = [
        # Net long and non-inverted: px / quote bid
        (df['amt_filled'].fillna(0) > 0) & (~df['inst_usd_is_inverted']),
        # Net long and inverted: px * quote bid
        (df['amt_filled'].fillna(0) > 0) & (df['inst_usd_is_inverted']),
        # Net short and non-inverted: px / quote bid
        (df['amt_filled'].fillna(0) < 0) & (~df['inst_usd_is_inverted']),
        # Net short and inverted: px * quote bid
        (df['amt_filled'].fillna(0) < 0) & (df['inst_usd_is_inverted'])
    ]

    choices_base = [
        df['px_buy'] / df['px_bid_0_quote'].replace(0, np.nan),
        df['px_buy'] * df['px_bid_0_quote'].replace(0, np.nan),
        df['px_sell'] / df['px_bid_0_quote'].replace(0, np.nan),
        df['px_sell'] * df['px_bid_0_quote'].replace(0, np.nan)
    ]

    df['px_base'] = np.select(conditions_base, choices_base, default=None)

    # rpnl: Realized PnL from matched trades
    # Only calculate when both px_sell and px_buy exist
    df['rpnl'] = np.where(
        df['px_sell'].notna() & df['px_buy'].notna(),
        (df['px_sell'] - df['px_buy']) * df['amt_matched'],
        0.0
    )

    # rpnl_usd: RPNL converted to USD
    usd_mid = (df['px_ask_0_usd'] + df['px_bid_0_usd']) / 2

    conditions_rpnl_usd = [
        df['instrument_usd'].isna(),
        df['inst_usd_is_inverted'],
        ~df['inst_usd_is_inverted']
    ]

    choices_rpnl_usd = [
        df['rpnl'],
        df['rpnl'] / usd_mid,
        df['rpnl'] * usd_mid
    ]

    df['rpnl_usd'] = np.select(conditions_rpnl_usd, choices_rpnl_usd, default=0.0)

    # vol_usd: Volume in USD (signed exposure change)
    df['vol_usd'] = df['amt_filled'] * df['px'] * (df['px_bid_0_usd']+df['px_ask_0_usd'])/2
######

    return df

if __name__ == "__main__":
    # for date_str in [
    #     "2025-10-20", "2025-10-21", "2025-10-22",
    #     "2025-10-23", "2025-10-24", "2025-10-25",
    #     "2025-10-26", "2025-10-27", "2025-10-28",
    #     "2025-10-29", "2025-10-30"
    # ]:
    #     _update(date_str)

    df = _update("2025-10-26")
    print(df.columns)

    print(df[df.px.notna()][['instrument', 'px_buy', 'px_sell', 'amt_buy', 'amt_sell', 'amt_matched', 'vol_usd', 'rpnl']].head(10))

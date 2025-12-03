import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from questdb.ingress import Sender, IngressError, TimestampNanos

# Configure pandas display options
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Auto-detect width
pd.set_option('display.max_colwidth', None)  # No truncation of column values

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
    """Create a SQLAlchemy engine for QuestDB's Postgres endpoint."""
    connection_string = f"postgresql://{QUESTDB_USER}:{QUESTDB_PASSWORD}@{QUESTDB_HOST}:{QUESTDB_PORT}/{QUESTDB_DB}"
    return create_engine(connection_string, connect_args={"connect_timeout": 30})


def _process(date_str: str):
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
        COALESCE(rd.px_buy, 0) AS px_buy,
        COALESCE(rd.px_sell, 0) AS px_sell,
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

    engine = _connect()
    df = pd.read_sql(insert_sql, engine)
    engine.dispose()

    print(f"Retrieved {len(df)} rows for date {date_str}")

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
    usd_mid = ((df['px_ask_0_usd'].fillna(0) + df['px_bid_0_usd'].fillna(0)) / 2).replace(0, np.nan)

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

    df['rpnl_usd'] = np.select(conditions_rpnl_usd, choices_rpnl_usd, default=0.0).astype(float)

    # vol_usd: Volume in USD (signed exposure change)
    # Need to convert native price (px) to USD, accounting for inverted instruments
    conditions_vol_usd = [
        df['instrument_usd'].isna(),
        df['inst_usd_is_inverted'],
        ~df['inst_usd_is_inverted']
    ]

    choices_vol_usd = [
        df['amt_filled'] * df['px'],  # Already in USD
        df['amt_filled'] * df['px'] / usd_mid,  # Inverted: divide
        df['amt_filled'] * df['px'] * usd_mid   # Non-inverted: multiply
    ]

    df['vol_usd'] = np.select(conditions_vol_usd, choices_vol_usd, default=0.0).astype(float)

    # cost_signed_usd: Cost basis using actual fill prices
    # Convert px_buy and px_sell to USD first
    px_buy_usd = np.select(
        [df['instrument_usd'].isna(), df['inst_usd_is_inverted'], ~df['inst_usd_is_inverted']],
        [df['px_buy'], df['px_buy'] / usd_mid, df['px_buy'] * usd_mid],
        default=np.nan
    )

    px_sell_usd = np.select(
        [df['instrument_usd'].isna(), df['inst_usd_is_inverted'], ~df['inst_usd_is_inverted']],
        [df['px_sell'], df['px_sell'] / usd_mid, df['px_sell'] * usd_mid],
        default=np.nan
    )

    # Calculate cost_signed_usd based on whether we have buys, sells, or both
    cost_conditions = [
        (df['amt_buy'] > 0) & (df['amt_sell'] > 0),  # Both buys and sells
        (df['amt_buy'] > 0) & (df['amt_sell'] == 0),  # Only buys
        (df['amt_buy'] == 0) & (df['amt_sell'] > 0)   # Only sells
    ]

    cost_choices = [
        # Both: cost = buys * buy_px_usd - sells * sell_px_usd
        (df['amt_buy'] * px_buy_usd) - (df['amt_sell'] * px_sell_usd),
        # Only buys
        df['amt_buy'] * px_buy_usd,
        # Only sells (negative cost)
        -(df['amt_sell'] * px_sell_usd)
    ]

    df['cost_signed_usd'] = np.select(cost_conditions, cost_choices, default=0.0).astype(float)

    # Cumulative calculations: running sums per instrument
    df = df.sort_values(['instrument', 'ts'])
    df['cum_amt'] = df.groupby('instrument')['amt_filled'].cumsum()
    df['cum_cost_usd'] = df.groupby('instrument')['cost_signed_usd'].cumsum()

    # Lagged values: previous cumulative snapshot
    df['prev_cum_amt'] = df.groupby('instrument')['cum_amt'].shift(1)
    df['prev_cum_cost_usd'] = df.groupby('instrument')['cum_cost_usd'].shift(1)

    # Compute realized PnL from reductions & flips
    # Helper function to calculate avg cost
    avg_cost = df['prev_cum_cost_usd'] / df['prev_cum_amt']

    # Market price to use for closing positions
    market_px_usd = np.where(
        df['prev_cum_amt'] > 0,
        df['px_bid_0_usd'],  # Long positions use bid
        df['px_ask_0_usd']   # Short positions use ask
    )

    # Realized PnL conditions
    rpnl_total_conditions = [
        # First row or prev position was flat: only bucket rpnl
        df['prev_cum_amt'].isna() | (df['prev_cum_amt'] == 0),

        # Position closed to zero or flipped sign
        (df['cum_amt'] == 0) | (np.sign(df['prev_cum_amt']) != np.sign(df['cum_amt'])),

        # Position reduced (same sign, smaller absolute value)
        (np.sign(df['prev_cum_amt']) == np.sign(df['cum_amt'])) & (np.abs(df['cum_amt']) < np.abs(df['prev_cum_amt']))
    ]

    rpnl_total_choices = [
        # Only bucket rpnl
        df['rpnl_usd'],

        # Closed/flipped: realize entire prev position
        df['rpnl_usd'] + df['prev_cum_amt'] * (market_px_usd - avg_cost),

        # Reduced: realize the reduction
        df['rpnl_usd'] + (df['prev_cum_amt'] - df['cum_amt']) * (market_px_usd - avg_cost)
    ]

    df['rpnl_usd_total'] = np.select(rpnl_total_conditions, rpnl_total_choices, default=df['rpnl_usd']).astype(float)

    # Cumulative volume and realized PnL
    df['cum_vol_usd'] = df.groupby('instrument')['vol_usd'].cumsum()
    df['cum_rpnl_usd'] = df.groupby('instrument')['rpnl_usd_total'].cumsum()

    # Unrealized PnL: current position valued at market price minus cost basis
    avg_cost_current = df['cum_cost_usd'] / df['cum_amt']

    upnl_usd_conditions = [
        df['cum_amt'] == 0,
        df['cum_amt'] > 0,
        df['cum_amt'] < 0
    ]

    upnl_usd_choices = [
        0,
        df['cum_amt'] * (df['px_bid_0_usd'] - avg_cost_current),
        df['cum_amt'] * (df['px_ask_0_usd'] - avg_cost_current)
    ]

    df['upnl_usd'] = np.select(upnl_usd_conditions, upnl_usd_choices, default=0.0).astype(float)

    # Unrealized PnL in base leg
    upnl_base_conditions = [
        df['cum_amt'] == 0,
        df['cum_amt'] > 0,
        df['cum_amt'] < 0
    ]

    upnl_base_choices = [
        0,
        df['cum_amt'] * (df['px_bid_0_base'] - df['px_base']),
        df['cum_amt'] * (df['px_ask_0_base'] - df['px_base'])
    ]

    df['upnl_base'] = np.select(upnl_base_conditions, upnl_base_choices, default=0.0).astype(float)

    # Unrealized PnL in quote leg
    upnl_quote_conditions = [
        df['cum_amt'] == 0,
        df['cum_amt'] > 0,
        df['cum_amt'] < 0
    ]

    upnl_quote_choices = [
        0,
        df['cum_amt'] * (df['px_bid_0_quote'] - df['px_quote']),
        df['cum_amt'] * (df['px_ask_0_quote'] - df['px_quote'])
    ]

    df['upnl_quote'] = np.select(upnl_quote_conditions, upnl_quote_choices, default=0.0).astype(float)

    # Total PnL in USD
    df['tpnl_usd'] = df['cum_rpnl_usd'] + df['upnl_usd']

    # Total PnL in quote leg: realized (native) + unrealized in quote
    df['tpnl_quote'] = df['rpnl'] + df['upnl_quote']

    return df

def _update(date_str: str):
    """Process data for a given date and push to QuestDB via ILP."""
    print(f"Updating mart for {date_str}")

    # Process the data
    df = _process(date_str)

    # Select only the columns we want to insert
    key_cols = ['ts', 'instrument', 'instrument_base', 'instrument_quote',
                'amt_filled', 'px', 'px_quote', 'px_base', 'vol_usd', 'num_deals',
                'cum_amt', 'cum_cost_usd', 'rpnl_usd_total', 'cum_rpnl_usd',
                'upnl_usd', 'upnl_base', 'upnl_quote', 'tpnl_usd', 'tpnl_quote']

    df_to_insert = df[key_cols].copy()

    print(f"Inserting {len(df_to_insert)} rows to {MART_TABLE}")

    # Connect to QuestDB via ILP
    try:
        # Format: tcp::addr=host:port
        conf = f'tcp::addr={QUESTDB_HOST}:9009;'
        with Sender.from_conf(conf) as sender:
            for _, row in df_to_insert.iterrows():
                # Convert timestamp to nanoseconds
                ts_nanos = TimestampNanos(int(pd.Timestamp(row['ts']).value))

                # Build the row with symbols and numeric columns

                sender.row(
                    MART_TABLE,
                    symbols={
                        'instrument': str(row['instrument']),
                        'instrument_base': str(row['instrument_base']) if pd.notna(row['instrument_base']) else None,
                        'instrument_quote': str(row['instrument_quote']) if pd.notna(row['instrument_quote']) else None
                    },
                    columns={
                        'amt_filled': float(row['amt_filled']) if pd.notna(row['amt_filled']) else 0.0,
                        'px': float(row['px']) if pd.notna(row['px']) else 0.0,
                        'px_quote': float(row['px_quote']) if pd.notna(row['px_quote']) else 0.0,
                        'px_base': float(row['px_base']) if pd.notna(row['px_base']) else 0.0,
                        'vol_usd': float(row['vol_usd']) if pd.notna(row['vol_usd']) else 0.0,
                        'num_deals': int(row['num_deals']) if pd.notna(row['num_deals']) else 0,
                        'cum_amt': float(row['cum_amt']) if pd.notna(row['cum_amt']) else 0.0,
                        'cum_cost_usd': float(row['cum_cost_usd']) if pd.notna(row['cum_cost_usd']) else 0.0,
                        'rpnl_usd_total': float(row['rpnl_usd_total']) if pd.notna(row['rpnl_usd_total']) else 0.0,
                        'cum_rpnl_usd': float(row['cum_rpnl_usd']) if pd.notna(row['cum_rpnl_usd']) else 0.0,
                        'upnl_usd': float(row['upnl_usd']) if pd.notna(row['upnl_usd']) else 0.0,
                        'upnl_base': float(row['upnl_base']) if pd.notna(row['upnl_base']) else 0.0,
                        'upnl_quote': float(row['upnl_quote']) if pd.notna(row['upnl_quote']) else 0.0,
                        'tpnl_usd': float(row['tpnl_usd']) if pd.notna(row['tpnl_usd']) else 0.0,
                        'tpnl_quote': float(row['tpnl_quote']) if pd.notna(row['tpnl_quote']) else 0.0
                    },
                    at=ts_nanos
                )

            # Flush to ensure all data is sent
            sender.flush()

        print(f"Successfully inserted {len(df_to_insert)} rows for {date_str}")

    except IngressError as e:
        print(f"Error inserting data via ILP: {e}")
        raise

if __name__ == "__main__":
    for date_str in [
        "2025-10-20", "2025-10-21", "2025-10-22",
        "2025-10-23", "2025-10-24", "2025-10-25",
        "2025-10-26", "2025-10-27", "2025-10-28",
        "2025-10-29", "2025-10-30"
    ]:
        _update(date_str)

    # df = _process("2025-10-26")
    # print(df.columns)
    # key_cols = ['ts', 'instrument', 'instrument_base', 'instrument_quote',
    #             'amt_filled', 'px', 'px_quote', 'px_base', 'vol_usd', 'num_deals',
    #             'cum_amt', 'cum_cost_usd', 'rpnl_usd_total', 'cum_rpnl_usd',
    #             'upnl_usd', 'upnl_base', 'upnl_quote', 'tpnl_usd','tpnl_quote']
    # print(df[df.px.notna()][key_cols].head(40))

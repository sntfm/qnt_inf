import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from questdb.ingress import Sender, IngressError, TimestampNanos

# Configure pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 100)

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


def _get_prev_cumsum(engine, date_str: str):
    """Fetch the last cumulative values before the target date for each instrument."""
    date_start = f"{date_str}T00:00:00.000000Z"

    query = f"""
    WITH ranked AS (
        SELECT
            instrument,
            cum_amt,
            cum_cost_usd,
            cum_cost_base,
            cum_cost_quote,
            cum_cost_native,
            cum_quote_amt,
            cum_vol_usd,
            cum_rpnl_usd,
            ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY ts DESC) as rn
        FROM {MART_TABLE}
        WHERE ts < '{date_start}'
    )
    SELECT
        instrument,
        cum_amt,
        cum_cost_usd,
        cum_cost_base,
        cum_cost_quote,
        cum_cost_native,
        cum_quote_amt,
        cum_vol_usd,
        cum_rpnl_usd
    FROM ranked
    WHERE rn = 1
    """

    try:
        prev_df = pd.read_sql(query, engine)
        if len(prev_df) > 0:
            print(f"Loaded previous cumsum values for {len(prev_df)} instruments")
            return prev_df.set_index('instrument')
        else:
            print("No previous data found, starting from zero")
            return pd.DataFrame()
    except Exception as e:
        print(f"Could not fetch previous data: {e}. Starting from zero.")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Helper functions for price conversions
# ---------------------------------------------------------------------------

def _forward_fill_by_instrument(df, series):
    """Forward fill a series grouping by instrument."""
    return series.groupby(df['instrument']).ffill()


def _convert_to_usd(df, native_values, usd_bid, usd_ask, inv_flag, value_type='positive'):
    """
    Convert native currency values to USD.

    value_type: 'positive' uses bid for positive values and ask for negative
                'mid' uses mid price
    """
    has_usd = df['instrument_usd'].notna()
    result = native_values.copy()

    if value_type == 'mid':
        usd_mid = (usd_bid + usd_ask) / 2
        result[has_usd & ~inv_flag] = native_values * usd_mid
        result[has_usd & inv_flag] = native_values / usd_mid
    else:
        positive = native_values > 0
        negative = native_values < 0
        result[has_usd & ~inv_flag & positive] = native_values * usd_bid
        result[has_usd & ~inv_flag & negative] = native_values * usd_ask
        result[has_usd & inv_flag & positive] = native_values / usd_bid
        result[has_usd & inv_flag & negative] = native_values / usd_ask

    return result.astype(float)


def _get_position_conditions(amounts):
    """Get boolean masks for position states (long/short/flat)."""
    return {
        'long': amounts > 0,
        'short': amounts < 0,
        'flat': amounts.isna() | (np.abs(amounts) < 1e-10)
    }


def _calculate_realized_pnl(df, amt_col, cost_col, market_px_usd, rpnl_base):
    """
    Calculate realized PnL from position changes (reductions/flips).
    Reusable for both instrument and quote legs.
    """
    prev_amt = df.groupby('instrument')[amt_col].shift(1)
    prev_cost = df.groupby('instrument')[cost_col].shift(1)

    avg_cost = prev_cost / prev_amt

    prev = _get_position_conditions(prev_amt)
    curr = _get_position_conditions(df[amt_col])

    # Position closed or flipped
    closed_or_flipped = curr['flat'] | ((prev['long'] & curr['short']) | (prev['short'] & curr['long']))

    # Position reduced (same sign, smaller absolute value)
    reduced = ((prev['long'] & curr['long']) | (prev['short'] & curr['short'])) & \
              (np.abs(df[amt_col]) < np.abs(prev_amt))

    # Compute realized PnL
    rpnl_total = rpnl_base.copy()

    # Closed/flipped: realize entire previous position
    rpnl_total[closed_or_flipped & ~prev['flat']] = (
        rpnl_base + prev_amt * (market_px_usd - avg_cost)
    )[closed_or_flipped & ~prev['flat']]

    # Reduced: realize the reduction
    rpnl_total[reduced] = (
        rpnl_base + (prev_amt - df[amt_col]) * (market_px_usd - avg_cost)
    )[reduced]

    return rpnl_total.astype(float)


def _initialize_cumulative_columns(df, prev_cumsum, columns):
    """Initialize cumulative columns with previous day's values."""
    for col in columns:
        prev_col = f'prev_day_{col}'
        if not prev_cumsum.empty and col in prev_cumsum.columns:
            df[prev_col] = df['instrument'].map(prev_cumsum[col]).fillna(0)
        else:
            df[prev_col] = 0
    return df


def _compute_cumsum_with_carryover(df, prev_cumsum, flow_columns):
    """
    Compute cumulative sums starting from previous day's values.

    flow_columns: dict mapping cumulative column name to flow column name
                  e.g., {'cum_amt': 'amt_base', 'cum_cost_usd': 'cost_signed_usd'}
    """
    cum_cols = list(flow_columns.keys())
    df = _initialize_cumulative_columns(df, prev_cumsum, cum_cols)

    for cum_col, flow_col in flow_columns.items():
        prev_col = f'prev_day_{cum_col}'
        df[cum_col] = df[prev_col] + df.groupby('instrument')[flow_col].cumsum()

    # Drop temporary columns
    df = df.drop(columns=[f'prev_day_{col}' for col in cum_cols])
    return df


# ---------------------------------------------------------------------------
# Main processing function
# ---------------------------------------------------------------------------

def _process(date_str: str):
    print(f"Processing PnL flow data for {date_str}")

    date_start = f"{date_str}T00:00:00.000000Z"
    date_end = f"{date_str}T23:59:59.999999Z"

    insert_sql = f"""
WITH
-- Load all feed rows for the target day range only
feed_all AS (
    SELECT DISTINCT
        ts,
        instrument,
        bid_px_0,
        ask_px_0
    FROM {SOURCE_FEED_TABLE}
    WHERE ts >= '{date_start}'
      AND ts <= '{date_end}'
),

-- Bucket deals to 1min for the target day
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
        COALESCE(b.amt_buy, 0) - COALESCE(s.amt_sell, 0) AS amt_base,
        LEAST(COALESCE(b.amt_buy, 0), COALESCE(s.amt_sell, 0)) AS amt_base_matched,
        COALESCE(b.cnt_buy, 0) + COALESCE(s.cnt_sell, 0) AS num_deals
    FROM deals_buy b
    FULL OUTER JOIN deals_sell s
        ON b.ts_1m = s.ts_1m
       AND b.instrument = s.instrument
),

-- Deduplicated conversion map (in case there are duplicates)
convmap AS (
    SELECT DISTINCT
        instrument,
        is_major,
        instrument_base,
        instrument_quote,
        instrument_usd,
        inst_usd_is_inverted
    FROM {CONVMAP_TABLE}
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
        COALESCE(rd.amt_base, 0) AS amt_base,
        COALESCE(rd.amt_buy, 0) AS amt_buy,
        COALESCE(rd.amt_sell, 0) AS amt_sell,
        COALESCE(rd.amt_base_matched, 0) AS amt_base_matched,
        COALESCE(rd.px_buy, 0) AS px_buy,
        COALESCE(rd.px_sell, 0) AS px_sell,
        COALESCE(rd.num_deals, 0) AS num_deals,
        -- Instrument's own native bid/ask prices
        f.bid_px_0 AS px_bid_0,
        f.ask_px_0 AS px_ask_0,
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
)

SELECT * FROM base;
    """

    engine = _connect()
    prev_cumsum = _get_prev_cumsum(engine, date_str)
    df = pd.read_sql(insert_sql, engine)
    engine.dispose()

    print(f"Retrieved {len(df)} rows for date {date_str}")

    # Sort data by instrument and timestamp
    df = df.sort_values(['instrument', 'ts'])

    # Precompute common masks and variables
    amt = df['amt_base'].fillna(0)
    inv = df['inst_usd_is_inverted']
    has_usd = df['instrument_usd'].notna()
    pos = _get_position_conditions(amt)

    # Forward-fill and prepare price columns
    usd_bid = _forward_fill_by_instrument(df, df['px_bid_0_usd'].replace(0, np.nan))
    usd_ask = _forward_fill_by_instrument(df, df['px_ask_0_usd'].replace(0, np.nan))
    base_bid = _forward_fill_by_instrument(df, df['px_bid_0_base'].replace(0, np.nan))
    base_ask = _forward_fill_by_instrument(df, df['px_ask_0_base'].replace(0, np.nan))
    quote_bid = _forward_fill_by_instrument(df, df['px_bid_0_quote'].replace(0, np.nan))
    quote_ask = _forward_fill_by_instrument(df, df['px_ask_0_quote'].replace(0, np.nan))

    # ---------------------------------------------------------------------------
    # Price calculations
    # ---------------------------------------------------------------------------
    df['px'] = np.where(amt > 0, df['px_buy'], np.where(amt < 0, df['px_sell'], np.nan))

    # PX_BASE - calculate from quote reference prices
    df['px_base'] = np.nan
    df.loc[pos['long'] & ~inv, 'px_base'] = df['px_buy'] / quote_bid
    df.loc[pos['long'] & inv, 'px_base'] = df['px_buy'] * quote_bid
    df.loc[pos['short'] & ~inv, 'px_base'] = df['px_sell'] / quote_ask
    df.loc[pos['short'] & inv, 'px_base'] = df['px_sell'] * quote_ask

    # PX_QUOTE
    df['px_quote'] = np.nan
    df.loc[~inv, 'px_quote'] = df['px'] / df['px_base']
    df.loc[inv, 'px_quote'] = df['px_base'] / df['px']

    # ---------------------------------------------------------------------------
    # Intrabucket realized PnL
    # ---------------------------------------------------------------------------
    mask = df['px_sell'].notna() & df['px_buy'].notna()
    df['rpnl_intra'] = 0.0
    df.loc[mask, 'rpnl_intra'] = ((df['px_sell'] - df['px_buy']) * df['amt_base_matched'])

    df['rpnl_usd'] = _convert_to_usd(df, df['rpnl_intra'], usd_bid, usd_ask, inv)

    # ---------------------------------------------------------------------------
    # Volume USD
    # ---------------------------------------------------------------------------
    usd_mid = (usd_bid + usd_ask) / 2
    native_vol = df['amt_base'] * df['px']
    df['vol_usd'] = _convert_to_usd(df, native_vol, usd_mid, usd_mid, inv, value_type='mid')

    # ---------------------------------------------------------------------------
    # Cost calculations
    # ---------------------------------------------------------------------------
    mask_buy = df['amt_buy'] > 0
    mask_sell = df['amt_sell'] > 0

    # Convert buy/sell prices to USD
    px_buy_usd = df['px_buy'].copy()
    px_sell_usd = df['px_sell'].copy()
    px_buy_usd[has_usd & inv] = df['px_buy'][has_usd & inv] / usd_bid[has_usd & inv]
    px_buy_usd[has_usd & ~inv] = df['px_buy'][has_usd & ~inv] * usd_bid[has_usd & ~inv]
    px_sell_usd[has_usd & inv] = df['px_sell'][has_usd & inv] / usd_ask[has_usd & inv]
    px_sell_usd[has_usd & ~inv] = df['px_sell'][has_usd & ~inv] * usd_ask[has_usd & ~inv]

    # Signed costs
    df['cost_signed_usd'] = (df['amt_buy'] * px_buy_usd - df['amt_sell'] * px_sell_usd).fillna(0.0).astype(float)

    df['cost_signed_base'] = 0.0
    df.loc[mask_buy, 'cost_signed_base'] = df.loc[mask_buy, 'amt_buy'] * base_bid[mask_buy]
    df.loc[mask_sell, 'cost_signed_base'] -= df.loc[mask_sell, 'amt_sell'] * base_ask[mask_sell]
    df['cost_signed_base'] = df['cost_signed_base'].fillna(0.0).astype(float)

    df['cost_signed_quote'] = 0.0
    df.loc[mask_buy, 'cost_signed_quote'] = -(df.loc[mask_buy, 'amt_buy'] * df.loc[mask_buy, 'px_buy']) * quote_bid[mask_buy]
    df.loc[mask_sell, 'cost_signed_quote'] += (df.loc[mask_sell, 'amt_sell'] * df.loc[mask_sell, 'px_sell']) * quote_ask[mask_sell]
    df['cost_signed_quote'] = df['cost_signed_quote'].fillna(0.0).astype(float)

    df['quote_amt_signed'] = 0.0
    df.loc[mask_buy, 'quote_amt_signed'] = -(df['amt_buy'] * df['px_buy'])
    df.loc[mask_sell, 'quote_amt_signed'] = (df['amt_sell'] * df['px_sell'])

    df['cost_signed_native'] = 0.0
    df.loc[mask_buy, 'cost_signed_native'] = df.loc[mask_buy, 'amt_buy'] * df.loc[mask_buy, 'px_buy']
    df.loc[mask_sell, 'cost_signed_native'] -= df.loc[mask_sell, 'amt_sell'] * df.loc[mask_sell, 'px_sell']

    # ---------------------------------------------------------------------------
    # Cumulative calculations
    # ---------------------------------------------------------------------------
    df = _compute_cumsum_with_carryover(df, prev_cumsum, {
        'cum_amt': 'amt_base',
        'cum_cost_usd': 'cost_signed_usd',
        'cum_cost_base': 'cost_signed_base',
        'cum_cost_quote': 'cost_signed_quote',
        'cum_cost_native': 'cost_signed_native',
        'cum_quote_amt': 'quote_amt_signed'
    })

    # ---------------------------------------------------------------------------
    # Instrument bid/ask in USD
    # ---------------------------------------------------------------------------
    df['instrument_bid_usd'] = df['px_bid_0'].copy()
    df['instrument_ask_usd'] = df['px_ask_0'].copy()

    usd_mid = (usd_bid + usd_ask) / 2
    df.loc[has_usd & inv, 'instrument_bid_usd'] = df['px_bid_0'] / usd_mid
    df.loc[has_usd & inv, 'instrument_ask_usd'] = df['px_ask_0'] / usd_mid
    df.loc[has_usd & ~inv, 'instrument_bid_usd'] = df['px_bid_0'] * usd_mid
    df.loc[has_usd & ~inv, 'instrument_ask_usd'] = df['px_ask_0'] * usd_mid

    df['instrument_bid_usd'] = _forward_fill_by_instrument(df, df['instrument_bid_usd'])
    df['instrument_ask_usd'] = _forward_fill_by_instrument(df, df['instrument_ask_usd'])

    # ---------------------------------------------------------------------------
    # Realized PnL (instrument leg)
    # ---------------------------------------------------------------------------
    prev_cum_amt = df.groupby('instrument')['cum_amt'].shift(1)
    prev_long = prev_cum_amt > 0
    market_px_usd = np.where(prev_long, df['instrument_bid_usd'], df['instrument_ask_usd'])

    df['rpnl_usd_total'] = _calculate_realized_pnl(
        df, 'cum_amt', 'cum_cost_usd', market_px_usd, df['rpnl_usd']
    )

    # ---------------------------------------------------------------------------
    # Cumulative volume and realized PnL
    # ---------------------------------------------------------------------------
    df = _compute_cumsum_with_carryover(df, prev_cumsum, {
        'cum_vol_usd': 'vol_usd',
        'cum_rpnl_usd': 'rpnl_usd_total'
    })

    # ---------------------------------------------------------------------------
    # Unrealized PnL (instrument leg)
    # ---------------------------------------------------------------------------
    curr_pos = _get_position_conditions(df['cum_amt'])

    avg_cost_native = np.where(
        np.abs(df['cum_amt']) < 1e-10,
        0,
        df['cum_cost_native'] / df['cum_amt']
    )

    df['upnl_native'] = 0.0
    df.loc[curr_pos['long'], 'upnl_native'] = df['cum_amt'] * (df['px_bid_0'] - avg_cost_native)
    df.loc[curr_pos['short'], 'upnl_native'] = df['cum_amt'] * (df['px_ask_0'] - avg_cost_native)

    df['upnl_usd'] = _convert_to_usd(df, df['upnl_native'], usd_bid, usd_ask, inv)

    # ---------------------------------------------------------------------------
    # Unrealized PnL (base leg)
    # ---------------------------------------------------------------------------
    base_market_value_usd = np.where(
        curr_pos['long'],
        df['cum_amt'] * base_bid,
        np.where(curr_pos['short'], df['cum_amt'] * base_ask, 0)
    )
    df['upnl_base'] = (base_market_value_usd - df['cum_cost_base']).astype(float)

    # ---------------------------------------------------------------------------
    # Realized PnL (quote leg)
    # ---------------------------------------------------------------------------
    # Convert intraday rpnl_intra to USD
    rpnl_intra_usd = df['rpnl_intra'].copy()
    rpnl_pos = df['rpnl_intra'] > 0
    rpnl_neg = df['rpnl_intra'] < 0
    rpnl_intra_usd[rpnl_pos] = df['rpnl_intra'][rpnl_pos] * quote_bid[rpnl_pos]
    rpnl_intra_usd[rpnl_neg] = df['rpnl_intra'][rpnl_neg] * quote_ask[rpnl_neg]
    rpnl_intra_usd = rpnl_intra_usd.fillna(0.0)

    # Calculate quote market price
    prev_cum_quote = df.groupby('instrument')['cum_quote_amt'].shift(1)
    prev_quote_long = prev_cum_quote > 0
    quote_market_px_usd = np.where(prev_quote_long, quote_bid, quote_ask)

    df['rpnl_quote_total'] = _calculate_realized_pnl(
        df, 'cum_quote_amt', 'cum_cost_quote', quote_market_px_usd, rpnl_intra_usd
    )

    df['cum_rpnl_quote'] = df.groupby('instrument')['rpnl_quote_total'].cumsum().astype(float)

    # ---------------------------------------------------------------------------
    # Total and unrealized PnL (quote leg)
    # ---------------------------------------------------------------------------
    quote_market_value_usd = np.where(
        df['cum_quote_amt'] < 0,
        df['cum_quote_amt'] * quote_ask,
        np.where(df['cum_quote_amt'] > 0, df['cum_quote_amt'] * quote_bid, 0)
    )

    df['tpnl_quote'] = (quote_market_value_usd - df['cum_cost_quote']).astype(float)
    df['upnl_quote'] = (df['tpnl_quote'] - df['cum_rpnl_quote']).astype(float)

    # ---------------------------------------------------------------------------
    # Total PnL
    # ---------------------------------------------------------------------------
    df['tpnl_usd'] = df['cum_rpnl_usd'] + df['upnl_usd']

    return df


def _update(date_str: str):
    """Process data for a given date and push to QuestDB via ILP."""
    print(f"Updating mart for {date_str}")

    df = _process(date_str)

    # Select and prepare columns for insertion
    key_cols = ['ts', 'instrument', 'instrument_base', 'instrument_quote',
                'amt_base', 'px', 'px_quote', 'px_base', 'vol_usd', 'num_deals',
                'cum_amt', 'cum_cost_usd', 'cum_cost_base', 'cum_cost_quote', 'cum_cost_native', 'cum_quote_amt', 'cum_vol_usd',
                'rpnl_usd_total', 'cum_rpnl_usd', 'cum_rpnl_quote',
                'upnl_usd', 'upnl_base', 'upnl_quote', 'tpnl_usd', 'tpnl_quote']

    df_to_insert = df[key_cols].copy()

    # Prepare data types
    symbol_cols = ['instrument', 'instrument_base', 'instrument_quote']
    float_cols = ['amt_base', 'px', 'px_quote', 'px_base', 'vol_usd',
                  'cum_amt', 'cum_cost_usd', 'cum_cost_base', 'cum_cost_quote', 'cum_cost_native', 'cum_quote_amt', 'cum_vol_usd',
                  'rpnl_usd_total', 'cum_rpnl_usd', 'cum_rpnl_quote',
                  'upnl_usd', 'upnl_base', 'upnl_quote', 'tpnl_usd', 'tpnl_quote']
    int_cols = ['num_deals']

    df_to_insert[float_cols] = df_to_insert[float_cols].fillna(0.0).astype(float)
    df_to_insert[int_cols] = df_to_insert[int_cols].fillna(0).astype(int)
    df_to_insert[symbol_cols] = df_to_insert[symbol_cols].astype(str)

    print(f"Inserting {len(df_to_insert)} rows to {MART_TABLE}")

    # Insert via ILP
    try:
        conf = f'tcp::addr={QUESTDB_HOST}:9009;'
        with Sender.from_conf(conf) as sender:
            for _, row in df_to_insert.iterrows():
                ts_nanos = TimestampNanos(int(pd.Timestamp(row['ts']).value))

                sender.row(
                    MART_TABLE,
                    symbols={
                        'instrument': row['instrument'],
                        'instrument_base': row['instrument_base'] if row['instrument_base'] != 'nan' else None,
                        'instrument_quote': row['instrument_quote'] if row['instrument_quote'] != 'nan' else None
                    },
                    columns={
                        'amt_base': row['amt_base'],
                        'px': row['px'],
                        'px_quote': row['px_quote'],
                        'px_base': row['px_base'],
                        'vol_usd': row['vol_usd'],
                        'num_deals': row['num_deals'],
                        'cum_amt': row['cum_amt'],
                        'cum_cost_usd': row['cum_cost_usd'],
                        'cum_cost_base': row['cum_cost_base'],
                        'cum_cost_quote': row['cum_cost_quote'],
                        'cum_cost_native': row['cum_cost_native'],
                        'cum_quote_amt': row['cum_quote_amt'],
                        'cum_vol_usd': row['cum_vol_usd'],
                        'rpnl_usd_total': row['rpnl_usd_total'],
                        'cum_rpnl_usd': row['cum_rpnl_usd'],
                        'cum_rpnl_quote': row['cum_rpnl_quote'],
                        'upnl_usd': row['upnl_usd'],
                        'upnl_base': row['upnl_base'],
                        'upnl_quote': row['upnl_quote'],
                        'tpnl_usd': row['tpnl_usd'],
                        'tpnl_quote': row['tpnl_quote']
                    },
                    at=ts_nanos
                )

            sender.flush()

        print(f"Successfully inserted {len(df_to_insert)} rows for {date_str}")

    except IngressError as e:
        print(f"Error inserting data via ILP: {e}")
        raise


if __name__ == "__main__":
    # for date_str in [
    # "2025-10-20",
    # "2025-10-21", "2025-10-22", "2025-10-23",
    # "2025-10-24", "2025-10-25", "2025-10-26",
    # "2025-10-27", "2025-10-28", "2025-10-29",
    # "2025-10-30"]:
    #     _update(date_str)

    df = _process("2025-10-20")
    cols = ['ts', 'instrument', 'amt_base', 'amt_buy',
       'amt_sell', 'amt_base_matched', 'px_buy', 'px_sell', 'num_deals',
       'px_bid_0', 'px_ask_0', 'px_bid_0_base', 'px_ask_0_base',
       'px_bid_0_quote', 'px_ask_0_quote', 'px_bid_0_usd', 'px_ask_0_usd',
       'px', 'px_base', 'px_quote', 'rpnl_intra', 'rpnl_usd', 'vol_usd',
       'cost_signed_usd', 'cost_signed_base', 'cost_signed_quote',
       'quote_amt_signed', 'cost_signed_native', 'cum_amt', 'cum_cost_usd',
       'cum_cost_base', 'cum_cost_quote', 'cum_cost_native', 'cum_quote_amt',
       'instrument_bid_usd', 'instrument_ask_usd', 'rpnl_usd_total',
       'cum_vol_usd', 'cum_rpnl_usd', 'upnl_native', 'upnl_usd', 'upnl_base',
       'rpnl_quote_total', 'cum_rpnl_quote', 'tpnl_quote', 'upnl_quote', 'tpnl_usd']
    print(df[cols].head(10))
    print(df.columns)
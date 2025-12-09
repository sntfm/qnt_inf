import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from questdb.ingress import Sender, IngressError, TimestampNanos

# Configure pandas display options
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Auto-detect width
pd.set_option('display.max_colwidth', None)  # No truncation of column values
pd.set_option('display.expand_frame_repr', False)  # Prevent wrapping
pd.set_option('display.max_rows', 100)  # Increase if needed

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
        COALESCE(b.amt_buy, 0) - COALESCE(s.amt_sell, 0) AS amt_filled,
        LEAST(COALESCE(b.amt_buy, 0), COALESCE(s.amt_sell, 0)) AS amt_matched,
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
        COALESCE(rd.amt_filled, 0) AS amt_filled,
        COALESCE(rd.amt_buy, 0) AS amt_buy,
        COALESCE(rd.amt_sell, 0) AS amt_sell,
        COALESCE(rd.amt_matched, 0) AS amt_matched,
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
    df = pd.read_sql(insert_sql, engine)
    engine.dispose()

    print(f"Retrieved {len(df)} rows for date {date_str}")

    # Initialize columns
    df['px_base'] = np.nan
    df['px_quote'] = np.nan
    df['rpnl_intra'] = 0.0
    df['rpnl_intra_usd'] = 0.0
    df['vol_usd'] = 0.0

# Precompute 
    amt = df['amt_filled'].fillna(0)
    long = amt > 0
    short = amt < 0
    inv = df['inst_usd_is_inverted']
    has_usd   = df['instrument_usd'].notna()
    
# Column PX
    df['px'] = np.where(amt > 0, df['px_buy'], np.where(amt < 0, df['px_sell'], np.nan))

# Column PX_BASE - calculate from quote reference prices
    ask_q = df['px_ask_0_quote'].replace(0, np.nan)
    bid_q = df['px_bid_0_quote'].replace(0, np.nan)

    df.loc[long & ~inv, 'px_base']  = df['px_buy'] / bid_q
    df.loc[long &  inv, 'px_base']  = df['px_buy'] * bid_q
    df.loc[short & ~inv, 'px_base'] = df['px_sell'] / ask_q
    df.loc[short &  inv, 'px_base'] = df['px_sell'] * ask_q

# Column PX_QUOTE - derive from reference px
    df.loc[~inv, 'px_quote'] = df['px'] / df['px_base']
    df.loc[inv, 'px_quote']  = df['px_base'] / df['px']

# Column RPNL_INTRA
    mask = df['px_sell'].notna() & df['px_buy'].notna()
    df.loc[mask, 'rpnl_intra'] = ((df['px_sell'] - df['px_buy']) * df['amt_matched'])

# Column RPNL_INTRA_USD
    usd_bid = df['px_bid_0_usd'].replace(0, np.nan)
    usd_ask = df['px_ask_0_usd'].replace(0, np.nan)

    usd_bid = usd_bid.groupby(df['instrument']).ffill()
    usd_ask = usd_ask.groupby(df['instrument']).ffill()

    df['rpnl_usd'] = df['rpnl_intra'].copy()

    positive = df['rpnl_intra'] > 0
    negative = df['rpnl_intra'] < 0

    df.loc[has_usd & ~inv & positive, 'rpnl_usd'] = df['rpnl_intra'] * usd_bid
    df.loc[has_usd & ~inv & negative, 'rpnl_usd'] = df['rpnl_intra'] * usd_ask
    df.loc[has_usd &  inv & positive, 'rpnl_usd'] = df['rpnl_intra'] / usd_bid
    df.loc[has_usd &  inv & negative, 'rpnl_usd'] = df['rpnl_intra'] / usd_ask

    df['rpnl_usd'] = df['rpnl_usd'].astype(float)

# Column VOL_USD
    usd_mid = ((df['px_ask_0_usd'].fillna(0) + df['px_bid_0_usd'].fillna(0)) / 2)
    usd_mid = usd_mid.replace(0, np.nan)

    usd_mid = usd_mid.groupby(df['instrument']).ffill()

    native_vol = df['amt_filled'] * df['px']
    df['vol_usd'] = native_vol  # default: already USD when instrument_usd is NaN

    df.loc[has_usd & ~inv, 'vol_usd'] = native_vol * usd_mid
    df.loc[has_usd &  inv, 'vol_usd'] = native_vol / usd_mid

    df['vol_usd'] = df['vol_usd'].astype(float)

# Column COST_SIGNED_USD
    px_buy_usd = df['px_buy'].copy()
    px_sell_usd = df['px_sell'].copy()

    # Convert buy prices
    px_buy_usd[has_usd & inv] = df['px_buy'][has_usd & inv] / usd_bid[has_usd & inv]
    px_buy_usd[has_usd & ~inv] = df['px_buy'][has_usd & ~inv] * usd_bid[has_usd & ~inv]

    # Convert sell prices
    px_sell_usd[has_usd & inv] = df['px_sell'][has_usd & inv] / usd_ask[has_usd & inv]
    px_sell_usd[has_usd & ~inv] = df['px_sell'][has_usd & ~inv] * usd_ask[has_usd & ~inv]

    # Compute signed cost
    df['cost_signed_usd'] = df['amt_buy'] * px_buy_usd - df['amt_sell'] * px_sell_usd

    # Default zero where both buy and sell are zero
    df['cost_signed_usd'] = df['cost_signed_usd'].fillna(0.0).astype(float)

# Column COST_SIGNED_BASE - track USD-equivalent value of base leg positions
    # We need to track the USD cost of base currency flows AT THE TIME OF TRADE
    base_bid = df['px_bid_0_base'].replace(0, np.nan)
    base_ask = df['px_ask_0_base'].replace(0, np.nan)

    # Forward-fill missing prices - these are used for ENTRY valuation
    base_bid_filled = base_bid.groupby(df['instrument']).ffill()
    base_ask_filled = base_ask.groupby(df['instrument']).ffill()

    mask_buy = df['amt_buy'] > 0
    mask_sell = df['amt_sell'] > 0

    # When we BUY amt_buy units of ETH/EUR:
    # - USD cost of this base AT ENTRY: amt_buy * base_bid_usd (at time of trade)
    # Use the actual base_bid at time of trade (not forward-filled)
    df['cost_signed_base'] = 0.0
    df.loc[mask_buy, 'cost_signed_base'] = df.loc[mask_buy, 'amt_buy'] * base_bid_filled[mask_buy]
    df.loc[mask_sell, 'cost_signed_base'] -= df.loc[mask_sell, 'amt_sell'] * base_ask_filled[mask_sell]

    df['cost_signed_base'] = df['cost_signed_base'].fillna(0.0).astype(float)

# Column COST_SIGNED_QUOTE - track USD-equivalent value of quote leg positions
    # Quote leg: when we buy/sell the instrument, we spend/receive quote currency
    # We need to track the USD cost of these quote currency flows AT THE TIME OF TRADE

    quote_bid = df['px_bid_0_quote'].replace(0, np.nan)
    quote_ask = df['px_ask_0_quote'].replace(0, np.nan)

    # Forward-fill missing prices - these are used for ENTRY valuation
    quote_bid_filled = quote_bid.groupby(df['instrument']).ffill()
    quote_ask_filled = quote_ask.groupby(df['instrument']).ffill()

    # When we BUY amt_buy units of ETH/EUR at px_buy EUR per ETH:
    # - We spend (amt_buy * px_buy) EUR (quote currency)
    # - USD cost of this quote AT ENTRY: (amt_buy * px_buy) * quote_bid_usd (at time of trade)
    # - This is NEGATIVE because we're spending (reducing) our quote holdings
    df['cost_signed_quote'] = 0.0
    df.loc[mask_buy, 'cost_signed_quote'] = -(df.loc[mask_buy, 'amt_buy'] * df.loc[mask_buy, 'px_buy']) * quote_bid_filled[mask_buy]
    df.loc[mask_sell, 'cost_signed_quote'] += (df.loc[mask_sell, 'amt_sell'] * df.loc[mask_sell, 'px_sell']) * quote_ask_filled[mask_sell]

    df['cost_signed_quote'] = df['cost_signed_quote'].fillna(0.0).astype(float)

#### Track cumulative quote amount in native currency
    df['quote_amt_signed'] = 0.0
    df.loc[mask_buy, 'quote_amt_signed'] = -(df['amt_buy'] * df['px_buy'])
    df.loc[mask_sell, 'quote_amt_signed'] = (df['amt_sell'] * df['px_sell'])

#### Cumulative calculations: running sums per instrument
    df = df.sort_values(['instrument', 'ts'])
    df['cum_amt'] = df.groupby('instrument')['amt_filled'].cumsum()
    df['cum_cost_usd'] = df.groupby('instrument')['cost_signed_usd'].cumsum()
    df['cum_cost_base'] = df.groupby('instrument')['cost_signed_base'].cumsum()
    df['cum_cost_quote'] = df.groupby('instrument')['cost_signed_quote'].cumsum()
    df['cum_quote_amt'] = df.groupby('instrument')['quote_amt_signed'].cumsum()

    # Lagged values: previous cumulative snapshot
    df['prev_cum_amt'] = df.groupby('instrument')['cum_amt'].shift(1)
    df['prev_cum_cost_usd'] = df.groupby('instrument')['cum_cost_usd'].shift(1)

    # Average cost from previous position
    avg_cost = df['prev_cum_cost_usd'] / df['prev_cum_amt']

# Instrument bid/ask in USD
    df['instrument_bid_usd'] = np.nan
    df['instrument_ask_usd'] = np.nan

    no_usd = df['instrument_usd'].isna()

    df.loc[no_usd, 'instrument_bid_usd'] = df['px_bid_0']
    df.loc[no_usd, 'instrument_ask_usd'] = df['px_ask_0']
    df.loc[has_usd & inv, 'instrument_bid_usd'] = df['px_bid_0'] / usd_mid
    df.loc[has_usd & inv, 'instrument_ask_usd'] = df['px_ask_0'] / usd_mid
    df.loc[has_usd & ~inv, 'instrument_bid_usd'] = df['px_bid_0'] * usd_mid
    df.loc[has_usd & ~inv, 'instrument_ask_usd'] = df['px_ask_0'] * usd_mid

    # Forward-fill NaN values per instrument
    df['instrument_bid_usd'] = df.groupby('instrument')['instrument_bid_usd'].ffill()
    df['instrument_ask_usd'] = df.groupby('instrument')['instrument_ask_usd'].ffill()

    instrument_bid_usd = df['instrument_bid_usd']
    instrument_ask_usd = df['instrument_ask_usd']

# Column RPNL_USD_TOTAL - realized PnL from position reductions/flips
    df['rpnl_usd_total'] = df['rpnl_usd'].copy()

    # Precompute position conditions
    prev_long = df['prev_cum_amt'] > 0
    prev_short = df['prev_cum_amt'] < 0
    prev_flat = df['prev_cum_amt'].isna() | (df['prev_cum_amt'] == 0)

    curr_long = df['cum_amt'] > 0
    curr_short = df['cum_amt'] < 0
    curr_flat = df['cum_amt'] == 0

    # Position closed or flipped
    closed_or_flipped = curr_flat | ((prev_long & curr_short) | (prev_short & curr_long))

    # Position reduced (same sign, smaller absolute value)
    reduced = ((prev_long & curr_long) | (prev_short & curr_short)) & (np.abs(df['cum_amt']) < np.abs(df['prev_cum_amt']))

    # Market price to use for closing positions
    market_px_usd = np.where(prev_long, instrument_bid_usd, instrument_ask_usd)

    # Closed/flipped: realize entire previous position
    df.loc[closed_or_flipped & ~prev_flat, 'rpnl_usd_total'] = (
        df['rpnl_usd'] + df['prev_cum_amt'] * (market_px_usd - avg_cost)
    )

    # Reduced: realize the reduction
    df.loc[reduced, 'rpnl_usd_total'] = (
        df['rpnl_usd'] + (df['prev_cum_amt'] - df['cum_amt']) * (market_px_usd - avg_cost)
    )

    df['rpnl_usd_total'] = df['rpnl_usd_total'].astype(float)

# Column CUM_VOL_USD, CUM_RPNL_USD
    df['cum_vol_usd'] = df.groupby('instrument')['vol_usd'].cumsum()
    df['cum_rpnl_usd'] = df.groupby('instrument')['rpnl_usd_total'].cumsum()

# Column UPNL_USD - unrealized PnL in USD
    # Average cost of current position (handle division by zero)
    avg_cost_current = np.where(
        np.abs(df['cum_amt']) < 1e-10,
        0,
        df['cum_cost_usd'] / df['cum_amt']
    )

    df['upnl_usd'] = 0.0
    df.loc[curr_long, 'upnl_usd'] = df['cum_amt'] * (instrument_bid_usd - avg_cost_current)
    df.loc[curr_short, 'upnl_usd'] = df['cum_amt'] * (instrument_ask_usd - avg_cost_current)
    df['upnl_usd'] = df['upnl_usd'].astype(float)

# Column UPNL_BASE - unrealized PnL in base leg (USD terms)
    # Get CURRENT market prices for base (not entry prices)
    base_bid_current = df['px_bid_0_base'].replace(0, np.nan).groupby(df['instrument']).ffill()
    base_ask_current = df['px_ask_0_base'].replace(0, np.nan).groupby(df['instrument']).ffill()

    # For long: value at bid (what we could sell for)
    # For short: value at ask (what we'd need to buy to cover)
    base_market_value_usd = np.where(
        curr_long,
        df['cum_amt'] * base_bid_current,
        np.where(
            curr_short,
            df['cum_amt'] * base_ask_current,
            0
        )
    )

    df['upnl_base'] = base_market_value_usd - df['cum_cost_base']
    df['upnl_base'] = df['upnl_base'].astype(float)

# Column UPNL_QUOTE - unrealized PnL in quote leg (USD terms)
    # Get CURRENT market prices for quote (not entry prices)
    quote_bid_current = df['px_bid_0_quote'].replace(0, np.nan).groupby(df['instrument']).ffill()
    quote_ask_current = df['px_ask_0_quote'].replace(0, np.nan).groupby(df['instrument']).ffill()

    # The quote amount is in cum_quote_amt (in native quote currency)
    # Current value in USD depends on position direction:
    # - If long instrument (short quote): use ask to value what we'd need to pay
    # - If short instrument (long quote): use bid to value what we could sell for
    quote_market_value_usd = np.where(
        curr_long,
        df['cum_quote_amt'] * quote_ask_current,  # negative amount * quote_ask
        np.where(
            curr_short,
            df['cum_quote_amt'] * quote_bid_current,  # positive amount * quote_bid
            0
        )
    )

    df['upnl_quote'] = quote_market_value_usd - df['cum_cost_quote']
    df['upnl_quote'] = df['upnl_quote'].astype(float)

# Column TPNL_USD, TPNL_QUOTE - total PnL
    df['tpnl_usd'] = df['cum_rpnl_usd'] + df['upnl_usd']
    df['tpnl_quote'] = df['rpnl_intra'] + df['upnl_quote']

    return df

def _update(date_str: str):
    """Process data for a given date and push to QuestDB via ILP."""
    print(f"Updating mart for {date_str}")

    # Process the data
    df = _process(date_str)

    # Select only the columns we want to insert
    key_cols = ['ts', 'instrument', 'instrument_base', 'instrument_quote',
                'amt_filled', 'px', 'px_quote', 'px_base', 'vol_usd', 'num_deals',
                'cum_amt', 'cum_cost_usd', 'cum_cost_base', 'cum_cost_quote',
                'rpnl_usd_total', 'cum_rpnl_usd',
                'upnl_usd', 'upnl_base', 'upnl_quote', 'tpnl_usd', 'tpnl_quote']

    df_to_insert = df[key_cols].copy()

    # Prepare data: fill NaNs and ensure correct types
    symbol_cols = ['instrument', 'instrument_base', 'instrument_quote']
    float_cols = ['amt_filled', 'px', 'px_quote', 'px_base', 'vol_usd',
                  'cum_amt', 'cum_cost_usd', 'cum_cost_base', 'cum_cost_quote',
                  'rpnl_usd_total', 'cum_rpnl_usd',
                  'upnl_usd', 'upnl_base', 'upnl_quote', 'tpnl_usd', 'tpnl_quote']
    int_cols = ['num_deals']

    df_to_insert[float_cols] = df_to_insert[float_cols].fillna(0.0).astype(float)
    df_to_insert[int_cols] = df_to_insert[int_cols].fillna(0).astype(int)
    df_to_insert[symbol_cols] = df_to_insert[symbol_cols].astype(str)

    print(f"Inserting {len(df_to_insert)} rows to {MART_TABLE}")

    # Connect to QuestDB via ILP
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
                        'amt_filled': row['amt_filled'],
                        'px': row['px'],
                        'px_quote': row['px_quote'],
                        'px_base': row['px_base'],
                        'vol_usd': row['vol_usd'],
                        'num_deals': row['num_deals'],
                        'cum_amt': row['cum_amt'],
                        'cum_cost_usd': row['cum_cost_usd'],
                        'cum_cost_base': row['cum_cost_base'],
                        'cum_cost_quote': row['cum_cost_quote'],
                        'rpnl_usd_total': row['rpnl_usd_total'],
                        'cum_rpnl_usd': row['cum_rpnl_usd'],
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
    for date_str in ["2025-10-20"]:
    # , "2025-10-21", "2025-10-22",
    #     "2025-10-23", "2025-10-24", "2025-10-25",
    #     "2025-10-26", "2025-10-27", "2025-10-28",
    #     "2025-10-29", "2025-10-30"]:
        _update(date_str)


    df = _process("2025-10-20")
    print(df.columns)
    key_cols = ['ts', 'instrument', 'instrument_base', 'instrument_quote',
                'amt_filled','px_bid_0', 'px_ask_0', 'px','px_base',  'px_quote',
                'vol_usd', 'num_deals', 'cum_amt', 'cum_cost_usd', 'rpnl_usd_total', 'cum_rpnl_usd',
                'upnl_usd', 'upnl_base', 'upnl_quote', 'tpnl_usd','tpnl_quote']

    debug_cols = ['ts', 'instrument', 'amt_filled','px_bid_0',
                    'px_bid_0_base', 'px_ask_0_base', 'px_bid_0_quote', 'px_ask_0_quote', 'px_bid_0_usd', 'px_ask_0_usd',\
                    'px','px_base',  'px_quote',]
    print(df[df.amt_filled != 0][key_cols].head(40))

    # # Calculate difference to verify upnl_base + upnl_quote = upnl_usd
    # df['upnl_diff'] = df['upnl_base'] + df['upnl_quote'] - df['upnl_usd']
    # print("\nUPNL Decomposition Check (showing rows where amt_filled != 0):")
    # check_cols = ['instrument', 'upnl_base', 'upnl_quote', 'upnl_usd', 'upnl_diff']
    # print(df[df.amt_filled != 0][check_cols].head(30))

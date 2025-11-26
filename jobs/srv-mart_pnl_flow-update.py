import os
import psycopg2

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
CONVMAP_TABLE = os.getenv("CONVMAP_TABLE", "convmap_usd")


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
    """
    Update mart_pnl_flow for a given date.

    Implements:
    ✔ Option A: cumulative sums across ALL historical data
    ✔ No resets at midnight
    ✔ Only insert rows belonging to target day
    """

    print(f"Processing PnL flow data for {date_str}")

    date_start = f"{date_str}T00:00:00.000000Z"
    date_end = f"{date_str}T23:59:59.999999Z"

    insert_sql = f"""
WITH
-- ------------------------------------------------------------
-- Load all feed rows up to end of target day
-- ------------------------------------------------------------
feed_all AS (
    SELECT *
    FROM {SOURCE_FEED_TABLE}
    WHERE ts <= '{date_end}'
),

-- ------------------------------------------------------------
-- Load all 1m-deals up to end of target day
-- ------------------------------------------------------------
deals_buy AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_buy_wavg,
        SUM(amt) AS buy_amt,
        COUNT(*) AS buy_count
    FROM {SOURCE_DEALS_TABLE}
    WHERE side = 'BUY'
      AND time <= '{date_end}'
    SAMPLE BY 1m ALIGN TO CALENDAR
),

deals_sell AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_sell_wavg,
        SUM(amt) AS sell_amt,
        COUNT(*) AS sell_count
    FROM {SOURCE_DEALS_TABLE}
    WHERE side = 'SELL'
      AND time <= '{date_end}'
    SAMPLE BY 1m ALIGN TO CALENDAR
),

-- ------------------------------------------------------------
-- Merge BUY/SELL buckets
-- ------------------------------------------------------------
rd AS (
    SELECT
        COALESCE(b.ts_1m, s.ts_1m) AS ts_1m,
        COALESCE(b.instrument, s.instrument) AS instrument,
        b.px_buy_wavg,
        s.px_sell_wavg,
        COALESCE(b.buy_amt, 0) AS buy_amt,
        COALESCE(s.sell_amt, 0) AS sell_amt,
        COALESCE(b.buy_amt, 0) - COALESCE(s.sell_amt, 0) AS amt_filled,
        LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS amt_matched,
        (COALESCE(s.px_sell_wavg, 0) - COALESCE(b.px_buy_wavg, 0))
            * LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS rpnl,
        COALESCE(b.buy_count, 0) + COALESCE(s.sell_count, 0) AS num_deals
    FROM deals_buy b
    FULL OUTER JOIN deals_sell s
        ON b.ts_1m = s.ts_1m
       AND b.instrument = s.instrument
),

-- ------------------------------------------------------------
-- Join FEED + DEALS + FX conversion
-- ------------------------------------------------------------
base AS (
    SELECT
        f.ts AS ts,
        f.instrument,
        COALESCE(rd.amt_filled, 0) AS amt_signed,
        COALESCE(rd.buy_amt, 0) AS buy_amt,
        COALESCE(rd.sell_amt, 0) AS sell_amt,
        COALESCE(rd.amt_matched, 0) AS matched_amt,
        rd.px_buy_wavg AS wavg_buy_px,
        rd.px_sell_wavg AS wavg_sell_px,
        COALESCE(rd.num_deals, 0) AS num_deals,
        f.ask_px_0,
        f.bid_px_0,

        -- ASK/BID converted to USD
        CASE
            WHEN c.usd_instrument IS NULL THEN f.ask_px_0
            WHEN c.is_inverted THEN f.ask_px_0 / u.bid_px_0
            ELSE f.ask_px_0 * u.ask_px_0
        END AS ask_px_0_usd,

        CASE
            WHEN c.usd_instrument IS NULL THEN f.bid_px_0
            WHEN c.is_inverted THEN f.bid_px_0 / u.ask_px_0
            ELSE f.bid_px_0 * u.bid_px_0
        END AS bid_px_0_usd,

        rd.rpnl AS rpnl_native,

        -- Weighted average buy/sell prices converted to USD
        CASE
            WHEN c.usd_instrument IS NULL THEN rd.px_buy_wavg
            WHEN c.is_inverted THEN rd.px_buy_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
            ELSE rd.px_buy_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
        END AS wavg_buy_px_usd,

        CASE
            WHEN c.usd_instrument IS NULL THEN rd.px_sell_wavg
            WHEN c.is_inverted THEN rd.px_sell_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
            ELSE rd.px_sell_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
        END AS wavg_sell_px_usd,

        -- RPNL converted to USD
        CASE
            WHEN c.usd_instrument IS NULL THEN rd.rpnl
            WHEN c.is_inverted THEN rd.rpnl / ((u.ask_px_0 + u.bid_px_0) / 2)
            ELSE rd.rpnl * ((u.ask_px_0 + u.bid_px_0) / 2)
        END AS rpnl_usd,

        -- volume in USD: signed exposure change
        CASE
            WHEN amt_filled < 0 THEN amt_filled *
                (CASE
                    WHEN c.usd_instrument IS NULL THEN f.ask_px_0
                    WHEN c.is_inverted THEN f.ask_px_0 / u.bid_px_0
                    ELSE f.ask_px_0 * u.ask_px_0
                END)
            ELSE amt_filled *
                (CASE
                    WHEN c.usd_instrument IS NULL THEN f.bid_px_0
                    WHEN c.is_inverted THEN f.bid_px_0 / u.ask_px_0
                    ELSE f.bid_px_0 * u.bid_px_0
                END)
        END AS vol_usd,

        -- --------------------------------------------------------
        -- Correct cost basis: use actual fill prices, not market prices
        -- --------------------------------------------------------
        CASE
            WHEN rd.buy_amt > 0 AND rd.sell_amt > 0 THEN
                -- Both buys and sells: cost = buys * buy_px - sells * sell_px
                (rd.buy_amt * 
                    CASE
                        WHEN c.usd_instrument IS NULL THEN rd.px_buy_wavg
                        WHEN c.is_inverted THEN rd.px_buy_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_buy_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
                ) -
                (rd.sell_amt *
                    CASE
                        WHEN c.usd_instrument IS NULL THEN rd.px_sell_wavg
                        WHEN c.is_inverted THEN rd.px_sell_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_sell_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
                )
            WHEN rd.buy_amt > 0 THEN
                -- Only buys
                rd.buy_amt *
                    CASE
                        WHEN c.usd_instrument IS NULL THEN rd.px_buy_wavg
                        WHEN c.is_inverted THEN rd.px_buy_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_buy_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
            WHEN rd.sell_amt > 0 THEN
                -- Only sells (negative cost)
                -(rd.sell_amt *
                    CASE
                        WHEN c.usd_instrument IS NULL THEN rd.px_sell_wavg
                        WHEN c.is_inverted THEN rd.px_sell_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_sell_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
                )
            ELSE 0
        END AS cost_signed_usd

    FROM feed_all f
    LEFT JOIN rd
        ON f.ts = rd.ts_1m
       AND f.instrument = rd.instrument
    LEFT JOIN {CONVMAP_TABLE} c
        ON f.instrument = c.instrument
    LEFT JOIN {SOURCE_FEED_TABLE} u
        ON f.ts = u.ts
       AND c.usd_instrument = u.instrument
),

-- ------------------------------------------------------------
-- Running cumulative sums (stateful PnL engine)
-- ------------------------------------------------------------
cum AS (
    SELECT
        *,
        SUM(amt_signed)       OVER (PARTITION BY instrument ORDER BY ts) AS cum_amt,
        SUM(cost_signed_usd)  OVER (PARTITION BY instrument ORDER BY ts) AS cum_cost_usd
    FROM base
),

-- ------------------------------------------------------------
-- Previous cumulative snapshot
-- ------------------------------------------------------------
lagged AS (
    SELECT
        *,
        LAG(cum_amt)       OVER (PARTITION BY instrument ORDER BY ts) AS prev_cum_amt,
        LAG(cum_cost_usd)  OVER (PARTITION BY instrument ORDER BY ts) AS prev_cum_cost_usd
    FROM cum
),

-- ------------------------------------------------------------
-- Compute realized PnL from reductions & flips
-- ------------------------------------------------------------
rpnl_calc AS (
    SELECT
        *,
        CASE
            -- First row or prev position was flat: only bucket rpnl
            WHEN prev_cum_amt IS NULL OR prev_cum_amt = 0 THEN rpnl_usd
            
            -- Position closed to zero or flipped sign: realize entire prev position
            WHEN cum_amt = 0 OR SIGN(prev_cum_amt) != SIGN(cum_amt)
            THEN
                rpnl_usd +
                prev_cum_amt *
                (
                    CASE
                        WHEN prev_cum_amt > 0 THEN bid_px_0_usd
                        ELSE ask_px_0_usd
                    END
                    -
                    (prev_cum_cost_usd / prev_cum_amt)
                )
            
            -- Position reduced (same sign, smaller absolute value)
            WHEN ABS(cum_amt) < ABS(prev_cum_amt)
            THEN
                rpnl_usd +
                (prev_cum_amt - cum_amt) *
                (
                    CASE
                        WHEN prev_cum_amt > 0 THEN bid_px_0_usd
                        ELSE ask_px_0_usd
                    END
                    -
                    (prev_cum_cost_usd / prev_cum_amt)
                )
            
            -- Position increased or stayed same: only bucket rpnl
            ELSE rpnl_usd
        END AS rpnl_usd_total
    FROM lagged
),

-- ------------------------------------------------------------
-- Final calculations: UPNL, TPNL, cumulative volume
-- ------------------------------------------------------------
final AS (
    SELECT
        ts,
        instrument,
        amt_signed,
        buy_amt,
        sell_amt,
        matched_amt,
        wavg_buy_px,
        wavg_sell_px,
        num_deals,
        ask_px_0,
        bid_px_0,
        ask_px_0_usd,
        bid_px_0_usd,
        rpnl_native AS rpnl,
        rpnl_usd_total AS rpnl_usd,
        vol_usd,
        cum_amt,
        SUM(vol_usd) OVER (PARTITION BY instrument ORDER BY ts) AS cum_vol_usd,
        SUM(rpnl_usd_total) OVER (PARTITION BY instrument ORDER BY ts) AS cum_rpnl_usd,

        -- Unrealized PnL: current position valued at market price minus cost basis
        CASE
            WHEN cum_amt = 0 THEN 0
            WHEN cum_amt > 0 THEN cum_amt * (bid_px_0_usd - (cum_cost_usd / cum_amt))
            ELSE cum_amt * (ask_px_0_usd - (cum_cost_usd / cum_amt))
        END AS upnl_usd
    FROM rpnl_calc
)

-- ------------------------------------------------------------
-- INSERT ONLY TARGET DAY RESULTS
-- ------------------------------------------------------------
INSERT INTO {MART_TABLE}
SELECT
    ts,
    instrument,
    amt_signed,
    buy_amt,
    sell_amt,
    matched_amt,
    wavg_buy_px,
    wavg_sell_px,
    num_deals,
    ask_px_0,
    bid_px_0,
    ask_px_0_usd,
    bid_px_0_usd,
    rpnl,
    rpnl_usd,
    vol_usd,
    cum_amt,
    cum_vol_usd,
    upnl_usd,
    cum_rpnl_usd + upnl_usd AS tpnl_usd
FROM final
WHERE ts BETWEEN '{date_start}' AND '{date_end}';

    """

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(insert_sql)
        conn.commit()

    print(f"Updated {MART_TABLE} for date {date_str}")


if __name__ == "__main__":
    for date_str in [
        "2025-10-20", "2025-10-21", "2025-10-22",
        "2025-10-23", "2025-10-24", "2025-10-25",
        "2025-10-26", "2025-10-27", "2025-10-28",
        "2025-10-29", "2025-10-30"
    ]:
        _update(date_str)

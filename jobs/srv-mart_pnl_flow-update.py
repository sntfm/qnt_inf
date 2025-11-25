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
    Update mart_pnl_flow table for a given date.

    Aggregates deals to 1m buckets, joins to feed_kraken_1m, calculates:
    - avg_px_usd: average price in USD
    - rpnl_usd: realized PnL = amt_signed * (mtm_price - avg_px_usd)
      where mtm_price is ask_px_0_usd if amt_signed < 0, otherwise bid_px_0_usd
    - cum_amt: cumulative position (running sum of amt_signed)
    - cum_volume_usd: cumulative notional value (cum_amt * mtm_price)
    - prev_cum_volume_usd: previous bucket's cumulative notional value
    - upnl_usd: unrealized PnL = (cum_volume_usd / prev_cum_volume_usd - 1) * prev_cum_volume_usd
      (zero at inception bucket)
    """
    print(f"Processing PnL flow data for {date_str}")

    start_ts = f"{date_str}T00:00:00.000000Z"
    end_ts = f"{date_str}T23:59:59.999999Z"

    # Step 1: Create temporary table with aggregated deals
    with _connect() as conn, conn.cursor() as cur:
        # Drop temp table if exists
        cur.execute("DROP TABLE IF EXISTS temp_deals_1m;")

        # Create and populate temp table with aggregated deals
        create_temp_sql = f"""
        CREATE TABLE temp_deals_1m AS (
            SELECT
                time AS ts_1m,
                instrument,
                SUM(CASE WHEN side = 'buy' THEN amt ELSE -amt END) AS net_amt,
                SUM(CASE WHEN side = 'buy' THEN amt * px ELSE -amt * px END) AS net_volume,
                SUM(amt * px) / SUM(amt) AS avg_px,
                COUNT(*) AS num_deals
            FROM {SOURCE_DEALS_TABLE}
            WHERE time BETWEEN '{start_ts}' AND '{end_ts}'
            SAMPLE BY 1m ALIGN TO CALENDAR
        );
        """
        cur.execute(create_temp_sql)
        conn.commit()
        print(f"Created temp_deals_1m with aggregated deals")

    # Step 2: Create temp table with cumulative volume
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS temp_vol_calc;")

        create_vol_sql = f"""
        CREATE TABLE temp_vol_calc AS (
            SELECT
                cum_calc.ts,
                cum_calc.instrument,
                cum_calc.amt_signed,
                cum_calc.avg_px,
                cum_calc.avg_px_usd,
                cum_calc.num_deals,
                cum_calc.ask_px_0,
                cum_calc.bid_px_0,
                cum_calc.ask_px_0_usd,
                cum_calc.bid_px_0_usd,
                cum_calc.cum_amt,
                cum_calc.cum_amt * (CASE WHEN cum_calc.cum_amt < 0 THEN cum_calc.ask_px_0_usd ELSE cum_calc.bid_px_0_usd END) AS cum_volume_usd
            FROM (
                SELECT
                    dwu.ts,
                    dwu.instrument,
                    dwu.amt_signed,
                    dwu.avg_px,
                    dwu.avg_px_usd,
                    dwu.num_deals,
                    dwu.ask_px_0,
                    dwu.bid_px_0,
                    dwu.ask_px_0_usd,
                    dwu.bid_px_0_usd,
                    SUM(dwu.amt_signed) OVER (PARTITION BY dwu.instrument ORDER BY dwu.ts) AS cum_amt
                FROM (
                    SELECT
                        dwf.ts,
                        dwf.instrument,
                        dwf.amt_signed,
                        dwf.avg_px,
                        dwf.num_deals,
                        dwf.ask_px_0,
                        dwf.bid_px_0,
                        CASE WHEN dwf.usd_instrument IS NULL THEN dwf.ask_px_0 WHEN dwf.is_inverted THEN dwf.ask_px_0 / u.bid_px_0 ELSE dwf.ask_px_0 * u.ask_px_0 END AS ask_px_0_usd,
                        CASE WHEN dwf.usd_instrument IS NULL THEN dwf.bid_px_0 WHEN dwf.is_inverted THEN dwf.bid_px_0 / u.ask_px_0 ELSE dwf.bid_px_0 * u.bid_px_0 END AS bid_px_0_usd,
                        CASE WHEN dwf.usd_instrument IS NULL THEN dwf.avg_px WHEN dwf.is_inverted THEN dwf.avg_px / u.bid_px_0 ELSE dwf.avg_px * u.ask_px_0 END AS avg_px_usd
                    FROM (
                        SELECT f.ts, f.instrument, f.ask_px_0, f.bid_px_0,
                               COALESCE(d.net_amt, 0) AS amt_signed, d.avg_px, COALESCE(d.num_deals, 0) AS num_deals,
                               c.usd_instrument, c.is_inverted
                        FROM {SOURCE_FEED_TABLE} f
                        LEFT JOIN temp_deals_1m d ON f.ts = d.ts_1m AND f.instrument = d.instrument
                        LEFT JOIN convmap_usd c ON f.instrument = c.instrument
                        WHERE f.ts BETWEEN '{start_ts}' AND '{end_ts}'
                    ) dwf
                    LEFT JOIN {SOURCE_FEED_TABLE} u ON dwf.usd_instrument = u.instrument AND dwf.ts = u.ts
                ) dwu
            ) cum_calc
        );
        """
        cur.execute(create_vol_sql)
        conn.commit()
        print(f"Created temp_vol_calc with cumulative volume")

    # Step 3: Create temp table with prev_cum_volume_usd using LAG
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS temp_with_lag;")

        create_lag_sql = f"""
        CREATE TABLE temp_with_lag AS (
            SELECT
                ts, instrument, amt_signed, avg_px, avg_px_usd, num_deals,
                ask_px_0, bid_px_0, ask_px_0_usd, bid_px_0_usd,
                cum_amt, cum_volume_usd,
                LAG(cum_volume_usd) OVER (PARTITION BY instrument ORDER BY ts) AS prev_cum_volume_usd
            FROM temp_vol_calc
        );
        """
        cur.execute(create_lag_sql)
        conn.commit()
        print(f"Created temp_with_lag with LAG calculation")

    # Step 4: Insert into mart table
    insert_sql = f"""
    INSERT INTO {MART_TABLE} (ts, instrument, amt_signed, avg_px, avg_px_usd, volume_usd, num_deals,
                              ask_px_0, bid_px_0, ask_px_0_usd, bid_px_0_usd, rpnl_usd,
                              cum_amt, cum_volume_usd, prev_cum_volume_usd, upnl_usd, tpnl_usd)
    SELECT
        ts, instrument, amt_signed, avg_px, avg_px_usd,
        amt_signed * avg_px_usd AS volume_usd,
        num_deals,
        ask_px_0, bid_px_0, ask_px_0_usd, bid_px_0_usd,
        COALESCE(amt_signed * (CASE WHEN amt_signed < 0 THEN ask_px_0_usd ELSE bid_px_0_usd END - avg_px_usd), 0) AS rpnl_usd,
        cum_amt, cum_volume_usd, prev_cum_volume_usd,
        CASE WHEN prev_cum_volume_usd IS NULL OR prev_cum_volume_usd = 0 THEN 0
             ELSE (cum_volume_usd / prev_cum_volume_usd - 1) * prev_cum_volume_usd
        END AS upnl_usd,
        COALESCE(amt_signed * (CASE WHEN amt_signed < 0 THEN ask_px_0_usd ELSE bid_px_0_usd END - avg_px_usd), 0) +
        CASE WHEN prev_cum_volume_usd IS NULL OR prev_cum_volume_usd = 0 THEN 0
             ELSE (cum_volume_usd / prev_cum_volume_usd - 1) * prev_cum_volume_usd
        END AS tpnl_usd
    FROM temp_with_lag
    ORDER BY ts
    """

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(insert_sql)
        conn.commit()
        print(f"Inserted {cur.rowcount} rows into {MART_TABLE}")

        # Clean up temp tables
        cur.execute("DROP TABLE IF EXISTS temp_deals_1m;")
        cur.execute("DROP TABLE IF EXISTS temp_vol_calc;")
        cur.execute("DROP TABLE IF EXISTS temp_with_lag;")
        conn.commit()

    print(f"Updated {MART_TABLE} for date {date_str}")


if __name__ == "__main__":
    # Process date range
    for date_str in ["2025-10-20", "2025-10-21", "2025-10-22",
                    "2025-10-23", "2025-10-24", "2025-10-25",
                    "2025-10-26", "2025-10-27", "2025-10-28",
                    "2025-10-29", "2025-10-30"]:
        _update(date_str)

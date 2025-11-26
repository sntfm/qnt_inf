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
    Update mart_pnl_flow for a given date.

    Combines resampled deals (1m buckets with weighted avg prices)
    with feed_kraken_1m to calculate PnL flow metrics.
    """
    print(f"Processing PnL flow data for {date_str}")

    date_start = f"{date_str}T00:00:00.000000Z"
    date_end = f"{date_str}T23:59:59.999999Z"

    insert_sql = f"""
    INSERT INTO {MART_TABLE}
    SELECT
        rd.ts_1m AS ts,
        rd.instrument,
        rd.amt_filled AS amt_signed,
        rd.buy_amt,
        rd.sell_amt,
        rd.amt_matched AS matched_amt,
        rd.px_buy_wavg AS wavg_buy_px,
        rd.px_sell_wavg AS wavg_sell_px,
        rd.num_deals,
        f.ask_px_0,
        f.bid_px_0,
        NULL AS ask_px_0_usd,
        NULL AS bid_px_0_usd,
        rd.rpnl
    FROM (
        SELECT
            COALESCE(b.ts_1m, s.ts_1m) AS ts_1m,
            COALESCE(b.instrument, s.instrument) AS instrument,
            b.px_buy_wavg,
            s.px_sell_wavg,
            COALESCE(b.buy_amt, 0) AS buy_amt,
            COALESCE(s.sell_amt, 0) AS sell_amt,
            COALESCE(b.buy_amt, 0) - COALESCE(s.sell_amt, 0) AS amt_filled,
            LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS amt_matched,
            (COALESCE(s.px_sell_wavg, 0) - COALESCE(b.px_buy_wavg, 0)) * LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS rpnl,
            COALESCE(b.buy_count, 0) + COALESCE(s.sell_count, 0) AS num_deals
        FROM (
            SELECT
                time AS ts_1m,
                instrument,
                SUM(amt * px) / SUM(amt) AS px_buy_wavg,
                SUM(amt) AS buy_amt,
                COUNT(*) AS buy_count
            FROM {SOURCE_DEALS_TABLE}
            WHERE side = 'BUY'
              AND time BETWEEN '{date_start}' AND '{date_end}'
            SAMPLE BY 1m ALIGN TO CALENDAR
        ) b
        FULL OUTER JOIN (
            SELECT
                time AS ts_1m,
                instrument,
                SUM(amt * px) / SUM(amt) AS px_sell_wavg,
                SUM(amt) AS sell_amt,
                COUNT(*) AS sell_count
            FROM {SOURCE_DEALS_TABLE}
            WHERE side = 'SELL'
              AND time BETWEEN '{date_start}' AND '{date_end}'
            SAMPLE BY 1m ALIGN TO CALENDAR
        ) s
            ON b.ts_1m = s.ts_1m
            AND b.instrument = s.instrument
    ) rd
    LEFT JOIN {SOURCE_FEED_TABLE} f
        ON rd.ts_1m = f.ts
        AND rd.instrument = f.instrument
    """

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(insert_sql)
        conn.commit()

    print(f"Updated {MART_TABLE} for date {date_str}")

if __name__ == "__main__":
    # Process date range
    for date_str in ["2025-10-20", "2025-10-21", "2025-10-22",
                    "2025-10-23", "2025-10-24", "2025-10-25",
                    "2025-10-26", "2025-10-27", "2025-10-28",
                    "2025-10-29", "2025-10-30"]:
        _update(date_str)

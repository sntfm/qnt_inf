"""Test the actual SQL query being generated for DOGE/GBP."""
import os
import psycopg2
from psycopg2.extras import RealDictCursor

QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

SLICES_TABLE = "mart_kraken_decay_slices"
PRICES_TABLE = "feed_kraken_1s"
FRAME_MINS = 15

conn = psycopg2.connect(
    host=QUESTDB_HOST,
    port=QUESTDB_PORT,
    user=QUESTDB_USER,
    password=QUESTDB_PASSWORD,
    database=QUESTDB_DB,
    connect_timeout=30,
)

# Use the first DOGE/GBP deal
deal_time = '2025-10-20T07:08:09.803000Z'
instrument = 'Kraken.Spot.DOGE/GBP_SPOT'
usd_instrument = 'Kraken.Spot.GBP/USD_SPOT'
deal_px = 0.1506349
deal_amt = 250.0
entry_usd = 18.9  # approximate

with conn.cursor(cursor_factory=RealDictCursor) as cur:
    # Test the SELECT part of the query (non-inverted, SELL side)
    sql = f"""
        SELECT
            CAST('{deal_time}' AS TIMESTAMP) time,
            CAST('{instrument}' AS SYMBOL) instrument,
            CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_from_deal,
            p.ask_px_0 ask_px_0,
            p.bid_px_0 bid_px_0,
            p.ask_px_0 * u.ask_px_0 usd_ask_px_0,
            p.bid_px_0 * u.bid_px_0 usd_bid_px_0,
            ({deal_px} - p.ask_px_0) / {deal_px} ret,
            {entry_usd} - p.ask_px_0 * {deal_amt} * (p.ask_px_0 * u.ask_px_0) pnl_usd
        FROM {PRICES_TABLE} p
        JOIN {PRICES_TABLE} u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
        WHERE p.instrument = '{instrument}'
          AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                       AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
        LIMIT 5
    """
    
    print("Testing query:")
    print(sql)
    print("\nExecuting...")
    
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        print(f"\nGot {len(rows)} rows")
        for row in rows:
            print(f"  t_from_deal={row['t_from_deal']}, ask={row['ask_px_0']}, bid={row['bid_px_0']}")
    except Exception as e:
        print(f"ERROR: {e}")

conn.close()

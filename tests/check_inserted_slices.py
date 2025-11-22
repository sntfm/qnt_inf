"""Check what was actually inserted for the DOGE/GBP deal."""
import os
import psycopg2
from psycopg2.extras import RealDictCursor

QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

conn = psycopg2.connect(
    host=QUESTDB_HOST,
    port=QUESTDB_PORT,
    user=QUESTDB_USER,
    password=QUESTDB_PASSWORD,
    database=QUESTDB_DB,
    connect_timeout=30,
)

with conn.cursor(cursor_factory=RealDictCursor) as cur:
    # Check slices for the first DOGE/GBP deal
    sql = """
        SELECT time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0
        FROM mart_kraken_decay_slices
        WHERE time = '2025-10-20T07:08:09.803000Z'
          AND instrument = 'Kraken.Spot.DOGE/GBP_SPOT'
        ORDER BY t_from_deal
        LIMIT 10
    """
    
    print("Slices for DOGE/GBP deal at 2025-10-20T07:08:09.803000Z:")
    cur.execute(sql)
    rows = cur.fetchall()
    
    if not rows:
        print("  NO ROWS FOUND!")
    else:
        for row in rows:
            print(f"  t_from_deal={row['t_from_deal']}, ask={row['ask_px_0']}, bid={row['bid_px_0']}, usd_ask={row['usd_ask_px_0']}, usd_bid={row['usd_bid_px_0']}")

conn.close()

"""Check if DOGE/GBP has USD conversion mapping."""
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
    # Check convmap for DOGE/GBP
    sql = """
        SELECT instrument, usd_instrument, is_inverted
        FROM convmap_usd
        WHERE instrument LIKE '%DOGE/GBP%'
    """
    cur.execute(sql)
    rows = cur.fetchall()
    
    print("DOGE/GBP in convmap:")
    for row in rows:
        print(f"  {row}")
    
    if not rows:
        print("  Not found - this instrument has NO USD conversion")
    
    # Check a sample deal for DOGE/GBP
    sql = """
        SELECT time, instrument, side, amt, px
        FROM mart_kraken_decay_deals
        WHERE instrument LIKE '%DOGE/GBP%'
          AND time >= '2025-10-20T00:00:00.000000Z'
          AND time < '2025-10-21T00:00:00.000000Z'
        LIMIT 5
    """
    cur.execute(sql)
    rows = cur.fetchall()
    
    print("\nSample DOGE/GBP deals:")
    for row in rows:
        print(f"  {row}")

conn.close()

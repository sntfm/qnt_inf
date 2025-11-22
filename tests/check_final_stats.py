"""Check t_from_deal statistics including the new test data."""
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
    # Check the test data we just inserted
    sql = """
        SELECT time, instrument, t_from_deal, ask_px_0, bid_px_0
        FROM mart_kraken_decay_slices
        WHERE time = '2025-10-20T10:00:00.000000Z'
          AND instrument = 'Kraken.Spot.DOGE/GBP_SPOT'
        LIMIT 10
    """
    
    print("Test data (time=10:00:00):")
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        print(f"  t_from_deal={row['t_from_deal']}, ask={row['ask_px_0']}, bid={row['bid_px_0']}")
    
    # Check overall statistics
    sql = """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(t_from_deal) as non_null_count,
            MIN(t_from_deal) as min_val,
            MAX(t_from_deal) as max_val
        FROM mart_kraken_decay_slices
        WHERE time >= '2025-10-20T00:00:00.000000Z' 
          AND time < '2025-10-21T00:00:00.000000Z'
    """
    cur.execute(sql)
    result = cur.fetchone()
    
    print(f"\nOverall statistics:")
    print(f"  Total rows: {result['total_rows']}")
    print(f"  Non-null count: {result['non_null_count']}")
    print(f"  Null count: {result['total_rows'] - result['non_null_count']}")
    print(f"  Percentage non-null: {100 * result['non_null_count'] / result['total_rows']:.1f}%")

conn.close()

"""Quick check to verify t_from_deal values are populated."""
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
    # Check if t_from_deal has values
    sql = """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(t_from_deal) as non_null_count,
            MIN(t_from_deal) as min_val,
            MAX(t_from_deal) as max_val,
            AVG(t_from_deal) as avg_val
        FROM mart_kraken_decay_slices
        WHERE time >= '2025-10-20T00:00:00.000000Z' 
          AND time < '2025-10-21T00:00:00.000000Z'
    """
    cur.execute(sql)
    result = cur.fetchone()
    
    print("t_from_deal Statistics for 2025-10-20:")
    print(f"  Total rows: {result['total_rows']}")
    print(f"  Non-null count: {result['non_null_count']}")
    print(f"  Min value: {result['min_val']}")
    print(f"  Max value: {result['max_val']}")
    print(f"  Avg value: {result['avg_val']}")
    
    # Sample a few rows
    sql = """
        SELECT time, instrument, t_from_deal, ask_px_0, bid_px_0
        FROM mart_kraken_decay_slices
        WHERE time >= '2025-10-20T00:00:00.000000Z' 
          AND time < '2025-10-21T00:00:00.000000Z'
        LIMIT 10
    """
    cur.execute(sql)
    rows = cur.fetchall()
    
    print("\nSample rows:")
    for row in rows:
        print(f"  {row['time']} | {row['instrument']} | t_from_deal={row['t_from_deal']} | ask={row['ask_px_0']:.4f} | bid={row['bid_px_0']:.4f}")

conn.close()

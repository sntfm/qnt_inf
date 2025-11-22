"""Test INSERT...SELECT with JOIN and extract(epoch)."""
import os
import psycopg2

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

deal_time = '2025-10-20T07:08:09.803000Z'
instrument = 'Kraken.Spot.DOGE/GBP_SPOT'
usd_instrument = 'Kraken.Spot.GBP/USD_SPOT'

with conn.cursor() as cur:
    # Create a test table
    try:
        cur.execute("DROP TABLE test_join_time_diff")
        conn.commit()
    except:
        pass
    
    cur.execute("""
        CREATE TABLE test_join_time_diff (
            ts TIMESTAMP,
            t_diff INT,
            ask_px DOUBLE,
            usd_ask_px DOUBLE
        ) timestamp(ts)
    """)
    conn.commit()
    print("Created test table")
    
    # Test INSERT with JOIN and extract(epoch)
    sql = f"""
        INSERT INTO test_join_time_diff (ts, t_diff, ask_px, usd_ask_px)
        SELECT
            p.ts,
            CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_diff,
            p.ask_px_0 ask_px,
            p.ask_px_0 * u.ask_px_0 usd_ask_px
        FROM feed_kraken_1s p
        JOIN feed_kraken_1s u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
        WHERE p.instrument = '{instrument}'
          AND p.ts BETWEEN DATEADD('m', -15, '{deal_time}')
                       AND DATEADD('m', 15, '{deal_time}')
        LIMIT 10
    """
    
    print("\nInserting with JOIN and extract(epoch)...")
    print(sql)
    cur.execute(sql)
    conn.commit()
    
    # Check what was inserted
    cur.execute("SELECT * FROM test_join_time_diff LIMIT 10")
    rows = cur.fetchall()
    
    print(f"\nInserted {len(rows)} rows:")
    for row in rows:
        print(f"  ts={row[0]}, t_diff={row[1]}, ask_px={row[2]}, usd_ask_px={row[3]}")
    
    # Clean up
    cur.execute("DROP TABLE test_join_time_diff")
    conn.commit()

conn.close()

"""Test INSERT...SELECT with extract(epoch) to see if it produces nulls."""
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

with conn.cursor() as cur:
    # Create a test table
    try:
        cur.execute("DROP TABLE test_time_diff")
        conn.commit()
    except:
        pass
    
    cur.execute("""
        CREATE TABLE test_time_diff (
            ts TIMESTAMP,
            t_diff_extract INT,
            t_diff_datediff INT
        ) timestamp(ts)
    """)
    conn.commit()
    print("Created test table")
    
    # Test INSERT with extract(epoch)
    sql = f"""
        INSERT INTO test_time_diff (ts, t_diff_extract, t_diff_datediff)
        SELECT
            ts,
            CAST((extract(epoch from ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP))) AS INT) t_diff_extract,
            DATEDIFF('s', CAST('{deal_time}' AS TIMESTAMP), ts) t_diff_datediff
        FROM feed_kraken_1s
        WHERE instrument = 'Kraken.Spot.DOGE/GBP_SPOT'
          AND ts BETWEEN DATEADD('m', -15, '{deal_time}')
                     AND DATEADD('m', 15, '{deal_time}')
        LIMIT 10
    """
    
    print("\nInserting with both methods...")
    cur.execute(sql)
    conn.commit()
    
    # Check what was inserted
    cur.execute("SELECT * FROM test_time_diff LIMIT 10")
    rows = cur.fetchall()
    
    print(f"\nInserted {len(rows)} rows:")
    for row in rows:
        print(f"  ts={row[0]}, t_diff_extract={row[1]}, t_diff_datediff={row[2]}")
    
    # Clean up
    cur.execute("DROP TABLE test_time_diff")
    conn.commit()

conn.close()

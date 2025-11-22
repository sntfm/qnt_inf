"""Test different ways to calculate time difference in QuestDB."""
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

deal_time = '2025-10-20T07:08:09.803000Z'

with conn.cursor(cursor_factory=RealDictCursor) as cur:
    # Test different approaches
    test_queries = [
        ("DATEDIFF('us', CAST('{deal_time}' AS TIMESTAMP), ts) / 1000000", "DATEDIFF microseconds / 1000000"),
        ("(extract(epoch from ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)))", "extract epoch difference"),
        ("CAST((extract(epoch from ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP))) AS INT)", "CAST extract epoch to INT"),
        ("DATEDIFF('s', CAST('{deal_time}' AS TIMESTAMP), ts)", "DATEDIFF seconds (original)"),
    ]
    
    for expr, desc in test_queries:
        sql = f"""
            SELECT 
                ts,
                {expr.format(deal_time=deal_time)} AS t_from_deal
            FROM feed_kraken_1s
            WHERE instrument = 'Kraken.Spot.DOGE/GBP_SPOT'
              AND ts BETWEEN DATEADD('m', -15, '{deal_time}')
                         AND DATEADD('m', 15, '{deal_time}')
            LIMIT 3
        """
        
        print(f"\nTesting: {desc}")
        print(f"Expression: {expr.format(deal_time=deal_time)}")
        
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            for row in rows:
                print(f"  ts={row['ts']}, t_from_deal={row['t_from_deal']}")
        except Exception as e:
            print(f"  ERROR: {e}")

conn.close()

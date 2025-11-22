"""Test extract(epoch) function in QuestDB."""
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
    # Test extract(epoch) with a simple timestamp
    test_queries = [
        ("extract(epoch from CAST('2025-10-20T07:08:09.803000Z' AS TIMESTAMP))", "Extract epoch from cast"),
        ("extract(epoch from to_timestamp('2025-10-20T07:08:09.803000Z', 'yyyy-MM-ddTHH:mm:ss.SSSSSSZ'))", "Extract epoch from to_timestamp"),
        ("CAST('2025-10-20T07:08:09.803000Z' AS TIMESTAMP)", "Just cast to timestamp"),
    ]
    
    for query, desc in test_queries:
        try:
            sql = f"SELECT {query} AS result"
            print(f"\nTesting: {desc}")
            print(f"Query: {sql}")
            cur.execute(sql)
            result = cur.fetchone()
            print(f"Result: {result}")
        except Exception as e:
            print(f"ERROR: {e}")

conn.close()

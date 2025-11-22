"""Check the schema of mart_kraken_decay_slices table."""
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
    # Get table schema
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'mart_kraken_decay_slices'
        ORDER BY ordinal_position
    """
    
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        
        print("Schema of mart_kraken_decay_slices:")
        for row in rows:
            print(f"  {row['column_name']}: {row['data_type']}")
    except Exception as e:
        print(f"ERROR getting schema via information_schema: {e}")
        
        # Try QuestDB-specific way
        sql = "SHOW COLUMNS FROM mart_kraken_decay_slices"
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            print("\nSchema (via SHOW COLUMNS):")
            for row in rows:
                print(f"  {row}")
        except Exception as e2:
            print(f"ERROR with SHOW COLUMNS: {e2}")

conn.close()

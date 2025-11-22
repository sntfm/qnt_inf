"""Clear decay slices data for 2025-10-20 before re-running."""
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

with conn.cursor() as cur:
    # QuestDB uses different DELETE syntax
    sql = """
        DELETE FROM mart_kraken_decay_slices
        WHERE time IN (
            SELECT time FROM mart_kraken_decay_slices
            WHERE time >= '2025-10-20T00:00:00.000000Z' 
              AND time < '2025-10-21T00:00:00.000000Z'
        )
    """
    cur.execute(sql)
    conn.commit()
    print("Deleted decay slices for 2025-10-20")

conn.close()

"""
Job to resample feed data into a mart table.

Resamples feed_kraken_tob_5 into mart_kraken_tob_1s with FILL(PREV).
"""

import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# QuestDB connection settings
# ---------------------------------------------------------------------------
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

SOURCE_TABLE = "feed_kraken_tob_5"
MART_TABLE = "feed_kraken_1s"
RESAMPLE = "1s"


def _connect():
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DB,
        connect_timeout=30,
    )


def _update(date_str: str):
    """Resample one day of data from source to mart table."""
    print(f"Resampling {SOURCE_TABLE} -> {MART_TABLE} for {date_str}")

    sql = f"""
        INSERT INTO {MART_TABLE} (ts, instrument, ask_px_0, bid_px_0)
        SELECT
            ts_server AS ts,
            instrument,
            last(ask_px_0) AS ask_px_0,
            last(bid_px_0) AS bid_px_0
        FROM {SOURCE_TABLE}
        WHERE ts_server BETWEEN '{date_str}T00:00:00.000000Z' AND '{date_str}T23:59:59.999999Z'
        SAMPLE BY {RESAMPLE} FILL(PREV) ALIGN TO CALENDAR
    """

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

    print(f"Done resampling {date_str}")


if __name__ == "__main__":
    for date_str in ["2025-10-20", "2025-10-21", "2025-10-22",
                     "2025-10-23", "2025-10-24", "2025-10-25",
                     "2025-10-26", "2025-10-27", "2025-10-28",
                     "2025-10-29", "2025-10-30"]:
        _update(date_str)
import os
import psycopg2

# ---------------------------------------------------------------------------
# QuestDB connection settings
# ---------------------------------------------------------------------------
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

SOURCE_TABLE = os.getenv("LATENCY_SOURCE_TABLE", "feed_kraken_tob_5")
MART_TABLE = os.getenv("LATENCY_MART_TABLE", "mart_kraken_latency")
STATS_TABLE = os.getenv("LATENCY_STATS_TABLE", "mart_kraken_latency_stats")
BIN_SIZE_MS = float(os.getenv("LATENCY_BIN_MS", "2"))
MAX_LATENCY_MS = float(os.getenv("LATENCY_MAX_MS", "200"))


def _connect():
    """Create a new psycopg2 connection to QuestDB's Postgres endpoint."""
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DB,
        connect_timeout=30,
    )


def _update(date_str: str):
    print(f"Processing latency data for {date_str}")

    ts_value = f"{date_str}T00:00:00.000000Z"

    insert_sql = f"""
    INSERT INTO {MART_TABLE} (ts, date, hour, latency_bin_start_ms, bin_count)
    SELECT
        '{ts_value}' AS ts,
        '{date_str}' AS date,
        hour,
        FLOOR(latency_ms / {BIN_SIZE_MS}) * {BIN_SIZE_MS} AS latency_bin_start_ms,
        COUNT(*) AS bin_count
    FROM (
        SELECT
            (CAST(ts_server AS LONG)/1000.0 - ts_exch) AS latency_ms,
            EXTRACT(HOUR FROM to_timezone(ts_server, 'UTC')) AS hour
        FROM {SOURCE_TABLE}
        WHERE ts_server BETWEEN '{date_str}T00:00:00.000000Z' AND '{date_str}T23:59:59.999999Z'
    )
    WHERE latency_ms >= 0 AND latency_ms <= {MAX_LATENCY_MS}
    GROUP BY hour, latency_bin_start_ms
    ORDER BY hour, latency_bin_start_ms
    """

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(insert_sql)
        conn.commit()

    print(f"Updated {MART_TABLE} for date {date_str}")


def _update_stats(date_str: str):
    """Calculate and insert latency statistics (mean, std, median, p99) per hour."""
    print(f"Processing latency stats for {date_str}")

    ts_value = f"{date_str}T00:00:00.000000Z"

    insert_sql = f"""
    INSERT INTO {STATS_TABLE} (ts, date, mean_ms, std_ms, median_ms, p99_ms, sample_count)
    SELECT
        '{ts_value}' AS ts,
        '{date_str}' AS date,
        AVG(latency_ms) AS mean_ms,
        STDDEV_SAMP(latency_ms) AS std_ms,
        approx_percentile(latency_ms, 0.50) AS median_ms,
        approx_percentile(latency_ms, 0.99) AS p99_ms,
        COUNT(*) AS sample_count
    FROM (
        SELECT
            (CAST(ts_server AS LONG)/1000.0 - ts_exch) AS latency_ms
        FROM {SOURCE_TABLE}
        WHERE ts_server BETWEEN '{date_str}T00:00:00.000000Z' AND '{date_str}T23:59:59.999999Z'
    )
    WHERE latency_ms >= 0 AND latency_ms <= {MAX_LATENCY_MS}
    """

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(insert_sql)
        conn.commit()

    print(f"Updated {STATS_TABLE} for date {date_str}")


if __name__ == "__main__":
    from datetime import datetime, timezone
    date_str = datetime.now(timezone.utc).date().isoformat()
    for date_str in ["2025-10-20", "2025-10-21", "2025-10-22",
                    "2025-10-23", "2025-10-24", "2025-10-25",
                    "2025-10-26", "2025-10-27", "2025-10-28",
                    "2025-10-29", "2025-10-30"]:
        _update(date_str)
        _update_stats(date_str)
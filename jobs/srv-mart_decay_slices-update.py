"""
Job to populate mart_kraken_decay_slices table.

Uses precomputed feed_kraken_1s table. Processes deals one by one
with efficient timestamp-bounded queries.
"""

import os

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

DEALS_TABLE = "mart_kraken_decay_deals"
SLICES_TABLE = "mart_kraken_decay_slices"
PRICES_TABLE = "feed_kraken_1s"
CONVMAP_TABLE = "convmap_usd"

FRAME_MINS = 15


def _connect():
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DB,
        connect_timeout=30,
    )


def _fetch_convmap(cur) -> dict:
    """Fetch USD conversion map: {instrument: (usd_instrument, is_inverted)}"""
    cur.execute(f"SELECT instrument, usd_instrument, is_inverted FROM {CONVMAP_TABLE}")
    rows = cur.fetchall()
    return {row['instrument']: (row['usd_instrument'], row['is_inverted']) for row in rows}


def _fetch_deals(cur, date_str: str) -> list:
    """Fetch deals for a specific date."""
    sql = f"""
        SELECT time, instrument
        FROM {DEALS_TABLE}
        WHERE time BETWEEN '{date_str}T00:00:00.000000Z' AND '{date_str}T23:59:59.999999Z'
        ORDER BY time
    """
    cur.execute(sql)
    return cur.fetchall()


def _process_deal(cur, deal, convmap: dict):
    """Process a single deal - insert slices directly."""
    deal_time = deal['time']
    instrument = deal['instrument']

    # Get USD conversion info
    usd_info = convmap.get(instrument)

    if usd_info is None:
        # No USD conversion needed - simple query
        sql = f"""
            INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0)
            SELECT
                '{deal_time}',
                '{instrument}',
                CAST(DATEDIFF('s', '{deal_time}', ts) AS INT),
                ask_px_0,
                bid_px_0,
                ask_px_0,
                bid_px_0
            FROM {PRICES_TABLE}
            WHERE instrument = '{instrument}'
              AND ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                         AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
        """
    else:
        usd_instrument, is_inverted = usd_info
        if is_inverted:
            # Inverted: divide by USD price
            sql = f"""
                INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0)
                SELECT
                    '{deal_time}',
                    '{instrument}',
                    CAST(DATEDIFF('s', '{deal_time}', p.ts) AS INT),
                    p.ask_px_0,
                    p.bid_px_0,
                    p.ask_px_0 / u.bid_px_0,
                    p.bid_px_0 / u.ask_px_0
                FROM {PRICES_TABLE} p
                JOIN {PRICES_TABLE} u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
                WHERE p.instrument = '{instrument}'
                  AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                               AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
            """
        else:
            # Normal: multiply by USD price
            sql = f"""
                INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0)
                SELECT
                    '{deal_time}',
                    '{instrument}',
                    CAST(DATEDIFF('s', '{deal_time}', p.ts) AS INT),
                    p.ask_px_0,
                    p.bid_px_0,
                    p.ask_px_0 * u.ask_px_0,
                    p.bid_px_0 * u.bid_px_0
                FROM {PRICES_TABLE} p
                JOIN {PRICES_TABLE} u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
                WHERE p.instrument = '{instrument}'
                  AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                               AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
            """

    cur.execute(sql)


def _update(date_str: str):
    """Process all deals for a given date."""
    print(f"Processing decay slices for {date_str}")

    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            convmap = _fetch_convmap(cur)
            print(f"Loaded {len(convmap)} USD conversion mappings")

            deals = _fetch_deals(cur, date_str)
            print(f"Found {len(deals)} deals")

        # Use regular cursor for inserts
        with conn.cursor() as cur:
            for idx, deal in enumerate(deals):
                _process_deal(cur, deal, convmap)
                if (idx + 1) % 100 == 0:
                    conn.commit()
                    print(f"Processed {idx + 1}/{len(deals)} deals")

            conn.commit()

    print(f"Done processing {date_str}")


if __name__ == "__main__":
    for date_str in ["2025-10-20", "2025-10-21", "2025-10-22",
                    "2025-10-23", "2025-10-24", "2025-10-25",
                    "2025-10-26", "2025-10-27", "2025-10-28",
                    "2025-10-29", "2025-10-30"]:
        _update(date_str)

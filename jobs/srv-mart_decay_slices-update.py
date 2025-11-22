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


def _fetch_price_at(cur, instrument: str, timestamp: str):
    """Fetch the nearest price for an instrument at or before the given timestamp."""
    sql = f"""
        SELECT ask_px_0, bid_px_0
        FROM {PRICES_TABLE}
        WHERE instrument = '{instrument}'
          AND ts <= '{timestamp}'
        ORDER BY ts DESC
        LIMIT 1
    """
    cur.execute(sql)
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return None


def _fetch_deals(cur, date_str: str) -> list:
    """Fetch deals for a specific date with side, amt, px for return/pnl calculations."""
    sql = f"""
        SELECT time, instrument, side, amt, px
        FROM {DEALS_TABLE}
        WHERE time BETWEEN '{date_str}T00:00:00.000000Z' AND '{date_str}T23:59:59.999999Z'
        ORDER BY time
    """
    cur.execute(sql)
    return cur.fetchall()


def _process_deal(cur, deal, convmap: dict):
    """Process a single deal - insert slices directly with return and pnl_usd calculations."""
    deal_time = deal['time']
    instrument = deal['instrument']
    deal_side = deal['side']
    deal_amt = deal['amt']
    deal_px = deal['px']

    # Get USD conversion info
    usd_info = convmap.get(instrument)

    # Return calculation:
    # BUY: (bid_px_0 - deal_px) / deal_px  (current sell price - entry price)
    # SELL: (deal_px - ask_px_0) / deal_px  (entry price - current buyback price)
    #
    # PnL USD calculation:
    # deal_volume_usd = deal_px * deal_amt * usd_px_at_t0
    # BUY: bid_px_0 * deal_amt * usd_bid_px_0 - deal_volume_usd
    # SELL: deal_volume_usd - ask_px_0 * deal_amt * usd_ask_px_0
    #
    # For usd_px_at_t0, we use usd_ask_px_0 at t=0 (deal time)

    if usd_info is None:
        # No USD conversion needed - usd prices equal native prices
        # Precompute entry_usd for pnl calculation
        entry_usd = deal_px * deal_amt
        if deal_side == 'BUY':
            sql = f"""
                INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0, ret, pnl_usd)
                SELECT
                    CAST('{deal_time}' AS TIMESTAMP) time,
                    CAST('{instrument}' AS SYMBOL) instrument,
                    CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_from_deal,
                    p.ask_px_0 ask_px_0,
                    p.bid_px_0 bid_px_0,
                    p.ask_px_0 usd_ask_px_0,
                    p.bid_px_0 usd_bid_px_0,
                    (p.bid_px_0 - {deal_px}) / {deal_px} ret,
                    p.bid_px_0 * {deal_amt} - {entry_usd} pnl_usd
                FROM {PRICES_TABLE} p
                WHERE p.instrument = '{instrument}'
                  AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                             AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
            """
        else:
            sql = f"""
                INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0, ret, pnl_usd)
                SELECT
                    CAST('{deal_time}' AS TIMESTAMP) time,
                    CAST('{instrument}' AS SYMBOL) instrument,
                    CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_from_deal,
                    p.ask_px_0 ask_px_0,
                    p.bid_px_0 bid_px_0,
                    p.ask_px_0 usd_ask_px_0,
                    p.bid_px_0 usd_bid_px_0,
                    ({deal_px} - p.ask_px_0) / {deal_px} ret,
                    {entry_usd} - p.ask_px_0 * {deal_amt} pnl_usd
                FROM {PRICES_TABLE} p
                WHERE p.instrument = '{instrument}'
                  AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                             AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
            """
    else:
        usd_instrument, is_inverted = usd_info
        
        # Fetch USD rate at deal time
        usd_prices = _fetch_price_at(cur, usd_instrument, deal_time)
        if not usd_prices:
            # print(f"Warning: No USD price found for {usd_instrument} at {deal_time}")
            return

        u_ask_0, u_bid_0 = usd_prices

        if is_inverted:
            # Inverted: rate = 1 / price
            # rate_ask = 1 / u_bid, rate_bid = 1 / u_ask
            rate_ask = 1.0 / u_bid_0 if u_bid_0 else 0
            rate_bid = 1.0 / u_ask_0 if u_ask_0 else 0
        else:
            rate_ask = u_ask_0
            rate_bid = u_bid_0

        # Calculate entry_usd (deal volume in USD)
        # We use the ask rate for the entry valuation (conservative/standard approach?)
        # Previous code used e.ask_px_0 / eu.bid_px_0 for inverted, which maps to rate_ask.
        entry_usd = deal_px * deal_amt * rate_ask

        if is_inverted:
            if deal_side == 'BUY':
                sql = f"""
                    INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0, ret, pnl_usd)
                    SELECT
                        CAST('{deal_time}' AS TIMESTAMP) time,
                        CAST('{instrument}' AS SYMBOL) instrument,
                        CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_from_deal,
                        p.ask_px_0 ask_px_0,
                        p.bid_px_0 bid_px_0,
                        p.ask_px_0 / u.bid_px_0 usd_ask_px_0,
                        p.bid_px_0 / u.ask_px_0 usd_bid_px_0,
                        (p.bid_px_0 - {deal_px}) / {deal_px} ret,
                        p.bid_px_0 * {deal_amt} * (p.bid_px_0 / u.ask_px_0) - {entry_usd} pnl_usd
                    FROM {PRICES_TABLE} p
                    JOIN {PRICES_TABLE} u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
                    WHERE p.instrument = '{instrument}'
                      AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                                   AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
                """
            else:
                sql = f"""
                    INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0, ret, pnl_usd)
                    SELECT
                        CAST('{deal_time}' AS TIMESTAMP) time,
                        CAST('{instrument}' AS SYMBOL) instrument,
                        CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_from_deal,
                        p.ask_px_0 ask_px_0,
                        p.bid_px_0 bid_px_0,
                        p.ask_px_0 / u.bid_px_0 usd_ask_px_0,
                        p.bid_px_0 / u.ask_px_0 usd_bid_px_0,
                        ({deal_px} - p.ask_px_0) / {deal_px} ret,
                        {entry_usd} - p.ask_px_0 * {deal_amt} * (p.ask_px_0 / u.bid_px_0) pnl_usd
                    FROM {PRICES_TABLE} p
                    JOIN {PRICES_TABLE} u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
                    WHERE p.instrument = '{instrument}'
                      AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                                   AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
                """
        else:
            if deal_side == 'BUY':
                sql = f"""
                    INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0, ret, pnl_usd)
                    SELECT
                        CAST('{deal_time}' AS TIMESTAMP) time,
                        CAST('{instrument}' AS SYMBOL) instrument,
                        CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_from_deal,
                        p.ask_px_0 ask_px_0,
                        p.bid_px_0 bid_px_0,
                        p.ask_px_0 * u.ask_px_0 usd_ask_px_0,
                        p.bid_px_0 * u.bid_px_0 usd_bid_px_0,
                        (p.bid_px_0 - {deal_px}) / {deal_px} ret,
                        p.bid_px_0 * {deal_amt} * (p.bid_px_0 * u.bid_px_0) - {entry_usd} pnl_usd
                    FROM {PRICES_TABLE} p
                    JOIN {PRICES_TABLE} u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
                    WHERE p.instrument = '{instrument}'
                      AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                                   AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
                """
            else:
                sql = f"""
                    INSERT INTO {SLICES_TABLE} (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0, ret, pnl_usd)
                    SELECT
                        CAST('{deal_time}' AS TIMESTAMP) time,
                        CAST('{instrument}' AS SYMBOL) instrument,
                        CAST(extract(epoch from p.ts) - extract(epoch from CAST('{deal_time}' AS TIMESTAMP)) AS INT) t_from_deal,
                        p.ask_px_0 ask_px_0,
                        p.bid_px_0 bid_px_0,
                        p.ask_px_0 * u.ask_px_0 usd_ask_px_0,
                        p.bid_px_0 * u.bid_px_0 usd_bid_px_0,
                        ({deal_px} - p.ask_px_0) / {deal_px} ret,
                        {entry_usd} - p.ask_px_0 * {deal_amt} * (p.ask_px_0 * u.ask_px_0) pnl_usd
                    FROM {PRICES_TABLE} p
                    JOIN {PRICES_TABLE} u ON u.ts = p.ts AND u.instrument = '{usd_instrument}'
                    WHERE p.instrument = '{instrument}'
                      AND p.ts BETWEEN DATEADD('m', -{FRAME_MINS}, '{deal_time}')
                                   AND DATEADD('m', {FRAME_MINS}, '{deal_time}')
                """

    cur.execute(sql)


def _update_amt_usd(cur, deal):
    """Update amt_usd for a deal from slices at t_from_deal=0."""
    deal_time = deal['time']
    instrument = deal['instrument']
    deal_amt = deal['amt']

    fetch_sql = f"""
        SELECT (usd_ask_px_0 + usd_bid_px_0) / 2 AS mid_usd
        FROM {SLICES_TABLE}
        WHERE time = '{deal_time}' AND instrument = '{instrument}' AND t_from_deal = 0
    """
    cur.execute(fetch_sql)
    row = cur.fetchone()
    if row and row[0] is not None:
        mid_usd = row[0]
        amt_usd = deal_amt * mid_usd
        update_sql = f"""
            UPDATE {DEALS_TABLE}
            SET amt_usd = {amt_usd}
            WHERE time = '{deal_time}' AND instrument = '{instrument}'
        """
        cur.execute(update_sql)


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
            print(f"Processed {len(deals)%100}/{len(deals)} deals")
            conn.commit()

        # Update amt_usd after all slices are committed
        print("Updating amt_usd")
        with conn.cursor() as cur:
            for idx, deal in enumerate(deals):
                _update_amt_usd(cur, deal)
                if (idx + 1) % 100 == 0:
                    conn.commit()
            conn.commit()

    print(f"Done processing {date_str}")


if __name__ == "__main__":
    for date_str in ["2025-10-20", "2025-10-21", "2025-10-22",
                    "2025-10-23", "2025-10-24", "2025-10-25",
                    "2025-10-26", "2025-10-27", "2025-10-28",
                    "2025-10-29", "2025-10-30"]:
    # for date_str in ["2025-10-20"]:
        _update(date_str)

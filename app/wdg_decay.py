sql = """
    SELECT time, instrument, side, amt, px, orderKind, orderType, tif, orderStatus
    FROM deals
    WHERE time BETWEEN %s AND %s
    ORDER BY time
"""

sql2 = f"""
    SELECT ts_server, instrument,
            last(ask_px_0) as ask_px_0,
            last(bid_px_0) as bid_px_0
    FROM {table_name}
    WHERE ts_server BETWEEN %s AND %s
        AND instrument IN ({placeholders})
    SAMPLE BY {resample} ALIGN TO CALENDAR FILL(LAST)
"""

populate_deals = """
INSERT INTO mart_kraken_decay_deals (time, instrument, side, amt, px, orderKind, orderType, tif, orderStatus, amt_usd)
SELECT time, instrument, side, amt, px, orderKind, orderType, tif, orderStatus, NULL
FROM deals;
"""

populate_slices = """ 
INSERT INTO mart_kraken_decay_slices (time, instrument, t_from_deal, ask_px_0, bid_px_0, usd_ask_px_0, usd_bid_px_0)
SELECT
    d.time,
    d.instrument,
    CAST(DATEDIFF('s', d.time, p.ts_server) AS INT) AS t_from_deal,
    p.ask_px_0,
    p.bid_px_0,
    NULL AS usd_ask_px_0,
    NULL AS usd_bid_px_0
FROM mart_kraken_decay_deals d
ASOF JOIN (
    SELECT ts_server, instrument, last(ask_px_0) AS ask_px_0, last(bid_px_0) AS bid_px_0
    FROM feed_kraken_tob_5
    SAMPLE BY 1s ALIGN TO CALENDAR
) p ON d.instrument = p.instrument
WHERE p.ts_server BETWEEN DATEADD('m', -15, d.time) AND DATEADD('m', 15, d.time);
"""
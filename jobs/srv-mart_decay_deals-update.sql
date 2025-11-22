-- Copy deals from deals table to mart_kraken_decay_deals
-- Date range: 2025-10-20 to 2025-10-30

INSERT INTO mart_kraken_decay_deals (time, instrument, side, amt, px, orderKind, orderType, tif, orderStatus, amt_usd)
SELECT
    time,
    instrument,
    side,
    amt,
    px,
    orderKind,
    orderType,
    tif,
    orderStatus,
    NULL AS amt_usd  -- will be populated by srv-mart_decay_slices-update.py
FROM deals
WHERE time BETWEEN '2025-10-20T00:00:00.000000Z' AND '2025-10-30T23:59:59.999999Z';
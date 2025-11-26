-- ============================================================================
-- Resample Deals to 1-minute buckets with weighted average fill prices
-- ============================================================================
-- For each 1m bucket, calculates:
-- - px_buy_wavg: weighted average price of buy trades (weighted by amt)
-- - px_sell_wavg: weighted average price of sell trades (weighted by amt)
-- ============================================================================

WITH buys AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_buy_wavg,
        SUM(amt) AS buy_amt,
        COUNT(*) AS buy_count
    FROM deals
    WHERE side = 'BUY'
      AND time BETWEEN '2025-10-20T00:00:00.000000Z' AND '2025-10-30T23:59:59.999999Z'
    SAMPLE BY 1m ALIGN TO CALENDAR
),
sells AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_sell_wavg,
        SUM(amt) AS sell_amt,
        COUNT(*) AS sell_count
    FROM deals
    WHERE side = 'SELL'
      AND time BETWEEN '2025-10-20T00:00:00.000000Z' AND '2025-10-30T23:59:59.999999Z'
    SAMPLE BY 1m ALIGN TO CALENDAR
)
SELECT
    COALESCE(b.ts_1m, s.ts_1m) AS ts_1m,
    COALESCE(b.instrument, s.instrument) AS instrument,
    b.px_buy_wavg,
    s.px_sell_wavg,
    COALESCE(b.buy_amt, 0) AS buy_amt,
    COALESCE(s.sell_amt, 0) AS sell_amt,
    COALESCE(b.buy_amt, 0) - COALESCE(s.sell_amt, 0) AS amt_filled,
    LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS amt_matched,
    (COALESCE(s.px_sell_wavg, 0) - COALESCE(b.px_buy_wavg, 0)) * LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS rpnl,
    COALESCE(b.buy_count, 0) + COALESCE(s.sell_count, 0) AS num_deals
FROM buys b
FULL OUTER JOIN sells s
    ON b.ts_1m = s.ts_1m
    AND b.instrument = s.instrument
ORDER BY ts_1m, instrument;

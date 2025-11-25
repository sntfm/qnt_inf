-- Aggregate deals to 1m buckets, join to feed_kraken_1m, calculate volume_usd, then cum_volume
-- side_int: buy=1, sell=-1
-- amt is multiplied by side_int
-- volume and volume_usd are multiplied by side_int*-1

WITH deals_1m AS (
    SELECT
        CAST(time AS TIMESTAMP) AS ts_1m,
        instrument,
        SUM(CASE WHEN side = 'buy' THEN amt ELSE -amt END) AS net_amt,
        SUM(CASE WHEN side = 'buy' THEN amt * px ELSE -amt * px END) AS net_volume,
        SUM(amt * px) / SUM(amt) AS avg_px,
        COUNT(*) AS num_deals
    FROM deals
    WHERE time BETWEEN '2025-10-28T00:00:00.000000Z' AND '2025-10-30T23:59:59.999999Z'
    SAMPLE BY 1m ALIGN TO CALENDAR
),
feed_with_deals AS (
    SELECT
        f.ts,
        f.instrument,
        f.ask_px_0,
        f.bid_px_0,
        d.ts_1m,
        COALESCE(d.net_amt, 0) AS amt_signed,
        COALESCE(d.net_volume, 0) AS volume_signed,
        d.avg_px,
        COALESCE(d.num_deals, 0) AS num_deals,
        -- Get USD conversion info
        c.usd_instrument,
        c.is_inverted
    FROM feed_kraken_1m f
    LEFT JOIN deals_1m d ON f.ts = d.ts_1m AND f.instrument = d.instrument
    LEFT JOIN convmap_usd c
        ON f.instrument = c.instrument
    WHERE f.ts BETWEEN '2025-10-28T00:00:00.000000Z' AND '2025-10-30T23:59:59.999999Z'
),
deals_with_usd AS (
    SELECT
        dwf.ts,
        dwf.instrument,
        dwf.amt_signed,
        dwf.avg_px,
        dwf.num_deals,
        dwf.ask_px_0,
        dwf.bid_px_0,
        -- Calculate USD conversion rates for ask and bid prices
        CASE
            -- No USD conversion needed (already in USD or no mapping)
            WHEN dwf.usd_instrument IS NULL THEN dwf.ask_px_0
            -- Inverted conversion (e.g., EUR/USD where we need 1/price)
            WHEN dwf.is_inverted THEN dwf.ask_px_0 / u.bid_px_0
            -- Direct conversion (e.g., BTC priced in EUR, multiply by EUR/USD)
            ELSE dwf.ask_px_0 * u.ask_px_0
        END AS ask_px_0_usd,
        CASE
            -- No USD conversion needed (already in USD or no mapping)
            WHEN dwf.usd_instrument IS NULL THEN dwf.bid_px_0
            -- Inverted conversion (e.g., EUR/USD where we need 1/price)
            WHEN dwf.is_inverted THEN dwf.bid_px_0 / u.ask_px_0
            -- Direct conversion (e.g., BTC priced in EUR, multiply by EUR/USD)
            ELSE dwf.bid_px_0 * u.bid_px_0
        END AS bid_px_0_usd,
        -- Calculate avg_px in USD
        CASE
            -- No USD conversion needed (already in USD or no mapping)
            WHEN dwf.usd_instrument IS NULL THEN dwf.avg_px
            -- Inverted conversion (e.g., EUR/USD where we need 1/price)
            WHEN dwf.is_inverted THEN dwf.avg_px / u.bid_px_0
            -- Direct conversion (e.g., BTC priced in EUR, multiply by EUR/USD)
            ELSE dwf.avg_px * u.ask_px_0
        END AS avg_px_usd
    FROM feed_with_deals dwf
    LEFT JOIN feed_kraken_1m u
        ON dwf.usd_instrument = u.instrument AND dwf.ts = u.ts
)
SELECT
    ts,
    instrument,
    amt_signed,
    avg_px,
    avg_px_usd,
    num_deals,
    ask_px_0,
    bid_px_0,
    ask_px_0_usd,
    bid_px_0_usd,
    -- Calculate rpnl_usd: amt_signed * (mtm_price - avg_px_usd)
    -- mtm_price = ask_px_0_usd if amt_signed < 0, otherwise bid_px_0_usd
    amt_signed * (CASE WHEN amt_signed < 0 THEN ask_px_0_usd ELSE bid_px_0_usd END - avg_px_usd) AS rpnl_usd
FROM deals_with_usd
ORDER BY ts;

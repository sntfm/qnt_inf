WITH
-- Load all feed rows up to end of target day
feed_all AS (
    SELECT *
    FROM {SOURCE_FEED_TABLE}
    WHERE ts <= '{date_end}'
),

-- Bucket deals to 1min
deals_buy AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_buy,
        SUM(amt) AS amt_buy,
        COUNT(*) AS cnt_buy
    FROM {SOURCE_DEALS_TABLE}
    WHERE side = 'BUY'
      AND time <= '{date_end}'
    SAMPLE BY 1m ALIGN TO CALENDAR
),

deals_sell AS (
    SELECT
        time AS ts_1m,
        instrument,
        SUM(amt * px) / SUM(amt) AS px_sell,
        SUM(amt) AS amt_sell,
        COUNT(*) AS cnt_sell
    FROM {SOURCE_DEALS_TABLE}
    WHERE side = 'SELL'
      AND time <= '{date_end}'
    SAMPLE BY 1m ALIGN TO CALENDAR
),

-- Merge BUY/SELL buckets
rd AS (
    SELECT
        COALESCE(b.ts_1m, s.ts_1m) AS ts_1m,
        COALESCE(b.instrument, s.instrument) AS instrument,
        b.px_buy,
        s.px_sell,
        COALESCE(b.amt_buy, 0) AS amt_buy,
        COALESCE(s.amt_sell, 0) AS amt_sell,
        COALESCE(b.amt_buy, 0) - COALESCE(s.amt_sell, 0) AS amt_filled,
        LEAST(COALESCE(b.amt_buy, 0), COALESCE(s.amt_sell, 0)) AS amt_matched,
        (COALESCE(s.px_sell, 0) - COALESCE(b.px_buy, 0))
            * LEAST(COALESCE(b.amt_buy, 0), COALESCE(s.amt_sell, 0)) AS rpnl,
        COALESCE(b.cnt_buy, 0) + COALESCE(s.cnt_sell, 0) AS num_deals
    FROM deals_buy b
    FULL OUTER JOIN deals_sell s
        ON b.ts_1m = s.ts_1m
       AND b.instrument = s.instrument
),

-- Join FEED + DEALS + FX conversion
base AS (
    SELECT
        f.ts AS ts,
        f.instrument,
        c.is_major,
        c.instrument_base,
        c.instrument_quote,
        c.instrument_usd,
        c.inst_usd_is_inverted,
        COALESCE(rd.amt_filled, 0) AS amt_signed,
        COALESCE(rd.amt_buy, 0) AS amt_buy,
        COALESCE(rd.amt_sell, 0) AS amt_sell,
        COALESCE(rd.amt_matched, 0) AS amt_matched,
        rd.px_buy AS px_buy,
        rd.px_sell AS px_sell,
        COALESCE(rd.num_deals, 0) AS num_deals,
        -- Base instrument prices
        b.bid_px_0 AS px_bid_0_base,
        b.ask_px_0 AS px_ask_0_base,
        -- Quote instrument prices
        q.bid_px_0 AS px_bid_0_quote,
        q.ask_px_0 AS px_ask_0_quote,
        -- USD instrument prices
        u.bid_px_0 AS px_bid_0_usd,
        u.ask_px_0 AS px_ask_0_usd
    FROM feed_all f
    LEFT JOIN rd
        ON f.ts = rd.ts_1m
       AND f.instrument = rd.instrument
    LEFT JOIN {CONVMAP_TABLE} c
        ON f.instrument = c.instrument
    LEFT JOIN {SOURCE_FEED_TABLE} u
        ON f.ts = u.ts
       AND c.instrument_usd = u.instrument
    LEFT JOIN {SOURCE_FEED_TABLE} b
        ON f.ts = b.ts
       AND c.instrument_base = b.instrument
    LEFT JOIN {SOURCE_FEED_TABLE} q
        ON f.ts = q.ts
       AND c.instrument_quote = q.instrument
)

SELECT * FROM base;
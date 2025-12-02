##
-- WITH
-- -- ------------------------------------------------------------
-- -- Load all feed rows up to end of target day
-- -- ------------------------------------------------------------
-- feed_all AS (
--     SELECT *
--     FROM {SOURCE_FEED_TABLE}
--     WHERE ts <= '{date_end}'
-- ),

-- -- ------------------------------------------------------------
-- -- Load all 1m-deals up to end of target day
-- -- ------------------------------------------------------------
-- deals_buy AS (
--     SELECT
--         time AS ts_1m,
--         instrument,
--         SUM(amt * px) / SUM(amt) AS px_buy,
--         SUM(amt) AS amt_buy,
--         COUNT(*) AS cnt_buy
--     FROM {SOURCE_DEALS_TABLE}
--     WHERE side = 'BUY'
--       AND time <= '{date_end}'
--     SAMPLE BY 1m ALIGN TO CALENDAR
-- ),

-- deals_sell AS (
--     SELECT
--         time AS ts_1m,
--         instrument,
--         SUM(amt * px) / SUM(amt) AS px_sell,
--         SUM(amt) AS amt_sell,
--         COUNT(*) AS cnt_sell
--     FROM {SOURCE_DEALS_TABLE}
--     WHERE side = 'SELL'
--       AND time <= '{date_end}'
--     SAMPLE BY 1m ALIGN TO CALENDAR
-- ),

-- -- ------------------------------------------------------------
-- -- Merge BUY/SELL buckets
-- -- ------------------------------------------------------------
-- rd AS (
--     SELECT
--         COALESCE(b.ts_1m, s.ts_1m) AS ts_1m,
--         COALESCE(b.instrument, s.instrument) AS instrument,
--         b.px_buy,
--         s.px_sell,
--         COALESCE(b.amt_buy, 0) AS amt_buy,
--         COALESCE(s.amt_sell, 0) AS amt_sell,
--         COALESCE(b.buy_amt, 0) - COALESCE(s.sell_amt, 0) AS amt_filled,
--         LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS amt_matched,
--         (COALESCE(s.px_sell_wavg, 0) - COALESCE(b.px_buy_wavg, 0))
--             * LEAST(COALESCE(b.buy_amt, 0), COALESCE(s.sell_amt, 0)) AS rpnl,
--         COALESCE(b.buy_count, 0) + COALESCE(s.sell_count, 0) AS num_deals
--     FROM deals_buy b
--     FULL OUTER JOIN deals_sell s
--         ON b.ts_1m = s.ts_1m
--        AND b.instrument = s.instrument
-- ),

-- -- ------------------------------------------------------------
-- -- Join FEED + DEALS + FX conversion
-- -- ------------------------------------------------------------
-- base AS (
--     SELECT
--         f.ts AS ts,
--         f.instrument,
--         c.instrument_base,
--         c.instrument_quote,
--         c.instrument_usd,
--         c.inst_usd_is_inverted,
--         COALESCE(rd.amt_filled, 0) AS amt_signed,
--         COALESCE(rd.buy_amt, 0) AS buy_amt,
--         COALESCE(rd.sell_amt, 0) AS sell_amt,
--         COALESCE(rd.amt_matched, 0) AS matched_amt,
--         rd.px_buy_wavg AS wavg_buy_px,
--         rd.px_sell_wavg AS wavg_sell_px,
--         COALESCE(rd.num_deals, 0) AS num_deals,

        -- Effective execution price in native: use buy wavg for net buys, sell wavg for net sells
        -- CASE
        --     WHEN COALESCE(rd.amt_filled, 0) > 0 THEN rd.px_buy_wavg
        --     WHEN COALESCE(rd.amt_filled, 0) < 0 THEN rd.px_sell_wavg
        --     ELSE NULL
        -- END AS px,

        -- -- Decomposed execution prices for quote/base legs
        -- CASE
        --     -- Net long and non-inverted: px_quote = px / base ask
        --     WHEN COALESCE(rd.amt_filled, 0) > 0 AND NOT c.inst_usd_is_inverted
        --         THEN rd.px_buy_wavg / NULLIF(b.ask_px_0, 0)
        --     -- Net long and inverted: px_quote = px / base ask
        --     WHEN COALESCE(rd.amt_filled, 0) > 0 AND c.inst_usd_is_inverted
        --         THEN rd.px_buy_wavg * NULLIF(b.ask_px_0, 0)
        --     -- Net short and non-inverted: px_quote = px / base ask
        --     WHEN COALESCE(rd.amt_filled, 0) < 0 AND NOT c.inst_usd_is_inverted
        --         THEN rd.px_sell_wavg / NULLIF(b.ask_px_0, 0)
        --     -- Net short and inverted: px_quote = px / base ask
        --     WHEN COALESCE(rd.amt_filled, 0) < 0 AND c.inst_usd_is_inverted
        --         THEN rd.px_sell_wavg * NULLIF(b.ask_px_0, 0)
        --     ELSE NULL
        -- END AS px_quote,

        -- CASE
        --     -- Net long and non-inverted: px_base = px / quote bid
        --     WHEN COALESCE(rd.amt_filled, 0) > 0 AND NOT c.inst_usd_is_inverted
        --         THEN rd.px_buy_wavg / NULLIF(q.bid_px_0, 0)
        --     -- Net long and inverted: px_base = px / quote bid
        --     WHEN COALESCE(rd.amt_filled, 0) > 0 AND c.inst_usd_is_inverted
        --         THEN rd.px_buy_wavg * NULLIF(q.bid_px_0, 0)
        --     -- Net short and inverted: px_base = px / quote bid
        --     WHEN COALESCE(rd.amt_filled, 0) < 0 AND NOT c.inst_usd_is_inverted
        --         THEN rd.px_sell_wavg / NULLIF(q.bid_px_0, 0)
        --     -- Net short and inverted: px_base = px / quote bid
        --     WHEN COALESCE(rd.amt_filled, 0) < 0 AND c.inst_usd_is_inverted
        --         THEN rd.px_sell_wavg * NULLIF(q.bid_px_0, 0)
        --     ELSE NULL
        -- END AS px_base,
        -- f.ask_px_0,
        -- f.bid_px_0,

        -- -- Base/quote leg prices (for decomposition debugging / analytics)
        -- b.ask_px_0 AS base_ask_px_0,
        -- b.bid_px_0 AS base_bid_px_0,
        -- q.ask_px_0 AS quote_ask_px_0,
        -- q.bid_px_0 AS quote_bid_px_0,

        -- -- ASK/BID converted to USD
        -- CASE
        --     WHEN c.instrument_usd IS NULL THEN f.ask_px_0
        --     WHEN c.inst_usd_is_inverted THEN f.ask_px_0 / u.bid_px_0
        --     ELSE f.ask_px_0 * u.ask_px_0
        -- END AS ask_px_0_usd,

        -- CASE
        --     WHEN c.instrument_usd IS NULL THEN f.bid_px_0
        --     WHEN c.inst_usd_is_inverted THEN f.bid_px_0 / u.ask_px_0
        --     ELSE f.bid_px_0 * u.bid_px_0
        -- END AS bid_px_0_usd,

        -- rd.rpnl AS rpnl_native,

        -- Weighted average buy/sell prices converted to USD
        -- CASE
        --     WHEN c.instrument_usd IS NULL THEN rd.px_buy_wavg
        --     WHEN c.inst_usd_is_inverted THEN rd.px_buy_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
        --     ELSE rd.px_buy_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
        -- END AS wavg_buy_px_usd,

        -- CASE
        --     WHEN c.instrument_usd IS NULL THEN rd.px_sell_wavg
        --     WHEN c.inst_usd_is_inverted THEN rd.px_sell_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
        --     ELSE rd.px_sell_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
        -- END AS wavg_sell_px_usd,

        -- RPNL converted to USD
        CASE
            WHEN c.instrument_usd IS NULL THEN rd.rpnl
            WHEN c.inst_usd_is_inverted THEN rd.rpnl / ((u.ask_px_0 + u.bid_px_0) / 2)
            ELSE rd.rpnl * ((u.ask_px_0 + u.bid_px_0) / 2)
        END AS rpnl_usd,

        -- volume in USD: signed exposure change
        CASE
            WHEN amt_filled < 0 THEN amt_filled *
                (CASE
                    WHEN c.instrument_usd IS NULL THEN f.ask_px_0
                    WHEN c.inst_usd_is_inverted THEN f.ask_px_0 / u.bid_px_0
                    ELSE f.ask_px_0 * u.ask_px_0
                END)
            ELSE amt_filled *
                (CASE
                    WHEN c.instrument_usd IS NULL THEN f.bid_px_0
                    WHEN c.inst_usd_is_inverted THEN f.bid_px_0 / u.ask_px_0
                    ELSE f.bid_px_0 * u.bid_px_0
                END)
        END AS vol_usd,

        -- --------------------------------------------------------
        -- Correct cost basis: use actual fill prices, not market prices
        -- --------------------------------------------------------
        CASE
            WHEN rd.buy_amt > 0 AND rd.sell_amt > 0 THEN
                -- Both buys and sells: cost = buys * buy_px - sells * sell_px
                (rd.buy_amt *
                    CASE
                        WHEN c.instrument_usd IS NULL THEN rd.px_buy_wavg
                        WHEN c.inst_usd_is_inverted THEN rd.px_buy_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_buy_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
                ) -
                (rd.sell_amt *
                    CASE
                        WHEN c.instrument_usd IS NULL THEN rd.px_sell_wavg
                        WHEN c.inst_usd_is_inverted THEN rd.px_sell_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_sell_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
                )
            WHEN rd.buy_amt > 0 THEN
                -- Only buys
                rd.buy_amt *
                    CASE
                        WHEN c.instrument_usd IS NULL THEN rd.px_buy_wavg
                        WHEN c.inst_usd_is_inverted THEN rd.px_buy_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_buy_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
            WHEN rd.sell_amt > 0 THEN
                -- Only sells (negative cost)
                -(rd.sell_amt *
                    CASE
                        WHEN c.instrument_usd IS NULL THEN rd.px_sell_wavg
                        WHEN c.inst_usd_is_inverted THEN rd.px_sell_wavg / ((u.ask_px_0 + u.bid_px_0) / 2)
                        ELSE rd.px_sell_wavg * ((u.ask_px_0 + u.bid_px_0) / 2)
                    END
                )
            ELSE 0
        END AS cost_signed_usd

    FROM feed_all f
    -- LEFT JOIN rd
    --     ON f.ts = rd.ts_1m
    --    AND f.instrument = rd.instrument
    -- LEFT JOIN {CONVMAP_TABLE} c
    --     ON f.instrument = c.instrument
    -- LEFT JOIN {SOURCE_FEED_TABLE} u
    --     ON f.ts = u.ts
    --    AND c.instrument_usd = u.instrument
    -- LEFT JOIN {SOURCE_FEED_TABLE} b
    --     ON f.ts = b.ts
    --    AND c.instrument_base = b.instrument
    -- LEFT JOIN {SOURCE_FEED_TABLE} q
    --     ON f.ts = q.ts
    --    AND c.instrument_quote = q.instrument
),

-- ------------------------------------------------------------
-- Running cumulative sums (stateful PnL engine)
-- ------------------------------------------------------------
cum AS (
    SELECT
        *,
        SUM(amt_signed)       OVER (PARTITION BY instrument ORDER BY ts) AS cum_amt,
        SUM(cost_signed_usd)  OVER (PARTITION BY instrument ORDER BY ts) AS cum_cost_usd
    FROM base
),

-- ------------------------------------------------------------
-- Previous cumulative snapshot
-- ------------------------------------------------------------
lagged AS (
    SELECT
        *,
        LAG(cum_amt)       OVER (PARTITION BY instrument ORDER BY ts) AS prev_cum_amt,
        LAG(cum_cost_usd)  OVER (PARTITION BY instrument ORDER BY ts) AS prev_cum_cost_usd
    FROM cum
),

-- ------------------------------------------------------------
-- Compute realized PnL from reductions & flips
-- ------------------------------------------------------------
rpnl_calc AS (
    SELECT
        *,
        CASE
            -- First row or prev position was flat: only bucket rpnl
            WHEN prev_cum_amt IS NULL OR prev_cum_amt = 0 THEN rpnl_usd
            
            -- Position closed to zero or flipped sign: realize entire prev position
            WHEN cum_amt = 0 OR SIGN(prev_cum_amt) != SIGN(cum_amt)
            THEN
                rpnl_usd +
                prev_cum_amt *
                (
                    CASE
                        WHEN prev_cum_amt > 0 THEN bid_px_0_usd
                        ELSE ask_px_0_usd
                    END
                    -
                    (prev_cum_cost_usd / prev_cum_amt)
                )
            
            -- Position reduced (same sign, smaller absolute value)
            WHEN ABS(cum_amt) < ABS(prev_cum_amt)
            THEN
                rpnl_usd +
                (prev_cum_amt - cum_amt) *
                (
                    CASE
                        WHEN prev_cum_amt > 0 THEN bid_px_0_usd
                        ELSE ask_px_0_usd
                    END
                    -
                    (prev_cum_cost_usd / prev_cum_amt)
                )
            
            -- Position increased or stayed same: only bucket rpnl
            ELSE rpnl_usd
        END AS rpnl_usd_total
    FROM lagged
),

-- ------------------------------------------------------------
-- Final calculations: UPNL, TPNL, cumulative volume
-- ------------------------------------------------------------
final AS (
    SELECT
        ts,
        instrument,
        instrument_base,
        instrument_quote,
        instrument_usd,
        amt_signed,
        buy_amt,
        sell_amt,
        matched_amt,
        wavg_buy_px,
        wavg_sell_px,
        px,
        num_deals,
        ask_px_0,
        bid_px_0,
        base_ask_px_0,
        base_bid_px_0,
        quote_ask_px_0,
        quote_bid_px_0,
        ask_px_0_usd,
        bid_px_0_usd,
        rpnl_native AS rpnl,
        rpnl_usd_total AS rpnl_usd,
        vol_usd,
        cum_amt,
        SUM(vol_usd) OVER (PARTITION BY instrument ORDER BY ts) AS cum_vol_usd,
        SUM(rpnl_usd_total) OVER (PARTITION BY instrument ORDER BY ts) AS cum_rpnl_usd,

        -- Unrealized PnL: current position valued at market price minus cost basis
        CASE
            WHEN cum_amt = 0 THEN 0
            WHEN cum_amt > 0 THEN cum_amt * (bid_px_0_usd - (cum_cost_usd / cum_amt))
            ELSE cum_amt * (ask_px_0_usd - (cum_cost_usd / cum_amt))
        END AS upnl_usd,

        -- Unrealized PnL in base leg
        CASE
            WHEN cum_amt = 0 THEN 0
            WHEN cum_amt > 0 THEN cum_amt * (base_bid_px_0 - px_base)
            ELSE cum_amt * (base_ask_px_0 - px_base)
        END AS upnl_base,

        -- Unrealized PnL in quote leg
        CASE
            WHEN cum_amt = 0 THEN 0
            WHEN cum_amt > 0 THEN cum_amt * (quote_bid_px_0 - px_quote)
            ELSE cum_amt * (quote_ask_px_0 - px_quote)
        END AS upnl_quote,

        -- Total PnL in quote leg: realized (native) + unrealized in quote
        (rpnl_native + 
            CASE
                WHEN cum_amt = 0 THEN 0
                WHEN cum_amt > 0 THEN cum_amt * (quote_bid_px_0 - px_quote)
                ELSE cum_amt * (quote_ask_px_0 - px_quote)
            END
        ) AS tpnl_quote
    FROM rpnl_calc
)

-- INSERT ONLY TARGET DAY RESULTS
-- ------------------------------------------------------------
INSERT INTO {MART_TABLE} (
    ts,
    instrument,
    instrument_base,
    instrument_quote,
    instrument_usd,
    amt_signed,
    buy_amt,
    sell_amt,
    matched_amt,
    wavg_buy_px,
    wavg_sell_px,
    px,
    num_deals,
    ask_px_0,
    bid_px_0,
    base_ask_px_0,
    base_bid_px_0,
    quote_ask_px_0,
    quote_bid_px_0,
    ask_px_0_usd,
    bid_px_0_usd,
    rpnl,
    rpnl_usd,
    vol_usd,
    cum_amt,
    cum_vol_usd,
    cum_rpnl_usd,
    upnl_usd,
    tpnl_usd,
    upnl_base,
    upnl_quote,
    tpnl_quote
)
SELECT
    ts,
    instrument,
    instrument_base,
    instrument_quote,
    instrument_usd,
    amt_signed,
    buy_amt,
    sell_amt,
    matched_amt,
    wavg_buy_px,
    wavg_sell_px,
    px,
    num_deals,
    ask_px_0,
    bid_px_0,
    base_ask_px_0,
    base_bid_px_0,
    quote_ask_px_0,
    quote_bid_px_0,
    ask_px_0_usd,
    bid_px_0_usd,
    rpnl,
    rpnl_usd,
    vol_usd,
    cum_amt,
    cum_vol_usd,
    cum_rpnl_usd,
    upnl_usd,
    cum_rpnl_usd + upnl_usd AS tpnl_usd,
    upnl_base,
    upnl_quote,
    tpnl_quote
FROM final
WHERE ts BETWEEN '{date_start}' AND '{date_end}';

    """
CREATE TABLE mart_kraken_latency (
    ts TIMESTAMP,
    date SYMBOL,
    hour INT,
    latency_bin_start_ms DOUBLE,
    bin_count LONG
) TIMESTAMP(ts) PARTITION BY MONTH;

CREATE TABLE mart_kraken_latency_stats (
    ts TIMESTAMP,
    date SYMBOL,
    mean_ms DOUBLE,
    std_ms DOUBLE,
    median_ms DOUBLE,
    p99_ms DOUBLE,
    sample_count LONG
) TIMESTAMP(ts) PARTITION BY YEAR;

CREATE TABLE IF NOT EXISTS convmap_usd (
    instrument SYMBOL,
    usd_instrument SYMBOL,
    is_inverted BOOLEAN
);

-- Deals slices table: for each deal, stores price snapshots within lookup_window
-- t_from_deal is seconds offset from deal time (negative = before, 0 = at deal, positive = after)
CREATE TABLE mart_kraken_decay_slices (
    time TIMESTAMP,             -- time of the deal
    instrument SYMBOL,               -- deal instrument
    t_from_deal INT,                 -- seconds offset from deal time
    ask_px_0 DOUBLE,                 -- ask price at this time
    bid_px_0 DOUBLE,                 -- bid price at this time
    usd_ask_px_0 DOUBLE,             -- USD conversion ask price (if applicable)
    usd_bid_px_0 DOUBLE,             -- USD conversion bid price (if applicable)
    ret DOUBLE,                   -- return: BUY=(bid-entry)/entry, SELL=(entry-ask)/entry
    pnl_usd DOUBLE                   -- PnL in USD: BUY=bid*amt*usd_bid - entry_usd, SELL=entry_usd - ask*amt*usd_ask
) TIMESTAMP(time) PARTITION BY MONTH;

CREATE TABLE mart_kraken_decay_deals (
    time TIMESTAMP,
    instrument SYMBOL,
    side SYMBOL,
    amt DOUBLE,
    px DOUBLE,
    orderKind SYMBOL,
    orderType SYMBOL,
    tif SYMBOL,
    orderStatus SYMBOL,
    amt_usd DOUBLE                   -- amt * px * usd_conversion_rate
) TIMESTAMP(time) PARTITION BY MONTH;

CREATE TABLE feed_kraken_1s (
    ts TIMESTAMP,
    instrument SYMBOL,
    ask_px_0 DOUBLE,
    bid_px_0 DOUBLE
) TIMESTAMP(ts) PARTITION BY MONTH;

CREATE TABLE mart_pnl_flow (
    ts TIMESTAMP,
    instrument SYMBOL,
    amt_signed DOUBLE,
    avg_px DOUBLE,
    avg_px_usd DOUBLE,
    volume_usd DOUBLE,           -- notional value of this bucket: amt_signed * avg_px_usd
    num_deals INT,
    ask_px_0 DOUBLE,
    bid_px_0 DOUBLE,
    ask_px_0_usd DOUBLE,
    bid_px_0_usd DOUBLE,
    rpnl_usd DOUBLE,
    cum_amt DOUBLE,              -- cumulative position (running sum of amt_signed)
    cum_volume_usd DOUBLE,       -- cumulative notional value: cum_amt * mtm_price
    prev_cum_volume_usd DOUBLE,  -- previous bucket's cumulative notional value
    upnl_usd DOUBLE              -- unrealized PnL: (cum_volume_usd / prev_cum_volume_usd - 1) * prev_cum_volume_usd (zero at inception)
) TIMESTAMP(ts) PARTITION BY MONTH;
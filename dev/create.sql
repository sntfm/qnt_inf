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

CREATE TABLE IF NOT EXISTS mart_pnl_flow (
    ts TIMESTAMP,
    instrument SYMBOL,
    instrument_base SYMBOL,
    instrument_quote SYMBOL,
    amt_filled DOUBLE,
    px DOUBLE,
    px_quote DOUBLE,
    px_base DOUBLE,
    vol_usd DOUBLE,
    num_deals INT,
    cum_amt DOUBLE,
    cum_cost_usd DOUBLE,
    cum_cost_base DOUBLE,
    cum_cost_quote DOUBLE,
    cum_cost_native DOUBLE,
    cum_quote_amt DOUBLE,
    cum_vol_usd DOUBLE,
    rpnl_usd_total DOUBLE,
    cum_rpnl_usd DOUBLE,
    cum_rpnl_quote DOUBLE,
    upnl_usd DOUBLE,
    upnl_base DOUBLE,
    upnl_quote DOUBLE,
    tpnl_usd DOUBLE,
    tpnl_quote DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;

CREATE TABLE map_decomposition_usd (
    instrument SYMBOL,
    is_major BOOLEAN,
    instrument_base SYMBOL,
    instrument_quote SYMBOL,
    instrument_usd SYMBOL,
    inst_usd_is_inverted BOOLEAN
);
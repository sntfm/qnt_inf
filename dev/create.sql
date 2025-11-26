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
    amt_signed DOUBLE,           -- net amount: buy_amt - sell_amt
    buy_amt DOUBLE,              -- total buy amount in this bucket
    sell_amt DOUBLE,             -- total sell amount in this bucket
    matched_amt DOUBLE,          -- min(buy_amt, sell_amt) - portion that offsets within bucket
    wavg_buy_px DOUBLE,          -- weighted average buy price (native currency)
    wavg_sell_px DOUBLE,         -- weighted average sell price (native currency)
    num_deals INT,               -- number of deals in this bucket
    ask_px_0 DOUBLE,             -- market ask price at bucket time (native currency)
    bid_px_0 DOUBLE,             -- market bid price at bucket time (native currency)
    ask_px_0_usd DOUBLE,         -- market ask price in USD
    bid_px_0_usd DOUBLE,         -- market bid price in USD
    rpnl DOUBLE,                 -- realized PnL in native currency: matched_amt * (wavg_sell_px - wavg_buy_px)
    rpnl_usd DOUBLE,             -- realized PnL in USD: rpnl * usd_conversion_rate
    vol_usd DOUBLE,              -- volume in USD: amt_signed * (bid_px_0_usd if amt>0 else ask_px_0_usd)
    cum_amt DOUBLE,              -- cumulative amount (running sum of amt_signed per instrument)
    cum_vol_usd DOUBLE,          -- cumulative volume in USD (running sum of vol_usd per instrument)
    upnl_usd DOUBLE,             -- unrealized PnL in USD: cum_amt * (exit_px_usd - abs(cum_vol_usd/cum_amt))
    tpnl_usd DOUBLE              -- total PnL in USD: rpnl_usd + upnl_usd
) TIMESTAMP(ts) PARTITION BY MONTH;
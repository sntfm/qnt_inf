CREATE TABLE mart_{}_latency (
    ts TIMESTAMP,
    date SYMBOL,
    hour INT,
    latency_bin_start_ms DOUBLE,
    bin_count LONG
) TIMESTAMP(ts) PARTITION BY MONTH;

CREATE TABLE mart_{}_latency_stats (
    ts TIMESTAMP,
    date SYMBOL,
    mean_ms DOUBLE,
    std_ms DOUBLE,
    median_ms DOUBLE,
    p99_ms DOUBLE,
    sample_count LONG
) TIMESTAMP(ts) PARTITION BY YEAR;
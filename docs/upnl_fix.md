# UPNL Calculation Fix - Both Base and Quote in USD

## Problem
Previously, `upnl_base` and `upnl_quote` were not both expressed in USD terms:
- `upnl_base` was correctly in USD
- `upnl_quote` was in base currency units (e.g., ETH), not USD

## Root Cause
The old calculation was:
```python
df.loc[curr_long, 'upnl_quote'] = df['cum_amt'] * (df['px_bid_0_quote'] - df['px_quote'])
```

For ETH/EUR with position of 0.04 ETH:
- `cum_amt` = 0.04 ETH
- `px_bid_0_quote` = current EUR/USD (e.g., 1.163)
- `px_quote` = entry EUR/USD (e.g., 1.163)
- Result: `upnl_quote` in ETH units, not USD!

## Solution
The quote leg UPNL must account for the quote currency position size.

For a long position in ETH/EUR:
- You're **long ETH** (base)
- You're **short EUR** (quote)

The EUR position size = `cum_amt * avg_native_price` (in EUR)
Or equivalently: `cum_cost_usd / entry_eur_usd_price`

### New Formula
```python
# Quote currency amount at entry (in quote currency units)
quote_curr_amount = cum_cost_usd / entry_quote_usd_price

# UPNL from quote leg (in USD)
upnl_quote = -quote_curr_amount * (current_quote_usd - entry_quote_usd)
           = -(cum_cost_usd / entry_quote_usd) * (current_quote_usd - entry_quote_usd)
```

The negative sign is because:
- Long base → Short quote
- Short base → Long quote

## Example
Buy 0.04 ETH at 3381.645 EUR when EUR/USD = 1.16297

**Position:**
- Long 0.04 ETH
- Short 135.27 EUR (= 0.04 * 3381.645)

**Entry:**
- `cum_cost_usd` = 157.28 USD
- `entry_quote_usd` = 1.16297

**Later when EUR/USD = 1.16294:**
- Quote amount = 157.28 / 1.16297 = 135.27 EUR
- `upnl_quote` = -135.27 * (1.16294 - 1.16297)
- `upnl_quote` = -135.27 * (-0.00003)
- `upnl_quote` = **0.00406 USD** ✓

## Implementation
```python
# Forward-fill px_quote for non-trade rows
px_quote_filled = df['px_quote'].replace(0, np.nan).groupby(df['instrument']).ffill()

# Avoid division by zero
safe_px_quote = np.where(px_quote_filled == 0, np.nan, px_quote_filled)

# For long: short quote currency
df.loc[curr_long, 'upnl_quote'] = -(quote_amount_usd / safe_px_quote) * (df['px_bid_0_quote'] - px_quote_filled)

# For short: long quote currency  
df.loc[curr_short, 'upnl_quote'] = -(quote_amount_usd / safe_px_quote) * (df['px_ask_0_quote'] - px_quote_filled)
```

## Result
Now both `upnl_base` and `upnl_quote` are expressed in **USD**, making them directly comparable and summable.

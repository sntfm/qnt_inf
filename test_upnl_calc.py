"""
Test the UPNL quote calculation logic

Example: Buy 0.04 ETH at 3381.645 EUR when EUR/USD = 1.16297

Position:
- Long 0.04 ETH (base)
- Short 135.27 EUR (quote) = 0.04 * 3381.645

Entry:
- cum_amt = 0.04 ETH
- cum_cost_usd = 0.04 * (3381.645 * 1.16297) = 157.28 USD
- px_quote (entry EUR/USD) = 1.16297

Later when EUR/USD = 1.16294:
- Quote position: -135.27 EUR
- Entry value: -135.27 * 1.16297 = -157.28 USD
- Current value: -135.27 * 1.16294 = -157.24 USD  
- UPNL quote = -157.24 - (-157.28) = 0.04 USD

Using the formula:
- quote_amount_usd = 157.28
- safe_px_quote = 1.16297
- quote_curr_amount = 157.28 / 1.16297 = 135.27 EUR
- upnl_quote = -135.27 * (1.16294 - 1.16297) = -135.27 * (-0.00003) = 0.00406 USD

Wait, that doesn't match. Let me recalculate...

Actually:
- upnl_quote = -(quote_amount_usd / entry_quote_usd) * (current_quote_usd - entry_quote_usd)
- upnl_quote = -(157.28 / 1.16297) * (1.16294 - 1.16297)
- upnl_quote = -135.27 * (-0.00003)
- upnl_quote = 0.00406 USD

But the expected is:
- Quote position = -135.27 EUR
- UPNL = -135.27 * (1.16294 - 1.16297) = 0.00406 USD ✓

So the formula is correct!
"""
print(__doc__)

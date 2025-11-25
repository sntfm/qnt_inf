# Flow Widget Implementation

## Overview
The Flow widget displays aggregate trading metrics for a selected date range:
- **PnL in USD**: Total profit/loss across all deals
- **Volume in USD**: Total trading volume (sum of amt_usd)
- **Quantity of Deals**: Number of deals executed

## Features

### Date Range Selection
- Reuses the date range selection pattern from `decay.py`
- Supports both date (`YYYY-MM-DD`) and datetime (`YYYY-MM-DD HH:MM:SS`) formats
- Default range: Last 7 days of available data

### Metrics Display
The widget shows three synchronized time-series plots:

1. **PnL (USD)** - Green line
   - Shows daily profit/loss
   - Includes zero reference line
   - Uses final PnL value for each deal (max t_from_deal)

2. **Volume (USD)** - Blue line
   - Shows daily trading volume
   - Sum of amt_usd from all deals

3. **Number of Deals** - Red line
   - Shows count of deals per day
   - Distinct count by (time, instrument)

### Summary Statistics
The status bar displays:
- Number of days loaded
- Total PnL across all days
- Total volume across all days
- Total number of deals

## Data Source

### Tables Used
- `mart_kraken_decay_deals`: Deal information (time, instrument, amt_usd, side)
- `mart_kraken_decay_slices`: PnL slices (pnl_usd at different t_from_deal values)

### SQL Query
The widget uses a single optimized SQL query that:
1. Aggregates deals by day
2. Joins with slices to get final PnL (using ROW_NUMBER to get max t_from_deal)
3. Computes daily totals for PnL, volume, and deal count

## Integration

### Files Modified
1. **`/app/widgets/flow.py`** (new file)
   - Widget layout and data fetching logic
   - `get_widget_layout()`: Creates the UI
   - `_fetch_flow_metrics()`: Fetches aggregated metrics from database

2. **`/app/app.py`** (modified)
   - Added flow widget import
   - Added flow widget container to main layout
   - Added initialization callback
   - Added load button callback to fetch and plot data

### Callbacks
- `initialize_flow_widget()`: Loads widget layout on page load
- `load_flow_data()`: Fetches metrics and updates graph when "Load Data" button is clicked

## Usage

1. Navigate to the app at `/app`
2. Scroll to the "Flow Metrics" section
3. Adjust the date range (default is last 7 days)
4. Click "Load Data" to fetch and display metrics
5. Hover over the plots to see detailed values
6. The status bar shows summary statistics

## Design Choices

### Why 3 Separate Subplots?
- Different scales: PnL can be negative, volume is always positive, deal count is integer
- Easier to read trends when each metric has its own y-axis
- Follows best practices for multi-metric dashboards

### Why Daily Aggregation?
- Provides clear overview of trading activity over time
- Reduces data volume for faster loading
- Matches typical analysis patterns (daily P&L review)

### Color Scheme
- **Green** (#2ecc71): PnL - traditional color for profit/loss
- **Blue** (#3498db): Volume - neutral, professional
- **Red** (#e74c3c): Deal count - attention-grabbing for activity level

## Future Enhancements

Potential improvements:
- Add filtering by instrument, side, order type, etc.
- Add hourly granularity option
- Add cumulative PnL view
- Add comparison with previous period
- Export data to CSV

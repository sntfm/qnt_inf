"""
Decay/PnL widget for Dash app.
Placeholder - to be implemented.
"""

from dash import html, dcc
import os
from datetime import date, datetime, time, timezone
from typing import List, Optional, Sequence, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------
# Maps instrument to (usd_conversion_instrument, is_inverted)
# is_inverted=True means we need to use 1/rate (e.g., USD/CHF -> CHF/USD = 1/rate)
USD_CONVERSION_MAP = {
    "Kraken.Spot.ADA/BTC_SPOT": ("Kraken.Spot.BTC/USD_SPOT", False),
    "Kraken.Spot.ADA/ETH_SPOT": ("Kraken.Spot.ETH/USD_SPOT", False),
    "Kraken.Spot.ADA/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.ADA/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.ADA/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.ADA/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.BCH/BTC_SPOT": ("Kraken.Spot.BTC/USD_SPOT", False),
    "Kraken.Spot.BCH/ETH_SPOT": ("Kraken.Spot.ETH/USD_SPOT", False),
    "Kraken.Spot.BCH/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.BCH/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.BCH/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.BCH/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.BTC/CHF_SPOT": ("Kraken.Spot.USD/CHF_SPOT", True),  # Use 1/rate
    "Kraken.Spot.BTC/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.BTC/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.BTC/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.BTC/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.DOGE/BTC_SPOT": ("Kraken.Spot.BTC/USD_SPOT", False),
    "Kraken.Spot.DOGE/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.DOGE/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.DOGE/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.DOGE/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.ETH/BTC_SPOT": ("Kraken.Spot.BTC/USD_SPOT", False),
    "Kraken.Spot.ETH/CHF_SPOT": ("Kraken.Spot.USD/CHF_SPOT", True),  # Use 1/rate
    "Kraken.Spot.ETH/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.ETH/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.ETH/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.ETH/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.EUR/CHF_SPOT": ("Kraken.Spot.USD/CHF_SPOT", True),  # Use 1/rate
    "Kraken.Spot.EUR/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),

    "Kraken.Spot.LTC/BTC_SPOT": ("Kraken.Spot.BTC/USD_SPOT", False),
    "Kraken.Spot.LTC/ETH_SPOT": ("Kraken.Spot.ETH/USD_SPOT", False),
    "Kraken.Spot.LTC/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.LTC/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.LTC/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.LTC/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.SOL/BTC_SPOT": ("Kraken.Spot.BTC/USD_SPOT", False),
    "Kraken.Spot.SOL/ETH_SPOT": ("Kraken.Spot.ETH/USD_SPOT", False),
    "Kraken.Spot.SOL/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.SOL/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.SOL/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.SOL/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.USD/CHF_SPOT": ("Kraken.Spot.USD/CHF_SPOT", True),  # USD/CHF -> CHF/USD = 1/rate
    "Kraken.Spot.USDC/CHF_SPOT": ("Kraken.Spot.USD/CHF_SPOT", True),  # Use 1/rate
    "Kraken.Spot.USDC/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.USDC/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.USDC/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),

    "Kraken.Spot.USDT/CHF_SPOT": ("Kraken.Spot.USD/CHF_SPOT", True),  # Use 1/rate
    "Kraken.Spot.USDT/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.USDT/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),

    "Kraken.Spot.XRP/BTC_SPOT": ("Kraken.Spot.BTC/USD_SPOT", False),
    "Kraken.Spot.XRP/ETH_SPOT": ("Kraken.Spot.ETH/USD_SPOT", False),
    "Kraken.Spot.XRP/EUR_SPOT": ("Kraken.Spot.EUR/USD_SPOT", False),
    "Kraken.Spot.XRP/GBP_SPOT": ("Kraken.Spot.GBP/USD_SPOT", False),
    "Kraken.Spot.XRP/USDC_SPOT": ("Kraken.Spot.USDC/USD_SPOT", False),
    "Kraken.Spot.XRP/USDT_SPOT": ("Kraken.Spot.USDT/USD_SPOT", False),
}


# ---------------------------------------------------------------------------
# QuestDB connection helpers
# ---------------------------------------------------------------------------
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")
VERBOSE = False

def _connect():
    """Create a new psycopg2 connection to QuestDB's Postgres endpoint."""
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DB,
        connect_timeout=30,
    )


def _run_query(sql: str, params: Sequence = ()) -> pd.DataFrame:
    """Execute a SQL query against QuestDB and return a pandas DataFrame."""
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        print("[DEBUG SQL]", cur.query.decode()) 
        rows = cur.fetchall()
    return pd.DataFrame(rows)

def _fetch_deals(date: str) -> pd.DataFrame:
    dt = datetime.strptime(date, '%Y-%m-%d')

    # Add timezone if not present
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)

    # Calculate start and end of day
    ts_start = dt
    ts_end = dt + pd.Timedelta(days=1)

    # Format timestamps for QuestDB (YYYY-MM-DDTHH:MM:SS.ffffffZ)
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    ts_start_str = ts_start.strftime(fmt)
    ts_end_str = ts_end.strftime(fmt)

    # Build SQL query
    sql = """
        SELECT time, instrument, side, amt, px, orderKind, orderType, tif, orderStatus
        FROM deals
        WHERE time BETWEEN %s AND %s
        ORDER BY time
    """

    # Execute query
    df = _run_query(sql, (ts_start_str, ts_end_str))

    # Convert time to datetime if data exists
    if not df.empty and 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], utc=True)

    return df

def _fetch_neighbors(instrument: str, ts_start: datetime, frame_mins: int = 15,
                        table_name: str = "feed_kraken_tob_5", resample: Optional[str] = "1s") -> pd.DataFrame:

    # Calculate time window
    ts_before = ts_start - pd.Timedelta(minutes=frame_mins)
    ts_after = ts_start + pd.Timedelta(minutes=frame_mins)

    # Format timestamps for QuestDB (YYYY-MM-DDTHH:MM:SS.ffffffZ)
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    ts_before_str = ts_before.strftime(fmt)
    ts_after_str = ts_after.strftime(fmt)

    # Check if we need USD conversion prices
    usd_conversion = USD_CONVERSION_MAP.get(instrument)
    usd_instrument = None
    is_inverted = False
    if usd_conversion:
        usd_instrument, is_inverted = usd_conversion

    # Build list of instruments to fetch
    instruments_to_fetch = [instrument]
    if usd_instrument:
        instruments_to_fetch.append(usd_instrument)
        print(f"[DEBUG] Fetching USD conversion: {instrument} -> {usd_instrument} (inverted={is_inverted})")

    # Build SQL query with optional resampling
    placeholders = ', '.join(['%s'] * len(instruments_to_fetch))
    if resample is not None:
        sql = f"""
            SELECT ts_server, instrument,
                   last(ask_px_0) as ask_px_0,
                   last(bid_px_0) as bid_px_0
            FROM {table_name}
            WHERE ts_server BETWEEN %s AND %s
              AND instrument IN ({placeholders})
            SAMPLE BY {resample}
            ALIGN TO CALENDAR
        """
    else:
        sql = f"""
            SELECT ts_server, instrument, ask_px_0, bid_px_0
            FROM {table_name}
            WHERE ts_server BETWEEN %s AND %s
              AND instrument IN ({placeholders})
            ORDER BY ts_server
        """

    # Execute query
    params = (ts_before_str, ts_after_str, *instruments_to_fetch)
    df = _run_query(sql, params)

    if df.empty:
        return df

    # Convert ts_server to datetime
    df['ts_server'] = pd.to_datetime(df['ts_server'], utc=True)

    # Split into main instrument and USD conversion instrument
    main_df = df[df['instrument'] == instrument].copy()

    if main_df.empty:
        return pd.DataFrame()

    # Sort by timestamp to ensure correct ordering
    main_df = main_df.sort_values('ts_server').reset_index(drop=True)

    # Find the index closest to ts_start (where t_from_deal = 0)
    time_diffs = (main_df['ts_server'] - ts_start).abs()
    center_idx = time_diffs.idxmin()

    # Build t_from_deal: negative before ts_start, 0 at ts_start, positive after
    main_df['t_from_deal'] = main_df.index - center_idx

    # Add USD prices
    if usd_instrument and usd_instrument in df['instrument'].values:
        print(f"[DEBUG] Found USD instrument {usd_instrument} in data (inverted={is_inverted})")
        # Fetch USD conversion data and merge
        usd_df = df[df['instrument'] == usd_instrument].copy()
        usd_df = usd_df.sort_values('ts_server').reset_index(drop=True)

        if is_inverted:
            # For inverted pairs (e.g., USD/CHF), we need 1/rate to get CHF/USD
            # USD/CHF ask (to buy USD with CHF) -> CHF/USD bid (to sell CHF for USD) = 1/ask
            # USD/CHF bid (to sell USD for CHF) -> CHF/USD ask (to buy CHF with USD) = 1/bid
            usd_df['usd_ask_px_0'] = 1.0 / usd_df['bid_px_0']  # Inverted: ask becomes 1/bid
            usd_df['usd_bid_px_0'] = 1.0 / usd_df['ask_px_0']  # Inverted: bid becomes 1/ask
            print(f"[DEBUG] Inverted USD prices - sample: ask={usd_df['usd_ask_px_0'].iloc[0]:.4f}, bid={usd_df['usd_bid_px_0'].iloc[0]:.4f}")
        else:
            usd_df = usd_df.rename(columns={
                'ask_px_0': 'usd_ask_px_0',
                'bid_px_0': 'usd_bid_px_0'
            })

        usd_df = usd_df.drop(columns=['instrument', 'ask_px_0', 'bid_px_0'], errors='ignore')

        # Merge on ts_server using asof merge (nearest timestamp)
        main_df = pd.merge_asof(
            main_df.sort_values('ts_server'),
            usd_df.sort_values('ts_server'),
            on='ts_server',
            direction='nearest'
        )
        main_df = main_df.sort_values('t_from_deal').reset_index(drop=True)
    else:
        # Check if instrument is already USD-denominated (quote currency is USD)
        # e.g., BTC/USD_SPOT -> quote is USD, so conversion rate is 1.0
        is_usd_denominated = '/USD_SPOT' in instrument or '/USD' in instrument.split('_')[0]

        if is_usd_denominated:
            # Already in USD - use 1.0 as conversion rate
            print(f"[DEBUG] {instrument} is USD-denominated, using 1.0 as conversion rate")
            main_df['usd_ask_px_0'] = 1.0
            main_df['usd_bid_px_0'] = 1.0
        elif usd_instrument:
            # USD conversion instrument was expected but not found
            available = df['instrument'].unique().tolist()
            print(f"[DEBUG] WARNING: USD instrument {usd_instrument} NOT found!")
            print(f"[DEBUG]   Query returned only: {available}")
            # Fallback: use 1.0 to avoid incorrect multiplier
            main_df['usd_ask_px_0'] = 1.0
            main_df['usd_bid_px_0'] = 1.0
        else:
            # No USD conversion needed and not USD-denominated - shouldn't happen often
            print(f"[DEBUG] WARNING: {instrument} not in USD_CONVERSION_MAP and not USD-denominated")
            main_df['usd_ask_px_0'] = 1.0
            main_df['usd_bid_px_0'] = 1.0

    # Drop ts_server column
    main_df = main_df.drop(columns=['ts_server'])

    return main_df


def _fetch_single_neighbor(idx, deal, frame_mins, table_name, resample):
    """Helper function to fetch neighbors for a single deal (for parallel execution)."""
    instrument = deal['instrument']
    deal_time = deal['time']

    neighbors_df = _fetch_neighbors(
        instrument=instrument,
        ts_start=deal_time,
        frame_mins=frame_mins,
        table_name=table_name,
        resample=resample
    )

    return idx, neighbors_df


def _build_dataset(date: str, view: str, frame_mins: int = 15, table_name: str = "feed_kraken_tob_5",
                    resample: Optional[str] = "1s", max_workers: int = 10) -> Tuple[pd.DataFrame, dict]:

    # Fetch all deals for the date
    deals_df = _fetch_deals(date)

    if deals_df.empty:
        print(f"No deals found for {date}")
        return pd.DataFrame(), {}

    print(f"Found {len(deals_df)} deals for {date}")

    # Dictionary to store neighbor slices keyed by deal index
    slices_dict = {}

    # Fetch neighbors in parallel using ThreadPoolExecutor
    print(f"Fetching neighbor data using {max_workers} parallel workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_idx = {
            executor.submit(_fetch_single_neighbor, idx, deal, frame_mins, table_name, resample): idx
            for idx, deal in deals_df.iterrows()
        }

        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_idx):
            idx, neighbors_df = future.result()
            if not neighbors_df.empty:
                slices_dict[idx] = neighbors_df

            completed += 1
            if completed % 10 == 0:
                print(f"  Progress: {completed}/{len(deals_df)} deals processed")

    print(f"Built {len(slices_dict)} neighbor slices for {len(deals_df)} deals")

    for idx, row in deals_df.iterrows():
        deal_price = row['px']
        deal_side = row['side']
        deal_amt = row['amt']

        df = slices_dict[idx]

        if view == "return":
            if deal_side == "BUY":
                # BUY: (current sell price - entry price) / entry price
                df['ret'] = (df['bid_px_0'] - deal_price) / deal_price
            else:
                # SELL: (entry price - current buyback price) / entry price
                df['ret'] = (deal_price - df['ask_px_0']) / deal_price

        if view == "usd_pnl":
            # Get USD price at t=0 to convert deal price to USD
            t0_row = df[df['t_from_deal'] == 0]
            if not t0_row.empty:
                usd_px_at_t0 = t0_row['usd_ask_px_0'].iloc[0]
            else:
                usd_px_at_t0 = df['usd_ask_px_0'].iloc[0]  # fallback to first row

            # USD deal volume = price * amount * usd conversion rate
            deal_volume_usd = deal_price * deal_amt * usd_px_at_t0

            if deal_side == "BUY":
                # BUY: current sell value - entry cost = profit if price went up
                df['pnl_usd'] = df['bid_px_0'] * deal_amt * df['usd_bid_px_0'] - deal_volume_usd
            else:
                # SELL: entry proceeds - current buyback cost = profit if price went down
                df['pnl_usd'] = deal_volume_usd - df['ask_px_0'] * deal_amt * df['usd_ask_px_0']

        slices_dict[idx] = df

    # print(slices_dict)
    # Debug: print only BTC/CHF slices
    for idx, row in deals_df.iterrows():
        if 'BTC/CHF' in row['instrument']:
            print(f"\n[DEBUG] BTC/CHF Deal idx={idx}:")
            print(f"  deal_price={row['px']}, deal_amt={row['amt']}, side={row['side']}")
            if idx in slices_dict:
                df = slices_dict[idx]
                print(f"  usd_ask_px_0 at t=0: {df[df['t_from_deal']==0]['usd_ask_px_0'].values}")
                print(f"  usd_bid_px_0 at t=0: {df[df['t_from_deal']==0]['usd_bid_px_0'].values}")
                print(f"  bid_px_0 at t=0: {df[df['t_from_deal']==0]['bid_px_0'].values}")
                print(f"  ask_px_0 at t=0: {df[df['t_from_deal']==0]['ask_px_0'].values}")
                if 'pnl_usd' in df.columns:
                    print(f"  pnl_usd at t=0: {df[df['t_from_deal']==0]['pnl_usd'].values}")
                    print(f"  pnl_usd min/max: {df['pnl_usd'].min():.2f} / {df['pnl_usd'].max():.2f}")

    return deals_df, slices_dict


def get_widget_layout(n_intervals):
    """
    Decay/Markouts widget with filtering and plotting.

    Args:
        n_intervals: Number of intervals (from dcc.Interval component)

    Returns:
        Dash HTML layout with graph and filters
    """
    # Create initial empty figure
    fig = go.Figure()
    fig.update_layout(
        margin=dict(l=40, r=20, t=60, b=40),
        template="plotly_white",
        xaxis_title="Time from Deal (index steps)",
        yaxis_title="Mid Price",
        annotations=[
            dict(
                text="Load data to see decay plots",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=14, color="#7f8c8d"),
            )
        ],
    )

    # Right panel with filters
    right_panel = html.Div([
        # Date input
        html.Div([
            dcc.Input(
                id='decay-date-input',
                type='text',
                value='2025-10-28',
                placeholder='YYYY-MM-DD',
                style={
                    'width': '100%',
                    'height': '40px',
                    'padding': '8px',
                    'borderRadius': '6px',
                    'border': '1px solid #e0e0e0',
                    'marginBottom': '12px'
                }
            ),
        ]),

        # View type dropdown
        html.Div([
            html.Label("View:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50'}),
            dcc.Dropdown(
                id='decay-view-dropdown',
                options=[
                    {'label': 'Return (%)', 'value': 'return'},
                    {'label': 'USD PnL', 'value': 'usd_pnl'},
                ],
                value='return',
                clearable=False,
                style={'marginBottom': '12px'}
            ),
        ]),

        # Show legend checkbox
        html.Div([
            dcc.Checklist(
                id='decay-show-legend',
                options=[{'label': ' Show Legend', 'value': 'show'}],
                value=['show'],
                style={'fontSize': '13px', 'color': '#2c3e50'}
            ),
        ], style={'marginBottom': '12px'}),

        # Load button
        html.Button(
            'Load Data',
            id='decay-load-button',
            n_clicks=0,
            style={
                'width': '100%',
                'height': '40px',
                'backgroundColor': '#3498db',
                'color': '#fff',
                'border': 'none',
                'borderRadius': '6px',
                'fontWeight': '600',
                'cursor': 'pointer',
                'marginBottom': '12px'
            }
        ),

        # Refresh button
        html.Button(
            'Refresh Filters',
            id='decay-refresh-button',
            n_clicks=0,
            style={
                'width': '100%',
                'height': '40px',
                'backgroundColor': '#2ecc71',
                'color': '#fff',
                'border': 'none',
                'borderRadius': '6px',
                'fontWeight': '600',
                'cursor': 'pointer',
                'marginBottom': '20px'
            }
        ),

        html.Hr(style={'border': 'none', 'borderTop': '1px solid #e0e0e0', 'margin': '20px 0'}),

        # Collapsible Filters section
        html.Details([
            html.Summary("Filters", style={
                'fontWeight': '600',
                'color': '#2c3e50',
                'cursor': 'pointer',
                'padding': '8px 0',
                'marginBottom': '12px',
                'userSelect': 'none'
            }),

            # Instrument filter
            html.Div([
                html.Label("Instrument:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50', 'fontSize': '13px'}),
                dcc.Dropdown(
                    id='decay-instrument-filter',
                    options=[],
                    value=[],
                    multi=True,
                    placeholder='All instruments',
                    style={'marginBottom': '16px'}
                ),
            ]),

            # Side filter
            html.Div([
                html.Label("Side:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50', 'fontSize': '13px'}),
                dcc.Dropdown(
                    id='decay-side-filter',
                    options=[],
                    value=[],
                    multi=True,
                    placeholder='All sides',
                    style={'marginBottom': '16px'}
                ),
            ]),

            # Order Kind filter
            html.Div([
                html.Label("Order Kind:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50', 'fontSize': '13px'}),
                dcc.Dropdown(
                    id='decay-orderkind-filter',
                    options=[],
                    value=[],
                    multi=True,
                    placeholder='All kinds',
                    style={'marginBottom': '16px'}
                ),
            ]),

            # Order Type filter
            html.Div([
                html.Label("Order Type:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50', 'fontSize': '13px'}),
                dcc.Dropdown(
                    id='decay-ordertype-filter',
                    options=[],
                    value=[],
                    multi=True,
                    placeholder='All types',
                    style={'marginBottom': '16px'}
                ),
            ]),

            # TIF filter
            html.Div([
                html.Label("Time In Force:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50', 'fontSize': '13px'}),
                dcc.Dropdown(
                    id='decay-tif-filter',
                    options=[],
                    value=[],
                    multi=True,
                    placeholder='All TIF',
                    style={'marginBottom': '16px'}
                ),
            ]),

            # Aggregate dropdown
            html.Div([
                html.Label("Aggregate:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50', 'fontSize': '13px'}),
                dcc.Dropdown(
                    id='decay-aggregate-dropdown',
                    options=[
                        {'label': 'None (show all lines)', 'value': 'none'},
                        {'label': 'VWA (Volume Weighted Avg)', 'value': 'vwa'},
                    ],
                    value='none',
                    clearable=False,
                    style={'marginBottom': '16px'}
                ),
            ]),
        ], open=True, style={'marginBottom': '16px'}),

        html.Hr(style={'border': 'none', 'borderTop': '1px solid #e0e0e0', 'margin': '20px 0'}),

        # Status message
        html.Div(
            id='decay-status',
            style={
                'fontSize': '12px',
                'color': '#7f8c8d',
                'textAlign': 'center',
                'padding': '10px'
            }
        ),

    ], style={'width': '280px', 'marginLeft': '20px'})

    # Main layout
    return html.Div([
        html.Div([
            # Left side - Graph
            html.Div(
                dcc.Graph(
                    id='decay-graph',
                    figure=fig,
                    config={'displaylogo': False}
                ),
                style={'flex': '1', 'minWidth': '0'}
            ),
            # Right side - Collapsible Controls Panel
            html.Details([
                html.Summary("Controls", style={
                    'fontWeight': '600',
                    'color': '#2c3e50',
                    'cursor': 'pointer',
                    'padding': '8px 12px',
                    'backgroundColor': '#f8f9fa',
                    'borderRadius': '6px',
                    'userSelect': 'none',
                    'writingMode': 'vertical-rl',
                    'textOrientation': 'mixed',
                    'height': 'fit-content'
                }),
                right_panel,
            ], open=True, style={'display': 'flex', 'alignItems': 'flex-start'}),
        ], style={'display': 'flex', 'alignItems': 'flex-start'}),
    ], style={
        'backgroundColor': '#ffffff',
        'padding': '20px',
        'borderRadius': '12px',
        'boxShadow': '0 2px 8px rgba(0,0,0,0.08)'
    })

if __name__ == "__main__":
    df = _fetch_neighbors(
        instrument="Kraken.Spot.ADA/BTC_SPOT",
        ts_start=datetime(2025, 10, 28, 7, 0, tzinfo=timezone.utc),
        frame_mins=15,
        table_name="feed_kraken_tob_5",
        resample="1s"
    )
    df2 = _fetch_deals("2025-10-28")
    print(df2.head())
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
USD_CONVERSION_MAP = {
    "Kraken.Spot.ADA/BTC_SPOT": "Kraken.Spot.BTC/USD_SPOT",
    "Kraken.Spot.ADA/ETH_SPOT": "Kraken.Spot.ETH/USD_SPOT",
    "Kraken.Spot.ADA/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.ADA/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.ADA/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.ADA/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.BCH/BTC_SPOT": "Kraken.Spot.BTC/USD_SPOT",
    "Kraken.Spot.BCH/ETH_SPOT": "Kraken.Spot.ETH/USD_SPOT",
    "Kraken.Spot.BCH/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.BCH/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.BCH/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.BCH/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.BTC/CHF_SPOT": "Kraken.Spot.CHF/USD_SPOT",
    "Kraken.Spot.BTC/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.BTC/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.BTC/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.BTC/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.DOGE/BTC_SPOT": "Kraken.Spot.BTC/USD_SPOT",
    "Kraken.Spot.DOGE/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.DOGE/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.DOGE/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.DOGE/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.ETH/BTC_SPOT": "Kraken.Spot.BTC/USD_SPOT",
    "Kraken.Spot.ETH/CHF_SPOT": "Kraken.Spot.CHF/USD_SPOT",
    "Kraken.Spot.ETH/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.ETH/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.ETH/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.ETH/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.EUR/CHF_SPOT": "Kraken.Spot.CHF/USD_SPOT",
    "Kraken.Spot.EUR/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",

    "Kraken.Spot.LTC/BTC_SPOT": "Kraken.Spot.BTC/USD_SPOT",
    "Kraken.Spot.LTC/ETH_SPOT": "Kraken.Spot.ETH/USD_SPOT",
    "Kraken.Spot.LTC/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.LTC/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.LTC/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.LTC/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.SOL/BTC_SPOT": "Kraken.Spot.BTC/USD_SPOT",
    "Kraken.Spot.SOL/ETH_SPOT": "Kraken.Spot.ETH/USD_SPOT",
    "Kraken.Spot.SOL/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.SOL/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.SOL/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.SOL/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.USD/CHF_SPOT": "Kraken.Spot.CHF/USD_SPOT",
    "Kraken.Spot.USDC/CHF_SPOT": "Kraken.Spot.CHF/USD_SPOT",
    "Kraken.Spot.USDC/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.USDC/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.USDC/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",

    "Kraken.Spot.USDT/CHF_SPOT": "Kraken.Spot.CHF/USD_SPOT",
    "Kraken.Spot.USDT/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.USDT/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",

    "Kraken.Spot.XRP/BTC_SPOT": "Kraken.Spot.BTC/USD_SPOT",
    "Kraken.Spot.XRP/ETH_SPOT": "Kraken.Spot.ETH/USD_SPOT",
    "Kraken.Spot.XRP/EUR_SPOT": "Kraken.Spot.EUR/USD_SPOT",
    "Kraken.Spot.XRP/GBP_SPOT": "Kraken.Spot.GBP/USD_SPOT",
    "Kraken.Spot.XRP/USDC_SPOT": "Kraken.Spot.USDC/USD_SPOT",
    "Kraken.Spot.XRP/USDT_SPOT": "Kraken.Spot.USDT/USD_SPOT",
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

    # Build SQL query with optional resampling
    if resample is not None:
        # QuestDB SAMPLE BY syntax for resampling
        sql = f"""
            SELECT ts_server, instrument,
                   last(ask_px_0) as ask_px_0,
                   last(bid_px_0) as bid_px_0
            FROM {table_name}
            WHERE ts_server BETWEEN %s AND %s
              AND instrument = %s
            SAMPLE BY {resample}
            ALIGN TO CALENDAR
        """
    else:
        # No resampling - fetch raw data
        sql = f"""
            SELECT ts_server, instrument, ask_px_0, bid_px_0
            FROM {table_name}
            WHERE ts_server BETWEEN %s AND %s
              AND instrument = %s
            ORDER BY ts_server
        """

    # Execute query
    df = _run_query(sql, (ts_before_str, ts_after_str, instrument))

    # Convert ts_server to datetime and build t_from_deal column
    if not df.empty and 'ts_server' in df.columns:
        df['ts_server'] = pd.to_datetime(df['ts_server'], utc=True)

        # Sort by timestamp to ensure correct ordering
        df = df.sort_values('ts_server').reset_index(drop=True)

        # Find the index closest to ts_start (where t_from_deal = 0)
        time_diffs = (df['ts_server'] - ts_start).abs()
        center_idx = time_diffs.idxmin()

        # Build t_from_deal: negative before ts_start, 0 at ts_start, positive after
        df['t_from_deal'] = df.index - center_idx

        # Drop ts_server column
        df = df.drop(columns=['ts_server'])

    return df

def _fetch_all_market_data(deals_df: pd.DataFrame, frame_mins: int = 15,
                           table_name: str = "feed_kraken_tob_5",
                           resample: Optional[str] = "1s") -> pd.DataFrame:
    """
    Fetch all market data for all deals in one query.

    Returns DataFrame with columns: ts_server, instrument, ask_px_0, bid_px_0
    """
    if deals_df.empty:
        return pd.DataFrame()

    # Calculate overall time range
    min_time = deals_df['time'].min() - pd.Timedelta(minutes=frame_mins)
    max_time = deals_df['time'].max() + pd.Timedelta(minutes=frame_mins)

    # Get unique instruments
    instruments = deals_df['instrument'].unique().tolist()

    # Format timestamps
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    min_time_str = min_time.strftime(fmt)
    max_time_str = max_time.strftime(fmt)

    # Build SQL with IN clause for instruments
    instruments_str = "', '".join(instruments)

    if resample is not None:
        sql = f"""
            SELECT ts_server, instrument,
                   last(ask_px_0) as ask_px_0,
                   last(bid_px_0) as bid_px_0
            FROM {table_name}
            WHERE ts_server BETWEEN '{min_time_str}' AND '{max_time_str}'
              AND instrument IN ('{instruments_str}')
            SAMPLE BY {resample}
            ALIGN TO CALENDAR
        """
    else:
        sql = f"""
            SELECT ts_server, instrument, ask_px_0, bid_px_0
            FROM {table_name}
            WHERE ts_server BETWEEN '{min_time_str}' AND '{max_time_str}'
              AND instrument IN ('{instruments_str}')
            ORDER BY ts_server
        """

    print(f"[DEBUG] Fetching market data from {min_time_str} to {max_time_str} for {len(instruments)} instruments")

    # Execute query (no params since we're using string formatting for IN clause)
    df = _run_query(sql, ())

    # Convert ts_server to datetime
    if not df.empty and 'ts_server' in df.columns:
        df['ts_server'] = pd.to_datetime(df['ts_server'], utc=True)

    print(f"[DEBUG] Fetched {len(df)} market data rows")

    return df


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


def _build_dataset(date: str, frame_mins: int = 15, table_name: str = "feed_kraken_tob_5",
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

        # Instrument filter
        html.Div([
            html.Label("Instrument:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50'}),
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
            html.Label("Side:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50'}),
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
            html.Label("Order Kind:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50'}),
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
            html.Label("Order Type:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50'}),
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
            html.Label("Time In Force:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block', 'color': '#2c3e50'}),
            dcc.Dropdown(
                id='decay-tif-filter',
                options=[],
                value=[],
                multi=True,
                placeholder='All TIF',
                style={'marginBottom': '16px'}
            ),
        ]),

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
            # Right side - Filters
            right_panel,
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
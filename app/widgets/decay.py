"""
Decay/Markouts widget for Dash app.

Refactored to use precomputed tables:
- mart_kraken_decay_deals: deal information
- mart_kraken_decay_slices: precomputed price slices with returns and PnL
"""

from dash import html, dcc
import os
from datetime import date, timedelta
from datetime import datetime, timezone
from typing import List, Optional, Sequence

import pandas as pd
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor


# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------
DEALS_TABLE = "mart_kraken_decay_deals"
SLICES_TABLE = "mart_kraken_decay_slices"


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
        if VERBOSE:
            print("[DEBUG SQL]", cur.query.decode())
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def _fetch_available_dates() -> List[str]:
    """Fetch list of available dates from the deals datamart."""
    sql = f"""
        SELECT DISTINCT CAST(DATE_TRUNC('day', time) AS DATE) as date
        FROM {DEALS_TABLE}
        ORDER BY date DESC
    """
    df = _run_query(sql)
    if df.empty:
        return []
    # Convert to datetime and extract date part to ensure YYYY-MM-DD format
    dates = pd.to_datetime(df["date"]).dt.date
    return [d.isoformat() for d in dates]


def _date_range(start: date, end: date) -> List[date]:
    """Generate list of dates from start to end inclusive."""
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _fetch_deals(start_datetime: str, end_datetime: str) -> pd.DataFrame:
    """Fetch all deals for a given datetime range from the precomputed deals table."""
    # Parse datetime strings - support both date and datetime formats
    def parse_datetime(dt_str: str) -> datetime:
        dt_str = dt_str.strip()
        # Try datetime format first
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
            try:
                dt = datetime.strptime(dt_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        raise ValueError(f"Cannot parse datetime: {dt_str}")
    
    dt_start = parse_datetime(start_datetime)
    dt_end = parse_datetime(end_datetime)

    # Format timestamps for QuestDB (YYYY-MM-DDTHH:MM:SS.ffffffZ)
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    ts_start_str = dt_start.strftime(fmt)
    ts_end_str = dt_end.strftime(fmt)

    # Build SQL query
    sql = f"""
        SELECT time, instrument, side, amt, px, orderKind, orderType, tif, orderStatus, amt_usd
        FROM {DEALS_TABLE}
        WHERE time BETWEEN %s AND %s
        ORDER BY time
    """

    # Execute query
    df = _run_query(sql, (ts_start_str, ts_end_str))

    # Convert time to datetime if data exists
    if not df.empty and 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], utc=True)

    return df


def _fetch_slices(start_datetime: str, end_datetime: str) -> pd.DataFrame:
    """Fetch all precomputed slices for a given datetime range."""
    # Parse datetime strings - support both date and datetime formats
    def parse_datetime(dt_str: str) -> datetime:
        dt_str = dt_str.strip()
        # Try datetime format first
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
            try:
                dt = datetime.strptime(dt_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        raise ValueError(f"Cannot parse datetime: {dt_str}")
    
    dt_start = parse_datetime(start_datetime)
    dt_end = parse_datetime(end_datetime)

    # Format timestamps for QuestDB (YYYY-MM-DDTHH:MM:SS.ffffffZ)
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    ts_start_str = dt_start.strftime(fmt)
    ts_end_str = dt_end.strftime(fmt)

    # Build SQL query - fetch all slices for deals in this datetime range
    sql = f"""
        SELECT time, instrument, t_from_deal, ask_px_0, bid_px_0, 
               usd_ask_px_0, usd_bid_px_0, ret, pnl_usd
        FROM {SLICES_TABLE}
        WHERE time BETWEEN %s AND %s
        ORDER BY time, t_from_deal
    """

    # Execute query
    df = _run_query(sql, (ts_start_str, ts_end_str))

    # Convert time to datetime if data exists
    if not df.empty and 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], utc=True)

    return df


def _build_dataset(start_datetime: str, end_datetime: str, view: str) -> tuple[pd.DataFrame, dict]:
    """
    Build dataset from precomputed tables.
    
    Args:
        start_datetime: Start datetime string in 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format
        end_datetime: End datetime string in 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format
        view: View type ('return' or 'usd_pnl')
    
    Returns:
        Tuple of (deals_df, slices_dict) where slices_dict maps deal index to slice DataFrame
    """
    # Fetch all deals for the datetime range
    deals_df = _fetch_deals(start_datetime, end_datetime)

    if deals_df.empty:
        print(f"No deals found for {start_datetime} to {end_datetime}")
        return pd.DataFrame(), {}

    print(f"Found {len(deals_df)} deals for {start_datetime} to {end_datetime}")

    # Fetch all slices for the datetime range
    slices_df = _fetch_slices(start_datetime, end_datetime)

    if slices_df.empty:
        print(f"No slices found for {start_datetime} to {end_datetime}")
        return deals_df, {}

    print(f"Found {len(slices_df)} slices for {start_datetime} to {end_datetime}")

    # Build slices_dict: map each deal index to its corresponding slices
    slices_dict = {}
    
    for idx, deal in deals_df.iterrows():
        deal_time = deal['time']
        deal_instrument = deal['instrument']
        
        # Filter slices for this specific deal (matching time and instrument)
        deal_slices = slices_df[
            (slices_df['time'] == deal_time) & 
            (slices_df['instrument'] == deal_instrument)
        ].copy()
        
        if not deal_slices.empty:
            # Sort by t_from_deal to ensure proper ordering
            deal_slices = deal_slices.sort_values('t_from_deal').reset_index(drop=True)
            slices_dict[idx] = deal_slices

    print(f"Built {len(slices_dict)} slices for {len(deals_df)} deals")

    return deals_df, slices_dict


def get_filter_options(start_datetime: str, end_datetime: str) -> dict:
    """
    Fetch distinct filter values from deals table for the given datetime range.
    
    Returns dict with keys: instruments, sides, order_kinds, order_types, tifs
    Each value is a list of {'label': x, 'value': x} dicts for dropdown options.
    """
    try:
        # Fetch deals to get distinct values
        deals_df = _fetch_deals(start_datetime, end_datetime)
        
        if deals_df.empty:
            return {
                'instruments': [],
                'sides': [],
                'order_kinds': [],
                'order_types': [],
                'tifs': []
            }
        
        # Generate filter options
        return {
            'instruments': [{'label': inst, 'value': inst} for inst in sorted(deals_df['instrument'].unique())],
            'sides': [{'label': side, 'value': side} for side in sorted(deals_df['side'].unique())],
            'order_kinds': [{'label': ok, 'value': ok} for ok in sorted(deals_df['orderKind'].dropna().unique())],
            'order_types': [{'label': ot, 'value': ot} for ot in sorted(deals_df['orderType'].dropna().unique())],
            'tifs': [{'label': tif, 'value': tif} for tif in sorted(deals_df['tif'].dropna().unique())]
        }
    except Exception as e:
        print(f"[ERROR] Failed to fetch filter options: {e}")
        return {
            'instruments': [],
            'sides': [],
            'order_kinds': [],
            'order_types': [],
            'tifs': []
        }


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
        xaxis_title="Time from Deal (seconds)",
        yaxis_title="Value",
        annotations=[
            dict(
                text="Select data to see decay plots",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=14, color="#7f8c8d"),
            )
        ],
    )

    # Fetch available dates for default values
    available_dates = _fetch_available_dates()
    
    # Determine default date range
    if available_dates:
        # Use most recent date as default
        max_date_str = max(available_dates)
        # Default to last day of available data, full day range
        default_start = f"{max_date_str} 00:00:00"
        default_end = f"{max_date_str} 23:59:59"
    else:
        # Fallback to today
        today = datetime.now(timezone.utc).date().isoformat()
        default_start = f"{today} 00:00:00"
        default_end = f"{today} 23:59:59"

    # Fetch initial filter options for default date range
    initial_options = get_filter_options(default_start, default_end)

    # Right panel with filters
    right_panel = html.Div([
        # Compact datetime inputs
        html.Div([
            html.Label("Start:", style={'fontSize': '13px', 'color': '#7f8c8d', 'marginBottom': '4px', 'display': 'block', 'fontWeight': '600'}),
            dcc.Input(
                id='decay-start-datetime',
                type='text',
                value=default_start,
                placeholder='YYYY-MM-DD HH:MM:SS',
                debounce=True,
                style={
                    'width': '100%',
                    'height': '32px',
                    'padding': '6px 8px',
                    'fontSize': '14px',
                    'borderRadius': '4px',
                    'border': '1px solid #e0e0e0',
                    'marginBottom': '8px'
                }
            ),
            html.Label("End:", style={'fontSize': '13px', 'color': '#7f8c8d', 'marginBottom': '4px', 'display': 'block', 'fontWeight': '600'}),
            dcc.Input(
                id='decay-end-datetime',
                type='text',
                value=default_end,
                placeholder='YYYY-MM-DD HH:MM:SS',
                debounce=True,
                style={
                    'width': '100%',
                    'height': '32px',
                    'padding': '6px 8px',
                    'fontSize': '14px',
                    'borderRadius': '4px',
                    'border': '1px solid #e0e0e0',
                    'marginBottom': '12px'
                }
            ),
        ]),

        html.Hr(style={'border': 'none', 'borderTop': '1px solid #e0e0e0', 'margin': '12px 0'}),

        # Filters section (no longer collapsible)
        # View type dropdown
        html.Div([
            html.Label("View:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='decay-view-dropdown',
                options=[
                    {'label': 'Return', 'value': 'return'},
                    {'label': 'PnL, $', 'value': 'usd_pnl'},
                ],
                value='return',
                clearable=False,
                style={'marginBottom': '12px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        # Instrument filter
        html.Div([
            html.Label("Instrument:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='decay-instrument-filter',
                options=initial_options['instruments'],
                value=[],
                multi=True,
                placeholder='All',
                style={'marginBottom': '12px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        # Side filter
        html.Div([
            html.Label("Side:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='decay-side-filter',
                options=initial_options['sides'],
                value=[],
                multi=True,
                placeholder='All',
                style={'marginBottom': '12px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        # Order Kind filter
        html.Div([
            html.Label("Order Kind:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='decay-orderkind-filter',
                options=initial_options['order_kinds'],
                value=[],
                multi=True,
                placeholder='All',
                style={'marginBottom': '12px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        # Order Type filter
        html.Div([
            html.Label("Order Type:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='decay-ordertype-filter',
                options=initial_options['order_types'],
                value=[],
                multi=True,
                placeholder='All',
                style={'marginBottom': '12px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        # TIF filter
        html.Div([
            html.Label("Time In Force:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='decay-tif-filter',
                options=initial_options['tifs'],
                value=[],
                multi=True,
                placeholder='All',
                style={'marginBottom': '12px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        # Aggregate dropdown
        html.Div([
            html.Label("Group by:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='decay-aggregate-dropdown',
                options=[
                    {'label': 'None (show all)', 'value': 'none'},
                    {'label': 'VWA (Volume Weighted Avg)', 'value': 'vwa'},
                ],
                value='none',
                clearable=False,
                style={'marginBottom': '16px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        # Plot button
        html.Button(
            'Plot',
            id='decay-plot-button',
            n_clicks=0,
            style={
                'width': '100%',
                'height': '36px',
                'backgroundColor': '#3498db',
                'color': '#fff',
                'border': 'none',
                'borderRadius': '6px',
                'fontWeight': '600',
                'fontSize': '14px',
                'cursor': 'pointer',
                'marginBottom': '12px'
            }
        ),

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
                    config={'displaylogo': False},
                    style={'height': '85vh'}
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
    # Test fetching data
    deals_df, slices_dict = _build_dataset("2025-10-28", "2025-10-28", "return")
    print(f"\nDeals: {len(deals_df)}")
    print(f"Slices: {len(slices_dict)}")
    
    if deals_df is not None and not deals_df.empty:
        print("\nFirst few deals:")
        print(deals_df.head())
    
    if slices_dict:
        first_idx = list(slices_dict.keys())[0]
        print(f"\nFirst slice (deal idx={first_idx}):")
        print(slices_dict[first_idx].head())
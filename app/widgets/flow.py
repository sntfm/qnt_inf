"""
Flow widget for Dash app.

Displays two panes:
1. PNL curves: upnl_usd, rpnl_usd, tpnl_usd (total pnl)
2. Volume curves: vol_usd, cum_vol_usd, num_deals

Supports filtration by instrument and datetime range.
"""

from dash import html, dcc
import os
from datetime import datetime, timezone
from typing import List

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor


# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------
FLOW_MART_TABLE = "mart_pnl_flow"


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


def _run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a SQL query against QuestDB and return a pandas DataFrame."""
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        if VERBOSE:
            print("[DEBUG SQL]", cur.query.decode())
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def _fetch_available_dates() -> List[str]:
    """Fetch list of available dates from the flow mart table."""
    sql = f"""
        SELECT DISTINCT CAST(DATE_TRUNC('day', ts) AS DATE) as date
        FROM {FLOW_MART_TABLE}
        ORDER BY date DESC
    """
    df = _run_query(sql)
    if df.empty:
        return []
    # Convert to datetime and extract date part to ensure YYYY-MM-DD format
    dates = pd.to_datetime(df["date"]).dt.date
    return [d.isoformat() for d in dates]


def _fetch_available_instruments(start_datetime: str, end_datetime: str) -> List[str]:
    """Fetch list of available instruments for the given date range."""
    # Parse datetime strings
    def parse_datetime(dt_str: str) -> datetime:
        dt_str = dt_str.strip()
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

    # Format timestamps for QuestDB
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    ts_start_str = dt_start.strftime(fmt)
    ts_end_str = dt_end.strftime(fmt)

    sql = f"""
        SELECT DISTINCT instrument
        FROM {FLOW_MART_TABLE}
        WHERE ts BETWEEN %s AND %s
        ORDER BY instrument
    """
    df = _run_query(sql, (ts_start_str, ts_end_str))
    if df.empty:
        return []
    return df['instrument'].tolist()


def _fetch_flow_metrics(start_datetime: str, end_datetime: str, instruments: List[str] = None) -> pd.DataFrame:
    """
    Fetch flow metrics from mart_pnl_flow table for a given datetime range.

    Args:
        start_datetime: Start datetime string
        end_datetime: End datetime string
        instruments: Optional list of instruments to filter by

    Returns:
        DataFrame with columns:
        - ts: Timestamp (1-minute buckets)
        - instrument: Instrument name
        - upnl_usd: Unrealized PnL in USD
        - rpnl_usd_total: Realized PnL in USD (per bucket)
        - tpnl_usd: Total PnL in USD
        - vol_usd: Volume in USD per bucket
        - cum_cost_usd: Cumulative cost in USD
        - num_deals: Number of deals in bucket
    """
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

    # Build WHERE clause
    where_clauses = [f"ts BETWEEN %s AND %s"]
    params = [ts_start_str, ts_end_str]

    if instruments:
        placeholders = ','.join(['%s'] * len(instruments))
        where_clauses.append(f"instrument IN ({placeholders})")
        params.extend(instruments)

    where_clause = " AND ".join(where_clauses)

    # Query flow metrics
    sql = f"""
        SELECT
            ts,
            instrument,
            instrument_base,
            instrument_quote,
            upnl_usd,
            upnl_base,
            upnl_quote,
            rpnl_usd_total,
            tpnl_usd,
            vol_usd,
            cum_cost_usd,
            num_deals
        FROM {FLOW_MART_TABLE}
        WHERE {where_clause}
        ORDER BY ts, instrument
    """

    # Execute query
    df = _run_query(sql, tuple(params))

    if df.empty:
        return pd.DataFrame()

    # Convert ts to datetime
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'], utc=True)

    return df



def get_widget_layout(n_intervals):
    """
    Flow widget with two panes:
    1. PNL curves (upnl_usd, rpnl_usd, tpnl_usd)
    2. Volume curves (vol_usd, cum_vol_usd, num_deals)

    Args:
        n_intervals: Number of intervals (from dcc.Interval component)

    Returns:
        Dash HTML layout with graphs and filters
    """
    # Create initial empty figure with 2 subplots (rows)
    # Second subplot has secondary y-axis for num_deals
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Mark to Market', 'Inventory'),
        vertical_spacing=0.15,
        specs=[[{"type": "xy"}], [{"type": "xy", "secondary_y": True}]]
    )

    fig.update_layout(
        margin=dict(l=40, r=20, t=80, b=40),
        template="plotly_white",
        height=800,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        annotations=[
            dict(
                text="Select date range and click 'Load Data' to see flow metrics",
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
        # Default to last 7 days of available data
        default_end = f"{max_date_str} 23:59:59"
        # Calculate start date (7 days before)
        from datetime import timedelta
        end_date = datetime.strptime(max_date_str, '%Y-%m-%d')
        start_date = end_date - timedelta(days=6)
        default_start = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
    else:
        # Fallback to today
        today = datetime.now(timezone.utc).date()
        from datetime import timedelta
        week_ago = today - timedelta(days=6)
        default_start = f"{week_ago.isoformat()} 00:00:00"
        default_end = f"{today.isoformat()} 23:59:59"

    # Fetch initial instrument options for default date range
    try:
        initial_instruments = _fetch_available_instruments(default_start, default_end)
        initial_instrument_options = [{'label': inst, 'value': inst} for inst in initial_instruments]
    except Exception:
        initial_instrument_options = []

    # Right panel with filters
    right_panel = html.Div([
        # Compact datetime inputs
        html.Div([
            html.Label("Start:", style={'fontSize': '13px', 'color': '#7f8c8d', 'marginBottom': '4px', 'display': 'block', 'fontWeight': '600'}),
            dcc.Input(
                id='flow-start-datetime',
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
                id='flow-end-datetime',
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

        # Instrument filter
        html.Div([
            html.Label("Instrument:", style={'fontWeight': '600', 'marginBottom': '4px', 'display': 'block', 'color': '#7f8c8d', 'fontSize': '13px'}),
            dcc.Dropdown(
                id='flow-instrument-filter',
                options=initial_instrument_options,
                value=[],
                multi=True,
                placeholder='All instruments',
                style={'marginBottom': '12px', 'fontSize': '14px', 'color': '#2c3e50'}
            ),
        ]),

        html.Hr(style={'border': 'none', 'borderTop': '1px solid #e0e0e0', 'margin': '12px 0'}),

        # Load button
        html.Button(
            'Plot',
            id='flow-load-button',
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
            id='flow-status',
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
        # Store component to persist legend visibility state
        dcc.Store(id='flow-legend-state', data={}),

        html.Div([
            # Left side - Graph
            html.Div(
                dcc.Graph(
                    id='flow-graph',
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
        'boxShadow': '0 2px 8px rgba(0,0,0,0.08)',
        'marginBottom': '40px'  # Add space after widget to prevent overlap
    })


if __name__ == "__main__":
    # Test fetching data
    print("Testing flow widget data fetching...")

    # Test available dates
    dates = _fetch_available_dates()
    print(f"\nAvailable dates: {dates[:5] if dates else 'None'}")

    if dates:
        # Test fetching instruments
        test_start = f"{dates[0]} 00:00:00"
        test_end = f"{dates[0]} 23:59:59"

        instruments = _fetch_available_instruments(test_start, test_end)
        print(f"\nAvailable instruments for {dates[0]}: {instruments}")

        # Test fetching metrics (all instruments)
        metrics_df = _fetch_flow_metrics(test_start, test_end)
        print(f"\nFlow Metrics (all instruments): {len(metrics_df)} rows")

        if not metrics_df.empty:
            print("\nFirst few rows:")
            print(metrics_df.head(10))
            print(f"\nColumns: {metrics_df.columns.tolist()}")

        # Test fetching metrics (filtered by instrument)
        if instruments:
            filtered_df = _fetch_flow_metrics(test_start, test_end, instruments=[instruments[0]])
            print(f"\nFlow Metrics (filtered to {instruments[0]}): {len(filtered_df)} rows")

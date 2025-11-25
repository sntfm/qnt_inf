"""
Flow widget for Dash app.

Displays aggregate metrics for a date range:
- PNL in USD
- Volume in USD
- Quantity of deals
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


def _run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
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


def _fetch_flow_metrics(start_datetime: str, end_datetime: str) -> pd.DataFrame:
    """
    Fetch flow metrics aggregated by day for a given datetime range.
    
    Returns DataFrame with columns:
    - date: The date (day)
    - total_pnl_usd: Sum of PnL in USD for all deals on that day
    - total_volume_usd: Sum of volume (amt_usd) for all deals on that day
    - deal_count: Number of deals on that day
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

    # QuestDB has limitations with complex queries, so we'll use a two-query approach:
    # 1. Get daily deal aggregates (volume, count)
    # 2. Get daily PnL aggregates
    # Then merge them in Python
    
    # Query 1: Daily deal metrics
    deals_sql = f"""
        SELECT 
            CAST(timestamp_floor('d', time) AS DATE) AS date,
            SUM(amt_usd) AS total_volume_usd,
            COUNT(*) AS deal_count
        FROM {DEALS_TABLE}
        WHERE time BETWEEN %s AND %s
        GROUP BY CAST(timestamp_floor('d', time) AS DATE)
        ORDER BY date
    """
    
    # Query 2: Daily PnL (using max t_from_deal for each deal)
    pnl_sql = f"""
        WITH ranked_slices AS (
            SELECT 
                CAST(timestamp_floor('d', time) AS DATE) AS date,
                time,
                instrument,
                pnl_usd,
                ROW_NUMBER() OVER (PARTITION BY time, instrument ORDER BY t_from_deal DESC) AS rn
            FROM {SLICES_TABLE}
            WHERE time BETWEEN %s AND %s
        )
        SELECT 
            date,
            SUM(pnl_usd) AS total_pnl_usd
        FROM ranked_slices
        WHERE rn = 1
        GROUP BY date
        ORDER BY date
    """
    
    # Execute both queries
    deals_df = _run_query(deals_sql, (ts_start_str, ts_end_str))
    pnl_df = _run_query(pnl_sql, (ts_start_str, ts_end_str))
    
    # Merge the results
    if deals_df.empty:
        return pd.DataFrame()
    
    # Convert dates to datetime for merging
    if not deals_df.empty and 'date' in deals_df.columns:
        deals_df['date'] = pd.to_datetime(deals_df['date'])
    if not pnl_df.empty and 'date' in pnl_df.columns:
        pnl_df['date'] = pd.to_datetime(pnl_df['date'])
    
    # Merge deals and PnL data
    if not pnl_df.empty:
        df = deals_df.merge(pnl_df, on='date', how='left')
        # Fill missing PnL with 0
        df['total_pnl_usd'] = df['total_pnl_usd'].fillna(0)
    else:
        # No PnL data, add column with zeros
        df = deals_df.copy()
        df['total_pnl_usd'] = 0
    
    return df



def get_widget_layout(n_intervals):
    """
    Flow widget with date range selection and metrics display.

    Args:
        n_intervals: Number of intervals (from dcc.Interval component)

    Returns:
        Dash HTML layout with graphs and filters
    """
    # Create initial empty figure with 3 subplots
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=('PnL (USD)', 'Volume (USD)', 'Number of Deals'),
        vertical_spacing=0.12,
        specs=[[{"type": "scatter"}], [{"type": "scatter"}], [{"type": "scatter"}]]
    )
    
    fig.update_layout(
        margin=dict(l=40, r=20, t=80, b=40),
        template="plotly_white",
        height=800,
        showlegend=False,
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

        # Load button
        html.Button(
            'Load Data',
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
        'boxShadow': '0 2px 8px rgba(0,0,0,0.08)'
    })


if __name__ == "__main__":
    # Test fetching data
    metrics_df = _fetch_flow_metrics("2025-10-28 00:00:00", "2025-10-28 23:59:59")
    print(f"\nFlow Metrics: {len(metrics_df)} days")
    
    if not metrics_df.empty:
        print("\nMetrics:")
        print(metrics_df)

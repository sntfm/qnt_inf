"""
Latency widget for Dash app.

Displays latency histogram and statistics from precomputed datamarts:
- mart_kraken_latency: histogram bins by date/hour
- mart_kraken_latency_stats: daily statistics (mean, std, median, p99)
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Sequence

import pandas as pd
import plotly.graph_objects as go
import psycopg2
from dash import dcc, html
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# QuestDB connection helpers
# ---------------------------------------------------------------------------
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

# Datamart tables
LATENCY_HISTOGRAM_TABLE = "mart_kraken_latency"
LATENCY_STATS_TABLE = "mart_kraken_latency_stats"

# Display constants
BIN_SIZE_MS = 2.0
MAX_LATENCY_MS = 200.0
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
        if VERBOSE: print("[DEBUG SQL]", cur.query.decode()) 
        rows = cur.fetchall()
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Data helpers - fetch from precomputed datamarts
# ---------------------------------------------------------------------------
def _fetch_available_dates() -> List[str]:
    """Fetch list of available dates from the histogram datamart."""
    sql = f"""
        SELECT DISTINCT date
        FROM {LATENCY_HISTOGRAM_TABLE}
        ORDER BY date DESC
    """
    df = _run_query(sql)
    if df.empty:
        return []
    return df["date"].tolist()


def _fetch_latest_date() -> Optional[str]:
    """Fetch the most recent available date from the histogram datamart."""
    sql = f"""
        SELECT date
        FROM {LATENCY_HISTOGRAM_TABLE}
        ORDER BY ts DESC
        LIMIT 1
    """
    df = _run_query(sql)
    if df.empty:
        return None
    return df["date"].iloc[0]


def _fetch_histogram_data(date_str: str) -> pd.DataFrame:
    """
    Fetch precomputed histogram bins for a specific date.

    Returns DataFrame with: hour, latency_bin_start_ms, bin_count, latency_ms (bin center)
    """
    sql = f"""
        SELECT hour, latency_bin_start_ms, bin_count
        FROM {LATENCY_HISTOGRAM_TABLE}
        WHERE date = %s
        ORDER BY hour, latency_bin_start_ms
    """
    df = _run_query(sql, (date_str,))

    if df.empty:
        return df

    df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype(int)
    df["latency_bin_start_ms"] = pd.to_numeric(df["latency_bin_start_ms"], errors="coerce")
    df["bin_count"] = pd.to_numeric(df["bin_count"], errors="coerce").fillna(0).astype(int)
    df.dropna(subset=["latency_bin_start_ms", "hour"], inplace=True)
    df["latency_ms"] = df["latency_bin_start_ms"] + BIN_SIZE_MS / 2.0
    return df


def _fetch_stats(date_str: str) -> Optional[dict]:
    """
    Fetch precomputed statistics for a specific date.

    Returns dict with: mean_ms, std_ms, median_ms, p99_ms, sample_count
    """
    sql = f"""
        SELECT mean_ms, std_ms, median_ms, p99_ms, sample_count
        FROM {LATENCY_STATS_TABLE}
        WHERE date = %s
        LIMIT 1
    """
    df = _run_query(sql, (date_str,))

    if df.empty:
        return None

    return {
        "mean_ms": float(df["mean_ms"].iloc[0]),
        "std_ms": float(df["std_ms"].iloc[0]),
        "median_ms": float(df["median_ms"].iloc[0]),
        "p99_ms": float(df["p99_ms"].iloc[0]),
        "sample_count": int(df["sample_count"].iloc[0]),
    }


def _build_histogram(df: pd.DataFrame, stats: Optional[dict] = None) -> go.Figure:
    """Create histogram figure with separate traces for each hour using magma palette."""
    import plotly.express as px

    fig = go.Figure()

    # Get magma colors from plotly (24 colors for 24 hours)
    magma_colors = px.colors.sequential.Magma
    # Expand to 24 colors by sampling
    n_hours = 24
    color_indices = [int(i * (len(magma_colors) - 1) / (n_hours - 1)) for i in range(n_hours)]
    hour_colors = [magma_colors[i] for i in color_indices]

    # Group by hour
    hours = sorted(df["hour"].unique())

    for hour in hours:
        hour_df = df[df["hour"] == hour].copy()

        fig.add_trace(
            go.Bar(
                x=hour_df["latency_ms"],
                y=hour_df["bin_count"],
                width=BIN_SIZE_MS,
                name=f"{int(hour):02d}:00",
                marker=dict(color=hour_colors[int(hour)]),
                opacity=0.7,
            )
        )

    # Add mean and median lines from precomputed stats
    if stats:
        mean_val = stats["mean_ms"]
        median_val = stats["median_ms"]

        # Add mean vertical line
        fig.add_vline(
            x=mean_val,
            line_dash="dash",
            line_color="red",
            line_width=2,
            annotation_text=f"Mean: {mean_val:.2f} ms",
            annotation_position="top",
        )

        # Add median vertical line
        fig.add_vline(
            x=median_val,
            line_dash="dot",
            line_color="blue",
            line_width=2,
            annotation_text=f"Median: {median_val:.2f} ms",
            annotation_position="top",
        )

    fig.update_layout(
        margin=dict(l=40, r=20, t=60, b=40),
        template="plotly_white",
        barmode="overlay",
        xaxis_title="Latency (ms)",
        yaxis_title="Count",
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
            title="Hour (UTC)"
        ),
    )
    return fig


def _date_range(start: date, end: date) -> List[date]:
    """Generate list of dates from start to end inclusive."""
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _stat_table(stats: Optional[dict]) -> html.Div:
    """Render a statistics table from precomputed stats."""
    if not stats:
        return html.Div(
            html.P("No data", style={"color": "#7f8c8d", "textAlign": "center", "padding": "20px"}),
        )

    def fmt(value: float) -> str:
        return f"{value:.2f} ms"

    table_cell_style = {
        "padding": "12px",
        "borderBottom": "1px solid #e0e0e0",
    }

    stats_data = [
        ("samples", f"{stats['sample_count']:,}"),
        ("mean", fmt(stats["mean_ms"])),
        ("std", fmt(stats["std_ms"])),
        ("median", fmt(stats["median_ms"])),
        ("p99", fmt(stats["p99_ms"])),
    ]

    table_rows = [
        html.Tr([
            html.Td(stat_name, style=table_cell_style),
            html.Td(stat_value, style={**table_cell_style, "fontWeight": "400"}),
        ])
        for stat_name, stat_value in stats_data
    ]

    return html.Div(
        html.Table(
            [
                html.Tbody(table_rows),
            ],
            style={
                "width": "100%",
                "borderCollapse": "collapse",
                "backgroundColor": "#ffffff",
                "borderRadius": "8px",
                "overflow": "hidden",
                "boxShadow": "0 1px 3px rgba(0,0,0,0.1)",
            }
        ),
        style={"minWidth": "200px"}
    )


# ---------------------------------------------------------------------------
# Public API used by Dash app
# ---------------------------------------------------------------------------
def create_filter_controls() -> html.Div:
    """Return empty div - filters are now integrated into the widget."""
    return html.Div()


def get_available_dates() -> List[str]:
    """Return list of available dates in the datamart."""
    return _fetch_available_dates()


def get_widget_content(date_str: Optional[str] = None) -> html.Div:
    """Return the latency histogram layout."""
    # Fetch available dates for the picker
    available_dates = _fetch_available_dates()

    # If no date specified, use the latest available date
    if not date_str:
        date_str = _fetch_latest_date()

    # If still no date (empty datamart), show empty state
    if not date_str:
        fig = go.Figure()
        fig.update_layout(
            margin=dict(l=40, r=20, t=60, b=40),
            template="plotly_white",
            xaxis_title="Latency (ms)",
            yaxis_title="Count",
            xaxis=dict(range=[0, MAX_LATENCY_MS]),
            yaxis=dict(range=[0, 100]),
            annotations=[
                dict(
                    text="No data available in datamart",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=14, color="#7f8c8d"),
                )
            ],
        )
        stats_component = _stat_table(None)
        default_date = datetime.now(timezone.utc).date().isoformat()
    else:
        # Fetch data from precomputed tables
        df = _fetch_histogram_data(date_str)
        stats = _fetch_stats(date_str)

        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                margin=dict(l=40, r=20, t=60, b=40),
                template="plotly_white",
                xaxis_title="Latency (ms)",
                yaxis_title="Count",
                xaxis=dict(range=[0, MAX_LATENCY_MS]),
                yaxis=dict(range=[0, 100]),
                annotations=[
                    dict(
                        text="No data available for selected date",
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                        showarrow=False,
                        font=dict(size=14, color="#7f8c8d"),
                    )
                ],
            )
            stats_component = _stat_table(None)
        else:
            fig = _build_histogram(df, stats)
            stats_component = _stat_table(stats)

        default_date = date_str

    # Convert available dates to date objects for the picker
    available_date_objects = []
    for d in available_dates:
        try:
            available_date_objects.append(datetime.strptime(d, "%Y-%m-%d").date())
        except ValueError:
            pass

    # Determine min/max dates from available dates
    if available_date_objects:
        min_date = min(available_date_objects)
        max_date = max(available_date_objects)
    else:
        min_date = date(2020, 1, 1)
        max_date = datetime.now(timezone.utc).date()

    # Right panel with date picker and collapsible stats
    right_panel = html.Div(
        [
            html.Div(
                [
                    dcc.DatePickerSingle(
                        id="latency-date-input",
                        min_date_allowed=min_date,
                        max_date_allowed=max_date,
                        initial_visible_month=max_date,
                        date=default_date,
                        display_format="YYYY-MM-DD",
                        disabled_days=[
                            d for d in _date_range(min_date, max_date)
                            if d not in available_date_objects
                        ],
                    ),
                ],
                style={"marginBottom": "20px"},
            ),
            html.Hr(style={"border": "none", "borderTop": "1px solid #e0e0e0", "margin": "20px 0"}),
            # Collapsible Stats section
            html.Details([
                html.Summary("Statistics", style={
                    "fontWeight": "600",
                    "color": "#2c3e50",
                    "cursor": "pointer",
                    "padding": "8px 0",
                    "marginBottom": "12px",
                    "userSelect": "none"
                }),
                stats_component,
            ], open=True, style={"marginBottom": "16px"}),
        ],
        style={"width": "280px", "marginLeft": "20px"},
    )

    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        dcc.Graph(id="latency-histogram", figure=fig, config={"displaylogo": False}),
                        style={"flex": "1", "minWidth": "0"},
                    ),
                    # Collapsible Controls Panel
                    html.Details([
                        html.Summary("Controls", style={
                            "fontWeight": "600",
                            "color": "#2c3e50",
                            "cursor": "pointer",
                            "padding": "8px 12px",
                            "backgroundColor": "#f8f9fa",
                            "borderRadius": "6px",
                            "userSelect": "none",
                            "writingMode": "vertical-rl",
                            "textOrientation": "mixed",
                            "height": "fit-content"
                        }),
                        right_panel,
                    ], open=True, style={"display": "flex", "alignItems": "flex-start"}),
                ],
                style={"display": "flex", "alignItems": "flex-start"},
            ),
        ],
        style={
            "backgroundColor": "#ffffff",
            "padding": "20px",
            "borderRadius": "12px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
        },
    )


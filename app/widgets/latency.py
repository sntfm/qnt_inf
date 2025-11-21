"""
Latency widget for Dash app.

Provides filter controls (date + instrument) and renders a histogram
showing the latency distribution pulled directly from QuestDB (PG port 8812).
"""

from __future__ import annotations

import os
from datetime import date, datetime, time, timezone
from typing import List, Optional, Sequence, Tuple, Union

import numpy as np
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
LATENCY_SAMPLE_TABLE = os.getenv("LATENCY_WIDGET_SAMPLE_TABLE", "feed_kraken_tob_5")
BIN_SIZE_MS = float(os.getenv("LATENCY_WIDGET_BIN_MS", "2"))
MAX_LATENCY_MS = float(os.getenv("LATENCY_WIDGET_MAX_MS", "200"))
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
# Data helpers
# ---------------------------------------------------------------------------
def _normalized_date_bounds(date_str: Optional[str]):
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_date = datetime.now(timezone.utc).date()

    start_dt = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    end_dt   = datetime.combine(target_date, time.max, tzinfo=timezone.utc)

    start_us = int(start_dt.timestamp() * 1_000_000)
    end_us   = int(end_dt.timestamp() * 1_000_000)

    if VERBOSE: print(f"[DEBUG] _normalized_date_bounds: date_str={date_str}, target_date={target_date}, start_dt={start_dt}, end_dt={end_dt}")
    if VERBOSE: print(f"[DEBUG] start_us={start_us}, end_us={end_us}")

    return start_us, end_us, target_date.isoformat()


def _fetch_latency_sample(
    table_name: str,
    date_str: Optional[str],
) -> pd.DataFrame:
    """
    Fetch latency histogram bins (already aggregated) directly from QuestDB.

    Returns a DataFrame with one row per latency bin and hour:
        hour, latency_bin_start_ms, bin_count, latency_ms (bin center), date_str
    """
    if VERBOSE: print(f"[DEBUG] _fetch_latency_sample called with table_name={table_name}, date_str={date_str}")
    start_us, end_us, normalized = _normalized_date_bounds(date_str)
    if VERBOSE: print(f"[DEBUG] Time bounds: start_us={start_us}, end_us={end_us}, normalized={normalized}")
    params = [start_us, end_us, BIN_SIZE_MS, BIN_SIZE_MS, MAX_LATENCY_MS]


    latency_expr = "(CAST(ts_server AS LONG)/1000.0 - ts_exch)"

    sql = f"""
        WITH samples AS (
            SELECT
                {latency_expr} AS latency_ms,
                EXTRACT(HOUR FROM to_timezone(ts_server, 'UTC')) AS hour
            FROM {table_name}
            WHERE ts_server BETWEEN %s AND %s
        )
        SELECT
            hour,
            FLOOR(latency_ms / %s) * %s AS latency_bin_start_ms,
            COUNT(*) AS bin_count
        FROM samples
        WHERE latency_ms >= 0 AND latency_ms <= %s
        GROUP BY hour, latency_bin_start_ms
        ORDER BY hour, latency_bin_start_ms
    """

    df = _run_query(sql, params)

    if df.empty:
        return df

    df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype(int)
    df["latency_bin_start_ms"] = pd.to_numeric(df["latency_bin_start_ms"], errors="coerce")
    df["bin_count"] = pd.to_numeric(df["bin_count"], errors="coerce").fillna(0).astype(int)
    df.dropna(subset=["latency_bin_start_ms", "hour"], inplace=True)
    df["latency_ms"] = df["latency_bin_start_ms"] + BIN_SIZE_MS / 2.0
    df["date_str"] = normalized
    return df


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, quantile: float) -> float:
    """Compute weighted quantile given sorted values."""
    if weights.sum() == 0:
        return float("nan")
    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]
    cumulative = np.cumsum(weights)
    cutoff = quantile * cumulative[-1]
    idx = np.searchsorted(cumulative, cutoff)
    idx = min(idx, len(values) - 1)
    return float(values[idx])


def _build_histogram(df: pd.DataFrame) -> go.Figure:
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
        values = hour_df["latency_ms"].to_numpy()
        weights = hour_df["bin_count"].to_numpy()

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

    # Calculate overall mean and median across all hours
    weights = df["bin_count"].to_numpy()
    values = df["latency_ms"].to_numpy()
    sample_count = int(weights.sum())

    if sample_count > 0:
        mean_val = float(np.average(values, weights=weights))
        median_val = _weighted_quantile(values, weights, 0.5)

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


def _stat_table(df: pd.DataFrame) -> html.Div:
    """Render a statistics table."""
    weights = df["bin_count"].to_numpy()
    values = df["latency_ms"].to_numpy()
    sample_count = int(weights.sum())
    num_hours = len(df["hour"].unique())

    if sample_count == 0:
        mean_val = median_val = p95_val = 0.0
    else:
        mean_val = float(np.average(values, weights=weights))
        median_val = _weighted_quantile(values, weights, 0.5)
        p95_val = _weighted_quantile(values, weights, 0.95)

    def fmt(value: float) -> str:
        return f"{value:.2f} ms"

    table_header_style = {
        "padding": "12px",
        "textAlign": "left",
        "borderBottom": "2px solid #e0e0e0",
        "fontWeight": "600",
        "color": "#2c3e50",
        "backgroundColor": "#f5f7fa",
    }

    table_cell_style = {
        "padding": "12px",
        "borderBottom": "1px solid #e0e0e0",
    }

    stats_data = [
        ("Samples", f"{sample_count:,}"),
        ("Mean", fmt(mean_val)),
        ("Median", fmt(median_val)),
        ("P95", fmt(p95_val)),
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
def create_filter_controls(table_name: str) -> html.Div:
    """Return empty div - filters are now integrated into the widget."""
    return html.Div()


def get_widget_content(
    date_str: Optional[str],
    table_name: str,
) -> html.Div:
    """Return the latency histogram layout."""
    df = _fetch_latency_sample(table_name, date_str)

    # Create empty figure or histogram based on data availability
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
        # Create empty stats for display
        stats_component = html.Div(
            html.P("No data", style={"color": "#7f8c8d", "textAlign": "center", "padding": "20px"}),
        )
    else:
        fig = _build_histogram(df)
        stats_component = _stat_table(df)

    # Default date for picker
    default_date = datetime.now(timezone.utc).date().isoformat()
    if date_str:
        default_date = date_str

    # Right panel with date picker, apply button, and collapsible stats
    right_panel = html.Div(
        [
            html.Div(
                [
                    dcc.DatePickerSingle(
                        id="latency-date-input",
                        min_date_allowed=date(2020, 1, 1),
                        max_date_allowed=datetime.now(timezone.utc).date(),
                        initial_visible_month=datetime.now(timezone.utc).date(),
                        date=default_date,
                        display_format="YYYY-MM-DD",
                        persistence=True,
                        persistence_type="memory",
                    ),
                ],
                style={"marginBottom": "12px"},
            ),
            html.Button(
                "Apply",
                id="latency-apply-button",
                n_clicks=0,
                style={
                    "width": "100%",
                    "height": "40px",
                    "backgroundColor": "#3498db",
                    "color": "#fff",
                    "border": "none",
                    "borderRadius": "6px",
                    "fontWeight": "600",
                    "cursor": "pointer",
                    "marginBottom": "20px",
                },
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


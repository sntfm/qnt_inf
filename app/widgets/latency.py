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


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------
from datetime import datetime, time, timezone

def _normalized_date_bounds(date_str: Optional[str]):
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_date = datetime.now(timezone.utc).date()

    start_dt = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    end_dt   = datetime.combine(target_date, time.max, tzinfo=timezone.utc)

    start_us = int(start_dt.timestamp() * 1_000_000)
    end_us   = int(end_dt.timestamp() * 1_000_000)

    print(f"[DEBUG] _normalized_date_bounds: date_str={date_str}, target_date={target_date}, start_dt={start_dt}, end_dt={end_dt}")
    print(f"[DEBUG] start_us={start_us}, end_us={end_us}")

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
    print(f"[DEBUG] _fetch_latency_sample called with table_name={table_name}, date_str={date_str}")
    start_us, end_us, normalized = _normalized_date_bounds(date_str)
    print(f"[DEBUG] Time bounds: start_us={start_us}, end_us={end_us}, normalized={normalized}")
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
    print(df)
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


def _stat_cards(df: pd.DataFrame) -> html.Div:
    """Render small cards with summary statistics (overall across all hours)."""
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

    card_style = {
        "padding": "12px 16px",
        "backgroundColor": "#f5f7fa",
        "borderRadius": "8px",
        "minWidth": "160px",
        "boxShadow": "0 1px 2px rgba(0,0,0,0.06)",
    }

    def fmt(value: float) -> str:
        return f"{value:.2f} ms"

    cards = [
        html.Div([html.Small("Samples"), html.H4(f"{sample_count:,}")], style=card_style),
        html.Div([html.Small("Hours"), html.H4(f"{num_hours}")], style=card_style),
        html.Div([html.Small("Mean"), html.H4(fmt(mean_val))], style=card_style),
        html.Div([html.Small("Median"), html.H4(fmt(median_val))], style=card_style),
        html.Div([html.Small("P95"), html.H4(fmt(p95_val))], style=card_style),
    ]
    return html.Div(cards, style={"display": "flex", "gap": "16px", "flexWrap": "wrap"})


# ---------------------------------------------------------------------------
# Public API used by Dash app
# ---------------------------------------------------------------------------
def create_filter_controls(table_name: str) -> html.Div:
    """Return static filter controls for the latency widget."""
    default_date = datetime.now(timezone.utc).date().isoformat()

    return html.Div(
        [
            html.Div(
                [
                    html.Label("Date", style={"fontWeight": "600"}),
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
                style={"flex": "1"},
            ),
            html.Div(
                [
                    html.Button(
                        "Apply",
                        id="latency-apply-button",
                        n_clicks=0,
                        style={
                            "width": "100%",
                            "height": "46px",
                            "backgroundColor": "#3498db",
                            "color": "#fff",
                            "border": "none",
                            "borderRadius": "6px",
                            "fontWeight": "600",
                        },
                    )
                ],
                style={"flex": "0.5", "alignSelf": "flex-end"},
            ),
        ],
        style={
            "display": "flex",
            "gap": "16px",
            "flexWrap": "wrap",
            "backgroundColor": "#ffffff",
            "padding": "16px",
            "borderRadius": "10px",
            "boxShadow": "0 2px 6px rgba(0,0,0,0.08)",
            "marginBottom": "18px",
        },
    )


def get_widget_content(
    date: Optional[str],
    table_name: str,
) -> html.Div:
    """Return the latency histogram layout."""
    df = _fetch_latency_sample(table_name, date)

    if df.empty:
        return html.Div(
            [
                html.P(
                    "No latency samples found for the selected date.",
                    style={"color": "#7f8c8d"},
                )
            ],
            style={
                "backgroundColor": "#fff",
                "padding": "40px",
                "borderRadius": "10px",
                "boxShadow": "0 2px 6px rgba(0,0,0,0.05)",
                "textAlign": "center",
            },
        )

    fig = _build_histogram(df)

    return html.Div(
        [
            html.Div(
                [
                    html.H3(
                        f"Latency histogram · {df['date_str'].iloc[0]}",
                        style={"marginBottom": "12px", "color": "#2c3e50"},
                    ),
                    _stat_cards(df),
                ],
                style={"marginBottom": "16px"},
            ),
            dcc.Graph(id="latency-histogram", figure=fig, config={"displaylogo": False}),
        ],
        style={
            "backgroundColor": "#ffffff",
            "padding": "20px",
            "borderRadius": "12px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
        },
    )


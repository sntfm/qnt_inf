"""
Decay/PnL widget for Dash app.
Placeholder - to be implemented.
"""

from dash import html


def get_widget_layout(n_intervals):
    """
    Placeholder function for decay widget.

    Args:
        n_intervals: Number of intervals (from dcc.Interval component)

    Returns:
        Dash HTML layout
    """
    return html.Div([
        html.Div([
            html.H3("Decay Widget - Coming Soon",
                   style={'textAlign': 'center', 'color': '#7f8c8d', 'marginBottom': '10px'}),
            html.P("This widget will display PnL decay metrics and visualizations.",
                  style={'textAlign': 'center', 'color': '#95a5a6'}),
        ], style={'padding': '40px', 'backgroundColor': '#f8f9fa', 'borderRadius': '8px'})
    ])

import dash
from dash import dcc, html, callback_context
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from flask import redirect
from datetime import datetime

# Import widgets (will be developed later)
from widgets import latency, decay

# Initialize the Dash app
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title="Quant Monitor"
)

# Server instance for Flask routes
server = app.server

# Define the layout for the main app
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Main page layout with widgets
def get_main_layout():
    """Layout for the main dashboard showing latency and decay widgets."""
    return html.Div([

        # Container for latency widget
        html.Div([
            html.H2("Latency",
                   style={'color': '#34495e', 'borderBottom': '2px solid', 'paddingBottom': 5}),

            # Filter controls container (separate, won't be re-rendered)
            html.Div(id='latency-controls-container'),

            # Widget content container (will be updated by filters)
            html.Div(id='latency-widget-container'),
        ], style={'marginBottom': 40}),

        # Container for decay widget
        html.Div([
            html.H2("Decay/Markouts",
                   style={'color': '#34495e', 'borderBottom': '2px solid', 'paddingBottom': 5}),
            html.Div(id='decay-widget-container'),
        ], style={'marginBottom': 40}),

        # Interval component for live updates
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # Update every 5 seconds
            n_intervals=0
        ),

        # Footer goes below


    ], style={
        'fontFamily': 'Arial, sans-serif',
        'maxWidth': '1400px',
        'margin': '0 auto',
        'padding': '20px'
    })

# Callback to handle page routing
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    """Route to different pages based on URL pathname."""
    if pathname == '/app':
        return get_main_layout()
    elif pathname == '/':
        # Redirect root to /app
        return dcc.Location(pathname='/app', id='redirect')
    else:
        # Return 404 for unknown paths
        return html.Div([
            html.H1('404: Page Not Found', style={'textAlign': 'center'}),
            html.A('Go to App', href='/app', style={'display': 'block', 'textAlign': 'center'})
        ])

# Flask route for Grafana redirect
@server.route('/grafana')
def grafana_redirect():
    """Redirect to Grafana dashboard at localhost:3000."""
    return redirect('http://localhost:3000', code=302)

# Latency controls - load once and never update
@app.callback(
    Output('latency-controls-container', 'children'),
    Input('url', 'pathname')
)
def initialize_latency_controls(pathname):
    """Initialize latency filter controls (only once on page load)."""
    if pathname == '/app':
        try:
            return latency.create_filter_controls(table_name="feed_kraken_tob_5")
        except Exception as e:
            return html.Div([
                html.P(f"Error loading controls: {str(e)}",
                      style={'color': '#e74c3c', 'fontSize': '12px'})
            ])
    return html.Div()

# Latency widget content - initial load
@app.callback(
    Output('latency-widget-container', 'children'),
    Input('url', 'pathname')
)
def initialize_latency_widget(pathname):
    """Initialize latency widget content on page load."""
    if pathname == '/app':
        try:
            # Load widget content with default settings (today's date)
            return latency.get_widget_content(date_str=None, table_name="feed_kraken_tob_5")
        except Exception as e:
            import traceback
            return html.Div([
                html.P(f"Latency widget error: {str(e)}",
                      style={'color': '#e74c3c', 'fontStyle': 'italic'}),
                html.Pre(traceback.format_exc(),
                        style={'fontSize': '10px', 'color': '#95a5a6'})
            ])
    return html.Div()

# Latency widget - filter updates
@app.callback(
    Output('latency-widget-container', 'children', allow_duplicate=True),
    Input('latency-apply-button', 'n_clicks'),
    State('latency-date-input', 'date'),
    prevent_initial_call=True
)
def update_latency_filters(n_clicks, date):
    """Update latency widget when filters are applied."""
    try:
        print(f"[DEBUG] update_latency_filters called with n_clicks={n_clicks}, date={date}")
        # Use get_widget_content to update only the data, not the controls
        return latency.get_widget_content(date_str=date, table_name="feed_kraken_tob_5")
    except Exception as e:
        import traceback
        print(f"Error in update_latency_filters: {e}")
        return html.Div([
            html.P(f"Latency widget error: {str(e)}",
                  style={'color': '#e74c3c', 'fontStyle': 'italic'}),
            html.Pre(traceback.format_exc(),
                    style={'fontSize': '10px', 'color': '#95a5a6'})
        ])

@app.callback(
    Output('decay-widget-container', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_decay_widget(n):
    """Update decay widget with latest data from QuestDB."""
    try:
        # This will call the decay widget module
        return decay.get_widget_layout(n)
    except Exception as e:
        return html.Div([
            html.P(f"Decay widget not yet implemented: {str(e)}",
                  style={'color': '#e74c3c', 'fontStyle': 'italic'})
        ])

if __name__ == '__main__':
    # Run the Dash app
    app.run(
        debug=True,
        host='0.0.0.0',
        port=8050
    )

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

# Server-side storage for decay data (too large for dcc.Store)
DECAY_DATA_CACHE = {
    'deals': None,
    'slices': None
}

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
    Input('url', 'pathname')
)
def initialize_decay_widget(pathname):
    """Initialize decay widget on page load."""
    if pathname == '/app':
        try:
            # Load widget layout once on page load (not on interval)
            return decay.get_widget_layout(0)
        except Exception as e:
            import traceback
            return html.Div([
                html.P(f"Decay widget error: {str(e)}",
                      style={'color': '#e74c3c', 'fontStyle': 'italic'}),
                html.Pre(traceback.format_exc(),
                        style={'fontSize': '10px', 'color': '#95a5a6'})
            ])
    return html.Div()

# Decay widget callbacks
@app.callback(
    [Output('decay-instrument-filter', 'options'),
     Output('decay-side-filter', 'options'),
     Output('decay-orderkind-filter', 'options'),
     Output('decay-ordertype-filter', 'options'),
     Output('decay-status', 'children')],
    [Input('decay-load-button', 'n_clicks'),
     Input('decay-refresh-button', 'n_clicks')],
    [State('decay-date-input', 'value')],
    prevent_initial_call=False
)
def load_decay_data(load_clicks, refresh_clicks, date_str):
    """Load deals and neighbor data, populate filter options."""
    import pandas as pd
    import numpy as np

    ctx = callback_context

    if not ctx.triggered:
        return [], [], [], [], ""

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # If refresh button clicked, use cached data to regenerate filters
    if button_id == 'decay-refresh-button':
        if DECAY_DATA_CACHE['deals'] is not None:
            deals_df = pd.DataFrame(DECAY_DATA_CACHE['deals'])

            # Regenerate filter options from stored data
            instruments = [{'label': inst, 'value': inst} for inst in sorted(deals_df['instrument'].unique())]
            sides = [{'label': side, 'value': side} for side in sorted(deals_df['side'].unique())]
            order_kinds = [{'label': ok, 'value': ok} for ok in sorted(deals_df['orderKind'].dropna().unique())]
            order_types = [{'label': ot, 'value': ot} for ot in sorted(deals_df['orderType'].dropna().unique())]

            status = f"Filters refreshed - {len(deals_df)} deals loaded"
            return instruments, sides, order_kinds, order_types, status
        else:
            return [], [], [], [], "No data loaded yet. Click 'Load Data' first."

    # If load button clicked
    if button_id == 'decay-load-button':
        if not load_clicks or load_clicks == 0:
            return [], [], [], [], ""

        try:
            print(f"[DEBUG] Loading data for date: {date_str}")

            # Fetch data
            deals_df, slices_dict = decay._build_dataset(date_str)

            if deals_df.empty:
                return [], [], [], [], f"No deals found for {date_str}"

            print(f"[DEBUG] deals_df columns: {deals_df.columns.tolist()}")
            print(f"[DEBUG] deals_df shape: {deals_df.shape}")

            # Convert timestamps to strings
            deals_df_copy = deals_df.copy()
            if 'time' in deals_df_copy.columns:
                deals_df_copy['time'] = deals_df_copy['time'].astype(str)

            # Store in server-side cache instead of dcc.Store
            DECAY_DATA_CACHE['deals'] = deals_df_copy.to_dict('records')

            # Convert slices_dict to dict format
            slices_data = {}
            for idx, df in slices_dict.items():
                df_dict = {}
                for col in df.columns:
                    col_data = df[col].tolist()
                    col_data = [float(x) if isinstance(x, (np.integer, np.floating)) else x for x in col_data]
                    df_dict[col] = col_data
                slices_data[str(idx)] = df_dict

            DECAY_DATA_CACHE['slices'] = slices_data

            # Generate filter options (sorted for better UX)
            instruments = [{'label': inst, 'value': inst} for inst in sorted(deals_df['instrument'].unique())]
            sides = [{'label': side, 'value': side} for side in sorted(deals_df['side'].unique())]
            order_kinds = [{'label': ok, 'value': ok} for ok in sorted(deals_df['orderKind'].dropna().unique())]
            order_types = [{'label': ot, 'value': ot} for ot in sorted(deals_df['orderType'].dropna().unique())]

            status = f"Loaded {len(deals_df)} deals with {len(slices_dict)} neighbor slices"
            print(f"[DEBUG] Data stored in server-side cache. Status: {status}")

            return instruments, sides, order_kinds, order_types, status

        except Exception as e:
            import traceback
            traceback.print_exc()
            return [], [], [], [], f"Error: {str(e)}"

    # Default return if no button was clicked
    return [], [], [], [], ""


@app.callback(
    Output('decay-graph', 'figure'),
    [Input('decay-instrument-filter', 'value'),
     Input('decay-side-filter', 'value'),
     Input('decay-orderkind-filter', 'value'),
     Input('decay-ordertype-filter', 'value')]
)
def update_decay_graph(instruments, sides, order_kinds, order_types):
    """Update graph based on filtered deals."""
    import pandas as pd

    # Get data from server-side cache
    deals_data = DECAY_DATA_CACHE['deals']
    slices_data = DECAY_DATA_CACHE['slices']

    print(f"[DEBUG] update_decay_graph called:")
    print(f"  deals_data is None: {deals_data is None}")
    print(f"  slices_data is None: {slices_data is None}")
    print(f"  instruments: {instruments}")
    print(f"  sides: {sides}")
    print(f"  order_kinds: {order_kinds}")
    print(f"  order_types: {order_types}")

    if deals_data is None or slices_data is None:
        # Return empty figure
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
        return fig

    # Convert back to DataFrame
    deals_df = pd.DataFrame(deals_data)
    print(f"[DEBUG] deals_df shape: {deals_df.shape}")
    print(f"[DEBUG] deals_df columns: {deals_df.columns.tolist()}")
    print(f"[DEBUG] slices_data keys: {list(slices_data.keys())[:5]}...")  # Show first 5

    # Apply filters
    mask = pd.Series([True] * len(deals_df))

    if instruments:
        mask &= deals_df['instrument'].isin(instruments)
    if sides:
        mask &= deals_df['side'].isin(sides)
    if order_kinds:
        mask &= deals_df['orderKind'].isin(order_kinds)
    if order_types:
        mask &= deals_df['orderType'].isin(order_types)

    filtered_deals = deals_df[mask]
    print(f"[DEBUG] filtered_deals shape: {filtered_deals.shape}")

    # Create figure
    fig = go.Figure()

    # Plot each filtered deal's neighbor data
    traces_added = 0
    for idx in filtered_deals.index:
        if str(idx) in slices_data:
            slice_dict = slices_data[str(idx)]
            t_from_deal = slice_dict.get('t_from_deal', [])

            # Get mid price
            ask_px = slice_dict.get('ask_px_0', [])
            bid_px = slice_dict.get('bid_px_0', [])

            if ask_px and bid_px and t_from_deal:
                mid_px = [(a + b) / 2 for a, b in zip(ask_px, bid_px)]

                deal = filtered_deals.loc[idx]
                fig.add_trace(go.Scatter(
                    x=t_from_deal,
                    y=mid_px,
                    mode='lines',
                    name=f"{deal['instrument']} {deal['side']}",
                    opacity=0.6,
                    line=dict(width=1.5)
                ))
                traces_added += 1
            else:
                print(f"[DEBUG] Skipping idx {idx}: ask_px={len(ask_px)}, bid_px={len(bid_px)}, t_from_deal={len(t_from_deal)}")

    print(f"[DEBUG] Added {traces_added} traces to graph")

    if traces_added == 0:
        # Add annotation if no traces
        fig.update_layout(
            annotations=[
                dict(
                    text="No data matches current filters",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=14, color="#e74c3c"),
                )
            ]
        )

    fig.update_layout(
        margin=dict(l=40, r=20, t=60, b=40),
        template="plotly_white",
        xaxis_title="Time from Deal (index steps)",
        yaxis_title="Mid Price",
        hovermode='closest',
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
        ),
    )

    return fig

if __name__ == '__main__':
    # Run the Dash app
    app.run(
        debug=True,
        host='0.0.0.0',
        port=8050
    )

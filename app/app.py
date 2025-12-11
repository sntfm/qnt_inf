import dash
from dash import dcc, html, callback_context
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go
from flask import redirect
from datetime import datetime

# Import widgets (will be developed later)
from widgets import latency, decay, flow

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
    'slices': None,
    'view': None
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

        # Container for flow widget
        html.Div([
            html.H2("PnL/Exposure",
                   style={'color': '#34495e', 'borderBottom': '2px solid', 'paddingBottom': 5}),
            html.Div(id='flow-widget-container'),
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
            return latency.create_filter_controls()
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
            # Load widget content with default settings (latest available date)
            return latency.get_widget_content()
        except Exception as e:
            import traceback
            return html.Div([
                html.P(f"Latency widget error: {str(e)}",
                      style={'color': '#e74c3c', 'fontStyle': 'italic'}),
                html.Pre(traceback.format_exc(),
                        style={'fontSize': '10px', 'color': '#95a5a6'})
            ])
    return html.Div()

# Latency widget - update on date selection
@app.callback(
    Output('latency-widget-container', 'children', allow_duplicate=True),
    Input('latency-date-input', 'date'),
    prevent_initial_call=True
)
def update_latency_on_date_change(date_str):
    """Update latency widget when date is selected."""
    try:
        print(f"[DEBUG] update_latency_on_date_change called with date_str={date_str}")
        return latency.get_widget_content(date_str=date_str)
    except Exception as e:
        import traceback
        print(f"Error in update_latency_on_date_change: {e}")
        return html.Div([
            html.P(f"Latency widget error: {str(e)}",
                  style={'color': '#e74c3c', 'fontStyle': 'italic'}),
            html.Pre(traceback.format_exc(),
                    style={'fontSize': '10px', 'color': '#95a5a6'})
         ])

@app.callback(
    Output('flow-widget-container', 'children'),
    Input('url', 'pathname')
)
def initialize_flow_widget(pathname):
    """Initialize flow widget on page load."""
    if pathname == '/app':
        try:
            # Load widget layout once on page load
            return flow.get_widget_layout(0)
        except Exception as e:
            import traceback
            return html.Div([
                html.P(f"Flow widget error: {str(e)}",
                      style={'color': '#e74c3c', 'fontStyle': 'italic'}),
                html.Pre(traceback.format_exc(),
                        style={'fontSize': '10px', 'color': '#95a5a6'})
            ])
    return html.Div()

# Flow widget callback - Load button
@app.callback(
    [Output('flow-graph', 'figure'),
     Output('flow-status', 'children'),
     Output('flow-instrument-filter', 'options')],
    Input('flow-load-button', 'n_clicks'),
    [State('flow-start-datetime', 'value'),
     State('flow-end-datetime', 'value'),
     State('flow-instrument-filter', 'value'),
     State('flow-legend-state', 'data')],
    prevent_initial_call=True
)
def load_flow_data(n_clicks, start_datetime, end_datetime, selected_instruments, legend_state):
    """Fetch flow metrics and plot them with two panes: PnL curves and Volume curves."""
    from plotly.subplots import make_subplots

    try:
        print(f"[DEBUG] Loading flow data for datetime range: {start_datetime} to {end_datetime}")
        print(f"[DEBUG] Selected instruments: {selected_instruments}")

        # Fetch available instruments for the dropdown
        available_instruments = flow._fetch_available_instruments(start_datetime, end_datetime)
        instrument_options = [{'label': inst, 'value': inst} for inst in available_instruments]

        # Fetch flow metrics
        instruments_filter = selected_instruments if selected_instruments else None
        metrics_df = flow._fetch_flow_metrics(start_datetime, end_datetime, instruments=instruments_filter)

        if metrics_df.empty:
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('PnL Curves (USD)', 'Exposure/Cost Curves (USD)'),
                vertical_spacing=0.15,
                specs=[[{"type": "xy"}], [{"type": "xy", "secondary_y": True}]],
                shared_xaxes='all'
            )
            fig.update_layout(
                margin=dict(l=40, r=20, t=80, b=40),
                template="plotly_white",
                height=800,
                showlegend=True,
                annotations=[
                    dict(
                        text=f"No data found for {start_datetime} to {end_datetime}",
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                        showarrow=False,
                        font=dict(size=14, color="#e74c3c"),
                    )
                ],
            )
            return fig, f"No data found for {start_datetime} to {end_datetime}", instrument_options

        print(f"[DEBUG] Found {len(metrics_df)} data points")

        # Group by timestamp and aggregate across all instruments (if multiple)
        # When multiple instruments are selected, we sum their values at each timestamp
        # to get the total portfolio metrics
        agg_df = metrics_df.groupby('ts').agg({
            'upnl_usd': 'sum',  # Sum UPNL across all instruments at each timestamp
            'rpnl_usd_total': 'sum',  # Sum per-bucket RPNL across instruments
            'tpnl_usd': 'sum',  # Sum total PnL across all instruments at each timestamp
            'vol_usd': 'sum',
            'cum_cost_usd': 'sum',
            'num_deals': 'sum'
        }).reset_index()

        # Create figure with 2 subplots (panes)
        # Second subplot has secondary y-axis for num_deals
        # shared_xaxes='all' synchronizes zooming and panning while keeping separate tick labels
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Mark to Market', 'Inventory'),
            vertical_spacing=0.15,
            specs=[[{"type": "xy"}], [{"type": "xy", "secondary_y": True}]],
            shared_xaxes='all'
        )

        # Initialize legend_state if None
        if legend_state is None:
            legend_state = {}

        # === Pane 1: PnL curves ===
        # Add UPNL trace
        fig.add_trace(
            go.Scatter(
                x=agg_df['ts'],
                y=agg_df['upnl_usd'],
                mode='lines',
                name='UPNL',
                line=dict(color='#3498db', width=2),
                hovertemplate='<b>%{x}</b><br>UPNL: $%{y:,.2f}<extra></extra>',
                visible=True if legend_state.get('UPNL', True) else 'legendonly'
            ),
            row=1, col=1
        )

        # Add RPNL trace (per bucket)
        fig.add_trace(
            go.Scatter(
                x=agg_df['ts'],
                y=agg_df['rpnl_usd_total'],
                mode='lines',
                name='RPNL',
                line=dict(color='#e74c3c', width=2),
                hovertemplate='<b>%{x}</b><br>RPNL: $%{y:,.2f}<extra></extra>',
                visible=True if legend_state.get('RPNL', True) else 'legendonly'
            ),
            row=1, col=1
        )

        # Add Total PnL trace (using tpnl_usd from database)
        fig.add_trace(
            go.Scatter(
                x=agg_df['ts'],
                y=agg_df['tpnl_usd'],
                mode='lines',
                name='TPNL',
                line=dict(color='#2ecc71', width=2.5),
                hovertemplate='<b>%{x}</b><br>TPNL: $%{y:,.2f}<extra></extra>',
                visible=True if legend_state.get('TPNL', True) else 'legendonly'
            ),
            row=1, col=1
        )

        # === Pane 2: Volume curves ===
        # Add Volume trace
        fig.add_trace(
            go.Scatter(
                x=agg_df['ts'],
                y=agg_df['vol_usd'],
                mode='lines',
                name='Deal Volume',
                line=dict(color='#9b59b6', width=2),
                hovertemplate='<b>%{x}</b><br>Volume: $%{y:,.2f}<extra></extra>',
                visible=True if legend_state.get('Deal Volume', True) else 'legendonly'
            ),
            row=2, col=1
        )

        # Add Cumulative Cost trace
        fig.add_trace(
            go.Scatter(
                x=agg_df['ts'],
                y=agg_df['cum_cost_usd'],
                mode='lines',
                name='Inventory Value',
                line=dict(color='#f39c12', width=2),
                hovertemplate='<b>%{x}</b><br>Cum. Cost: $%{y:,.2f}<extra></extra>',
                visible=True if legend_state.get('Inventory Value', True) else 'legendonly'
            ),
            row=2, col=1,
            secondary_y=False
        )

        # Add Number of Deals trace on secondary y-axis
        fig.add_trace(
            go.Scatter(
                x=agg_df['ts'],
                y=agg_df['num_deals'],
                mode='lines',
                name='# Deals',
                line=dict(color='#95a5a6', width=1.5, dash='dot'),
                hovertemplate='<b>%{x}</b><br># Deals: %{y}<extra></extra>',
                visible=True if legend_state.get('# Deals', True) else 'legendonly'
            ),
            row=2, col=1,
            secondary_y=True
        )

        # Update layout
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
            hovermode='x unified',  # Show unified hover across all subplots
            hoverdistance=100,  # Distance to show hover
            spikedistance=1000  # Distance to show spike lines
        )

        # Update axes labels and enable tick labels for both x-axes
        # Add spike lines to show vertical crosshair on both subplots
        # matches='x' ensures spikes appear on all subplots sharing the x-axis

        # X axis (pane 1)
        fig.update_xaxes(
            title_text="",
            showticklabels=True,
            matches='x',
            showspikes=True,
            spikemode='across',
            spikesnap='cursor',
            spikethickness=1,
            spikecolor='rgba(0,0,0,0.3)',
            row=1, col=1
        )

        fig.update_yaxes(title_text="Instrument PnL, $", row=1, col=1, secondary_y=False)

        # X axis (pane 2)
        fig.update_xaxes(
            title_text="Time",
            showticklabels=True,
            matches='x',
            showspikes=True,
            spikemode='across',
            spikesnap='cursor',
            spikethickness=1,
            spikecolor='rgba(0,0,0,0.3)',
            row=2, col=1
        )

        # Y axis (pane 2)
        fig.update_yaxes(title_text="Inventory Value/Deal Volume, $", row=2, col=1, secondary_y=False)

        # Y axis secondary (pane 2)
        fig.update_yaxes(
            title_text="Deals, #",
            showspikes=False,
            spikemode='across',
            spikesnap='cursor',
            spikethickness=1,
            spikecolor='rgba(0,0,0,0.3)',
            secondary_y=True,
            row=2, col=1
        )

        # Add zero reference lines to PnL pane
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5, row=1, col=1)

        # Calculate summary stats
        total_pnl = agg_df['tpnl_usd'].iloc[-1] if len(agg_df) > 0 else 0
        total_upnl = agg_df['upnl_usd'].iloc[-1] if len(agg_df) > 0 else 0
        total_rpnl = agg_df['rpnl_usd_total'].sum()  # Sum of per-bucket RPNL
        total_volume = agg_df['vol_usd'].sum()
        total_deals = int(agg_df['num_deals'].sum())

        instruments_str = f"{len(selected_instruments)} instruments" if selected_instruments else "All instruments"
        status = html.Div([
            instruments_str,
            html.Br(),
            f"PnL:",
            html.Br(),
            f"\u00A0\u00A0\u00A0\u00A0Total: ${total_pnl:,.2f}",
            html.Br(),
            f"\u00A0\u00A0\u00A0\u00A0Unrealized: ${total_upnl:,.2f}",
            html.Br(),
            f"\u00A0\u00A0\u00A0\u00A0Realized: ${total_rpnl:,.2f}",
            html.Br(),
            f"Deal Volume: ${total_volume:,.2f}",
            html.Br(),
            f"Deals: {total_deals:,}"
        ], style={'textAlign': 'left'})

        return fig, status, instrument_options

    except Exception as e:
        import traceback
        traceback.print_exc()

        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('PnL Curves (USD)', 'Exposure/Cost Curves (USD)'),
            vertical_spacing=0.15,
            specs=[[{"type": "xy"}], [{"type": "xy", "secondary_y": True}]],
            shared_xaxes='all'
        )
        fig.update_layout(
            margin=dict(l=40, r=20, t=80, b=40),
            template="plotly_white",
            height=800,
            showlegend=True,
            annotations=[
                dict(
                    text=f"Error: {str(e)}",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=14, color="#e74c3c"),
                )
            ],
        )
        return fig, f"Error: {str(e)}", []

# Flow widget callback - Persist legend state
@app.callback(
    Output('flow-legend-state', 'data'),
    Input('flow-graph', 'restyleData'),
    State('flow-legend-state', 'data'),
    prevent_initial_call=True
)
def update_legend_state(restyle_data, current_state):
    """Capture legend clicks and persist the visibility state."""
    if restyle_data is None:
        raise PreventUpdate

    # Initialize state if None
    if current_state is None:
        current_state = {}

    # restyle_data format: [{'visible': [True/False/'legendonly']}, [trace_index]]
    # or [{'visible': True/False/'legendonly'}, [trace_index1, trace_index2, ...]]
    try:
        updates = restyle_data[0]
        trace_indices = restyle_data[1] if len(restyle_data) > 1 else []

        # Map trace indices to trace names
        trace_names = ['UPNL', 'RPNL', 'TPNL', 'Deal Volume', 'Inventory Value', '# Deals']

        if 'visible' in updates:
            visible_value = updates['visible']

            # Handle both single value and list of values
            if isinstance(visible_value, list):
                for idx, trace_idx in enumerate(trace_indices):
                    if trace_idx < len(trace_names):
                        # Convert 'legendonly' to False, True stays True
                        current_state[trace_names[trace_idx]] = visible_value[idx] is True
            else:
                for trace_idx in trace_indices:
                    if trace_idx < len(trace_names):
                        # Convert 'legendonly' to False, True stays True
                        current_state[trace_names[trace_idx]] = visible_value is True

        return current_state
    except Exception as e:
        print(f"[DEBUG] Error updating legend state: {e}")
        raise PreventUpdate

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

# Decay widget callback - Plot button
@app.callback(
    [Output('decay-graph', 'figure'),
     Output('decay-instrument-filter', 'options'),
     Output('decay-side-filter', 'options'),
     Output('decay-orderkind-filter', 'options'),
     Output('decay-ordertype-filter', 'options'),
     Output('decay-tif-filter', 'options'),
     Output('decay-status', 'children')],
    [Input('decay-plot-button', 'n_clicks'),
     Input('decay-start-datetime', 'value'),
     Input('decay-end-datetime', 'value')],
    [State('decay-view-dropdown', 'value'),
     State('decay-instrument-filter', 'value'),
     State('decay-side-filter', 'value'),
     State('decay-orderkind-filter', 'value'),
     State('decay-ordertype-filter', 'value'),
     State('decay-tif-filter', 'value'),
     State('decay-aggregate-dropdown', 'value')],
    prevent_initial_call=True
)
def plot_decay_data(n_clicks, start_datetime, end_datetime, view,
                    instruments, sides, order_kinds, order_types, tifs, aggregate):
    """Fetch filtered data and plot it."""
    import pandas as pd
    import numpy as np
    import plotly.express as px
    from dash import callback_context, no_update

    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update

    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    # If triggered by datetime change (Enter or Blur), only update filters
    if trigger_id in ['decay-start-datetime', 'decay-end-datetime']:
        print(f"[DEBUG] Fetching filter options for {start_datetime} to {end_datetime}")
        options = decay.get_filter_options(start_datetime, end_datetime)

        status = f"Filters updated for range {start_datetime} to {end_datetime}"
        return (
            no_update, # graph
            options.get('instruments', []),
            options.get('sides', []),
            options.get('order_kinds', []),
            options.get('order_types', []),
            options.get('tifs', []),
            status
        )
    
    # Otherwise (Plot button), proceed with full data fetch and plot
    
    # Set y-axis label based on view
    yaxis_label = "Return" if view == "return" else "PnL, $"
    
    try:
        print(f"[DEBUG] Plotting data for datetime range: {start_datetime} to {end_datetime}, view: {view}")
        print(f"[DEBUG] Filters: instruments={instruments}, sides={sides}, order_kinds={order_kinds}, order_types={order_types}, tifs={tifs}")
        print(f"[DEBUG] Aggregation mode: {aggregate}")
        
        # OPTIMIZATION: Use database aggregation for grouped views (99% less memory!)
        # Only fetch raw slices for 'none' (show all) mode
        use_db_aggregation = aggregate in ['instrument', 'side', 'day', 'hour']
        
        if use_db_aggregation:
            print(f"[DEBUG] Using DATABASE aggregation (memory efficient!)")
            
            # Build filters dict
            filters_dict = {
                'instruments': instruments if instruments else [],
                'sides': sides if sides else [],
                'order_kinds': order_kinds if order_kinds else [],
                'order_types': order_types if order_types else [],
                'tifs': tifs if tifs else []
            }
            
            # Fetch pre-aggregated data from database
            agg_df = decay._fetch_aggregated_slices(
                start_datetime, end_datetime, view, aggregate, filters_dict
            )
            
            # Also fetch deals for filter options (much smaller query)
            deals_df = decay._fetch_deals(start_datetime, end_datetime)
            
            if deals_df.empty or agg_df.empty:
                fig = go.Figure()
                fig.update_layout(
                    margin=dict(l=40, r=20, t=60, b=40),
                    template="plotly_white",
                    xaxis_title="Time from Deal (sec)",
                    yaxis_title=yaxis_label,
                    annotations=[
                        dict(
                            text=f"No deals found for {start_datetime} to {end_datetime}",
                            xref="paper",
                            yref="paper",
                            x=0.5,
                            y=0.5,
                            showarrow=False,
                            font=dict(size=14, color="#e74c3c"),
                        )
                    ],
                )
                return fig, [], [], [], [], [], f"No deals found for {start_datetime} to {end_datetime}"
            
            print(f"[DEBUG] Fetched {len(agg_df)} aggregated rows (vs millions of raw slices!)")
            
        else:
            # 'none' mode - show all individual lines
            # Validate date range to prevent memory issues
            from datetime import datetime
            dt_start = datetime.strptime(start_datetime.split()[0], '%Y-%m-%d')
            dt_end = datetime.strptime(end_datetime.split()[0], '%Y-%m-%d')
            num_days = (dt_end - dt_start).days + 1
            
            if num_days > 3:
                fig = go.Figure()
                fig.update_layout(
                    margin=dict(l=40, r=20, t=60, b=40),
                    template="plotly_white",
                    xaxis_title="Time from Deal (sec)",
                    yaxis_title=yaxis_label,
                    annotations=[
                        dict(
                            text=f"'Show all' mode limited to 3 days.<br>You selected {num_days} days.<br>Please use grouping (Instrument/Side/Day/Hour) for larger ranges.",
                            xref="paper",
                            yref="paper",
                            x=0.5,
                            y=0.5,
                            showarrow=False,
                            font=dict(size=14, color="#e74c3c"),
                        )
                    ],
                )
                return fig, [], [], [], [], [], f"'Show all' mode limited to 3 days (you selected {num_days} days)"
            
            print(f"[DEBUG] Using PYTHON aggregation (fetching all slices...)")
            
            # Fetch data for the datetime range
            deals_df, slices_dict = decay._build_dataset(start_datetime, end_datetime, view=view)
            print(f"[DEBUG] Dataset fetched successfully")
            
            if deals_df.empty:
                fig = go.Figure()
                fig.update_layout(
                    margin=dict(l=40, r=20, t=60, b=40),
                    template="plotly_white",
                    xaxis_title="Time from Deal (sec)",
                    yaxis_title=yaxis_label,
                    annotations=[
                        dict(
                            text=f"No deals found for {start_datetime} to {end_datetime}",
                            xref="paper",
                            yref="paper",
                            x=0.5,
                            y=0.5,
                            showarrow=False,
                            font=dict(size=14, color="#e74c3c"),
                        )
                    ],
                )
                return fig, [], [], [], [], [], f"No deals found for {start_datetime} to {end_datetime}"
            
            print(f"[DEBUG] Found {len(deals_df)} deals, {len(slices_dict)} slices")
        
        # Generate filter options from ALL data (before filtering)
        all_instruments = [{'label': inst, 'value': inst} for inst in sorted(deals_df['instrument'].unique())]
        all_sides = [{'label': side, 'value': side} for side in sorted(deals_df['side'].unique())]
        all_order_kinds = [{'label': ok, 'value': ok} for ok in sorted(deals_df['orderKind'].dropna().unique())]
        all_order_types = [{'label': ot, 'value': ot} for ot in sorted(deals_df['orderType'].dropna().unique())]
        all_tifs = [{'label': tif, 'value': tif} for tif in sorted(deals_df['tif'].dropna().unique())]
        
        # For DB aggregation mode, we already have filtered data
        if use_db_aggregation:
            # Apply filters to deals for display purposes only
            mask = pd.Series([True] * len(deals_df))
            if instruments:
                mask &= deals_df['instrument'].isin(instruments)
            if sides:
                mask &= deals_df['side'].isin(sides)
            if order_kinds:
                mask &= deals_df['orderKind'].isin(order_kinds)
            if order_types:
                mask &= deals_df['orderType'].isin(order_types)
            if tifs:
                mask &= deals_df['tif'].isin(tifs)
            
            filtered_deals = deals_df[mask]
            print(f"[DEBUG] After filtering: {len(filtered_deals)} deals")
        else:
            # Apply filters to deals for Python aggregation
            mask = pd.Series([True] * len(deals_df))
            if instruments:
                mask &= deals_df['instrument'].isin(instruments)
            if sides:
                mask &= deals_df['side'].isin(sides)
            if order_kinds:
                mask &= deals_df['orderKind'].isin(order_kinds)
            if order_types:
                mask &= deals_df['orderType'].isin(order_types)
            if tifs:
                mask &= deals_df['tif'].isin(tifs)
            
            filtered_deals = deals_df[mask]
            print(f"[DEBUG] After filtering: {len(filtered_deals)} deals")
        
        if filtered_deals.empty:
            fig = go.Figure()
            fig.update_layout(
                margin=dict(l=40, r=20, t=60, b=40),
                template="plotly_white",
                xaxis_title="Time from Deal (sec)",
                yaxis_title=yaxis_label,
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
                ],
            )
            status = f"Loaded {len(deals_df)} deals, 0 match filters"
            return fig, all_instruments, all_sides, all_order_kinds, all_order_types, all_tifs, status
        
        # Create figure
        fig = go.Figure()
        
        # Create color map for instruments
        unique_instruments = sorted(filtered_deals['instrument'].unique())
        colors = px.colors.qualitative.Plotly
        color_map = {inst: colors[i % len(colors)] for i, inst in enumerate(unique_instruments)}
        
        # Track which instruments have been added to legend
        legend_added = set()
        
        # Select the correct column based on view
        y_column = 'pnl_usd' if view == 'usd_pnl' else 'ret'
        
        traces_added = 0
        
        # Helper function to compute weighted average using pandas (optimized)
        def compute_weighted_avg_pandas(group_deals, group_name, group_value):
            """Compute weighted average for a group using efficient pandas operations."""
            import time
            import gc
            start_time = time.time()
            
            # Filter slices_dict to only include deals in this group (avoid iteration)
            group_indices = set(group_deals.index)
            relevant_slices = {idx: slices_dict[idx] for idx in group_indices if idx in slices_dict and not slices_dict[idx].empty}
            
            if not relevant_slices:
                return None, None
            
            # OPTIMIZED: Build lists more efficiently using list comprehensions
            # This reduces memory fragmentation compared to repeated extend() calls
            all_data = []
            for idx, slice_df in relevant_slices.items():
                deal = group_deals.loc[idx]
                weight = deal['amt_usd'] if 'amt_usd' in deal and pd.notna(deal['amt_usd']) else 0.0
                
                if weight > 0:
                    # Extract arrays directly (faster than copying DataFrame)
                    t_arr = slice_df['t_from_deal'].values
                    y_arr = slice_df[y_column].values
                    
                    # Build tuples for this slice
                    for t, y in zip(t_arr, y_arr):
                        all_data.append((t, y, weight))
            
            if not all_data:
                return None, None
            
            # Create DataFrame from list of tuples (more memory efficient)
            combined = pd.DataFrame(all_data, columns=['t_from_deal', y_column, 'weight'])
            
            # Free intermediate data
            del all_data
            gc.collect()
            
            print(f"[DEBUG] {group_name}={group_value}: Built combined DataFrame with {len(combined)} rows from {len(relevant_slices)} deals")
            
            # Compute weighted average at each t_from_deal using pandas groupby
            # Formula: weighted_avg = sum(value * weight) / sum(weight)
            combined['weighted_value'] = combined[y_column] * combined['weight']
            
            grouped = combined.groupby('t_from_deal', sort=True).agg({
                'weighted_value': 'sum',
                'weight': 'sum'
            })
            
            grouped['weighted_avg'] = grouped['weighted_value'] / grouped['weight']
            
            # Free combined DataFrame
            del combined
            gc.collect()
            
            elapsed = time.time() - start_time
            print(f"[DEBUG] {group_name}={group_value}: Computed weighted avg in {elapsed:.2f}s")
            
            return grouped.index.tolist(), grouped['weighted_avg'].tolist()
        
        if use_db_aggregation:
            # Plot directly from database-aggregated results (FAST!)
            print(f"[DEBUG] Plotting from database-aggregated data")
            
            # Group by the group_key column
            unique_groups = sorted(agg_df['group_key'].unique())
            
            # Color palettes based on aggregation type
            if aggregate == 'instrument':
                colors_palette = px.colors.qualitative.Bold
            elif aggregate == 'side':
                colors_palette = {'buy': '#00D9FF', 'BUY': '#00D9FF', 'sell': '#FF6B9D', 'SELL': '#FF6B9D'}
            elif aggregate == 'day':
                colors_palette = px.colors.qualitative.Vivid
            elif aggregate == 'hour':
                colors_palette = px.colors.qualitative.T10
            else:
                colors_palette = px.colors.qualitative.Plotly
            
            for idx, group_val in enumerate(unique_groups):
                group_data = agg_df[agg_df['group_key'] == group_val]
                
                # Extract x and y data
                x_data = group_data['t_from_deal'].tolist()
                y_data = group_data['weighted_avg'].tolist()
                
                if not x_data or not y_data:
                    continue
                
                # Get color
                if isinstance(colors_palette, dict):
                    color = colors_palette.get(group_val, '#636EFA')
                else:
                    color = colors_palette[idx % len(colors_palette)]
                
                # Format group name
                if aggregate == 'hour':
                    group_name = f"Hour {group_val:02d}" if isinstance(group_val, int) else str(group_val)
                else:
                    group_name = str(group_val)
                
                # Add trace
                fig.add_trace(go.Scatter(
                    x=x_data,
                    y=y_data,
                    mode='lines',
                    name=group_name,
                    legendgroup=str(group_val),
                    showlegend=True,
                    opacity=0.9,
                    line=dict(width=2.5, color=color),
                    hovertemplate=f"<b>{group_name}</b><br>t=%{{x}}s<br>y=%{{y:.4f}}<extra></extra>"
                ))
                traces_added += 1
            
            print(f"[DEBUG] Added {traces_added} traces from database aggregation")
            
        elif aggregate == 'instrument':
            # Weighted average by amt_usd per instrument
            # Use Bold color palette for instruments
            inst_colors = px.colors.qualitative.Bold
            inst_color_map = {inst: inst_colors[i % len(inst_colors)] for i, inst in enumerate(unique_instruments)}
            
            for instrument in unique_instruments:
                inst_deals = filtered_deals[filtered_deals['instrument'] == instrument]
                
                all_t, weighted_avg_y = compute_weighted_avg_pandas(inst_deals, 'instrument', instrument)
                
                if all_t is None:
                    continue
                
                # Plot the weighted average line for this instrument
                fig.add_trace(go.Scatter(
                    x=all_t,
                    y=weighted_avg_y,
                    mode='lines',
                    name=f"{instrument}",
                    legendgroup=instrument,
                    showlegend=True,
                    opacity=0.9,
                    line=dict(width=2.5, color=inst_color_map.get(instrument, '#636EFA')),
                    hovertemplate=f"<b>{instrument}</b><br>t=%{{x}}s<br>y=%{{y:.4f}}<extra></extra>"
                ))
                traces_added += 1
                
        elif aggregate == 'side':
            # Weighted average by amt_usd per side
            unique_sides = sorted(filtered_deals['side'].unique())
            # Use more vibrant colors for buy/sell
            side_colors = {
                'buy': '#00D9FF', 'BUY': '#00D9FF',  # Bright cyan for buy
                'sell': '#FF6B9D', 'SELL': '#FF6B9D'  # Bright pink for sell
            }
            
            for side in unique_sides:
                side_deals = filtered_deals[filtered_deals['side'] == side]
                
                all_t, weighted_avg_y = compute_weighted_avg_pandas(side_deals, 'side', side)
                
                if all_t is None:
                    continue
                
                # Plot the weighted average line for this side
                fig.add_trace(go.Scatter(
                    x=all_t,
                    y=weighted_avg_y,
                    mode='lines',
                    name=f"{side}",
                    legendgroup=side,
                    showlegend=True,
                    opacity=0.9,
                    line=dict(width=2.5, color=side_colors.get(side, '#636EFA')),
                    hovertemplate=f"<b>{side}</b><br>t=%{{x}}s<br>y=%{{y:.4f}}<extra></extra>"
                ))
                traces_added += 1
                
        elif aggregate == 'day':
            # Weighted average by amt_usd per day
            # OPTIMIZED: Add 'day' column directly instead of copying entire DataFrame
            filtered_deals['day'] = pd.to_datetime(filtered_deals['time']).dt.date
            unique_days = sorted(filtered_deals['day'].unique())
            
            # Create color map for days - use Vivid palette for vibrant colors
            day_colors = px.colors.qualitative.Vivid
            day_color_map = {day: day_colors[i % len(day_colors)] for i, day in enumerate(unique_days)}
            
            for day in unique_days:
                day_deals = filtered_deals[filtered_deals['day'] == day]
                
                all_t, weighted_avg_y = compute_weighted_avg_pandas(day_deals, 'day', day)
                
                if all_t is None:
                    continue
                
                # Plot the weighted average line for this day
                fig.add_trace(go.Scatter(
                    x=all_t,
                    y=weighted_avg_y,
                    mode='lines',
                    name=f"{day}",
                    legendgroup=str(day),
                    showlegend=True,
                    opacity=0.9,
                    line=dict(width=2.5, color=day_color_map.get(day, '#636EFA')),
                    hovertemplate=f"<b>{day}</b><br>t=%{{x}}s<br>y=%{{y:.4f}}<extra></extra>"
                ))
                traces_added += 1
            
            # Clean up temporary column
            filtered_deals.drop(columns=['day'], inplace=True)
                
        elif aggregate == 'hour':
            # Weighted average by amt_usd per hour
            # OPTIMIZED: Add 'hour' column directly instead of copying entire DataFrame
            filtered_deals['hour'] = pd.to_datetime(filtered_deals['time']).dt.hour
            unique_hours = sorted(filtered_deals['hour'].unique())
            
            # Create color map for hours - use T10 palette for vibrant colors
            hour_colors = px.colors.qualitative.T10
            hour_color_map = {hour: hour_colors[i % len(hour_colors)] for i, hour in enumerate(unique_hours)}
            
            for hour in unique_hours:
                hour_deals = filtered_deals[filtered_deals['hour'] == hour]
                
                all_t, weighted_avg_y = compute_weighted_avg_pandas(hour_deals, 'hour', hour)
                
                if all_t is None:
                    continue
                
                # Plot the weighted average line for this hour
                fig.add_trace(go.Scatter(
                    x=all_t,
                    y=weighted_avg_y,
                    mode='lines',
                    name=f"Hour {hour:02d}",
                    legendgroup=f"hour_{hour}",
                    showlegend=True,
                    opacity=0.9,
                    line=dict(width=2.5, color=hour_color_map.get(hour, '#636EFA')),
                    hovertemplate=f"<b>Hour {hour:02d}</b><br>t=%{{x}}s<br>y=%{{y:.4f}}<extra></extra>"
                ))
                traces_added += 1
            
            # Clean up temporary column
            filtered_deals.drop(columns=['hour'], inplace=True)
        else:
            # No aggregation - show all individual lines
            # OPTIMIZATION: Batch all deals per instrument into single trace with NaN separators
            # This is MUCH faster than creating 1,500 individual traces!
            print(f"[DEBUG] Batching {len(filtered_deals)} deals into traces by instrument")
            
            import time
            start_time = time.time()
            
            # Group deals by instrument
            for instrument in unique_instruments:
                inst_deals = filtered_deals[filtered_deals['instrument'] == instrument]
                
                # Collect all x and y data for this instrument with NaN separators
                all_x = []
                all_y = []
                
                for idx in inst_deals.index:
                    if idx in slices_dict:
                        slice_df = slices_dict[idx]
                        
                        if slice_df.empty or y_column not in slice_df.columns:
                            continue
                        
                        # Append this deal's data
                        all_x.extend(slice_df['t_from_deal'].tolist())
                        all_y.extend(slice_df[y_column].tolist())
                        
                        # Add NaN separator to create gap between deals
                        all_x.append(None)
                        all_y.append(None)
                
                if all_x and all_y:
                    # Create single trace for all deals of this instrument
                    fig.add_trace(go.Scatter(
                        x=all_x,
                        y=all_y,
                        mode='lines',
                        name=instrument,
                        legendgroup=instrument,
                        showlegend=True,
                        opacity=0.6,
                        line=dict(width=1.5, color=color_map.get(instrument, '#636EFA')),
                        hovertemplate=f"<b>{instrument}</b><br>t=%{{x}}s<br>y=%{{y:.4f}}<extra></extra>",
                        connectgaps=False  # Don't connect across NaN gaps
                    ))
                    traces_added += 1
            
            elapsed = time.time() - start_time
            print(f"[DEBUG] Created {traces_added} batched traces in {elapsed:.2f}s (vs {len(filtered_deals)} individual traces)")
        
        print(f"[DEBUG] Added {traces_added} traces to graph")
        
        # Add reference lines at x=0 and y=0
        if traces_added > 0:
            fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.5, annotation_text="t=0")
            fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        
        fig.update_layout(
            margin=dict(l=40, r=20, t=60, b=40),
            template="plotly_white",
            xaxis_title="Time from Deal (sec)",
            yaxis_title=yaxis_label,
            hovermode='closest',
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=1.01,
                bgcolor="rgba(255, 255, 255, 0.8)",
                bordercolor="rgba(0, 0, 0, 0.2)",
                borderwidth=1
            ),
            # Enable legend click interactions
            # Single click: toggle visibility (hide/show)
            # Double click: isolate trace (show only that one)
        )

        status = f"Plotted {len(filtered_deals)} of {len(deals_df)} deals"
        
        # CRITICAL: Force garbage collection to free memory after processing
        # This is essential for handling 5+ days of data on remote servers
        import gc
        gc.collect()
        
        return fig, all_instruments, all_sides, all_order_kinds, all_order_types, all_tifs, status
        
    except Exception as e:
        import traceback
        traceback.print_exc()

        fig = go.Figure()
        fig.update_layout(
            margin=dict(l=40, r=20, t=60, b=40),
            template="plotly_white",
            xaxis_title="Time from Deal (sec)",
            yaxis_title=yaxis_label,
            annotations=[
                dict(
                    text=f"Error: {str(e)}",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=14, color="#e74c3c"),
                )
            ],
        )
        return fig, [], [], [], [], [], f"Error: {str(e)}"



if __name__ == '__main__':
    # Run the Dash app
    app.run(
        debug=False,
        host='0.0.0.0',
        port=8050
    )

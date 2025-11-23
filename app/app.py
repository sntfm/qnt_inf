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
        
        # Fetch data for the datetime range
        print(f"[DEBUG] Fetching dataset...")
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
        
        # Apply filters to deals
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
            start_time = time.time()
            
            # Collect all slices for this group
            all_slices = []
            
            for idx in group_deals.index:
                if idx not in slices_dict:
                    continue
                
                slice_df = slices_dict[idx]
                if slice_df.empty:
                    continue
                
                deal = group_deals.loc[idx]
                weight = deal['amt_usd'] if 'amt_usd' in deal and pd.notna(deal['amt_usd']) else 0.0
                
                if weight > 0:
                    # Add weight and group identifier to slice
                    slice_copy = slice_df[['t_from_deal', y_column]].copy()
                    slice_copy['weight'] = weight
                    slice_copy['deal_idx'] = idx
                    all_slices.append(slice_copy)
            
            if not all_slices:
                return None, None
            
            # Combine all slices into single DataFrame
            combined = pd.concat(all_slices, ignore_index=True)
            print(f"[DEBUG] {group_name}={group_value}: Combined {len(all_slices)} slices into {len(combined)} rows")
            
            # Compute weighted average at each t_from_deal using pandas groupby
            # Formula: weighted_avg = sum(value * weight) / sum(weight)
            combined['weighted_value'] = combined[y_column] * combined['weight']
            
            grouped = combined.groupby('t_from_deal').agg({
                'weighted_value': 'sum',
                'weight': 'sum'
            })
            
            grouped['weighted_avg'] = grouped['weighted_value'] / grouped['weight']
            
            # Sort by t_from_deal
            grouped = grouped.sort_index()
            
            elapsed = time.time() - start_time
            print(f"[DEBUG] {group_name}={group_value}: Computed weighted avg in {elapsed:.2f}s")
            
            return grouped.index.tolist(), grouped['weighted_avg'].tolist()
        
        if aggregate == 'instrument':
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
            # Add 'day' column to filtered_deals
            filtered_deals_copy = filtered_deals.copy()
            filtered_deals_copy['day'] = pd.to_datetime(filtered_deals_copy['time']).dt.date
            unique_days = sorted(filtered_deals_copy['day'].unique())
            
            # Create color map for days - use Vivid palette for vibrant colors
            day_colors = px.colors.qualitative.Vivid
            day_color_map = {day: day_colors[i % len(day_colors)] for i, day in enumerate(unique_days)}
            
            for day in unique_days:
                day_deals = filtered_deals_copy[filtered_deals_copy['day'] == day]
                
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
                
        elif aggregate == 'hour':
            # Weighted average by amt_usd per hour
            # Add 'hour' column to filtered_deals
            filtered_deals_copy = filtered_deals.copy()
            filtered_deals_copy['hour'] = pd.to_datetime(filtered_deals_copy['time']).dt.hour
            unique_hours = sorted(filtered_deals_copy['hour'].unique())
            
            # Create color map for hours - use T10 palette for vibrant colors
            hour_colors = px.colors.qualitative.T10
            hour_color_map = {hour: hour_colors[i % len(hour_colors)] for i, hour in enumerate(unique_hours)}
            
            for hour in unique_hours:
                hour_deals = filtered_deals_copy[filtered_deals_copy['hour'] == hour]
                
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
        else:
            # No aggregation - show all individual lines
            for idx in filtered_deals.index:
                if idx in slices_dict:
                    slice_df = slices_dict[idx]
                    
                    if slice_df.empty or y_column not in slice_df.columns:
                        continue
                    
                    t_from_deal = slice_df['t_from_deal'].tolist()
                    y_data = slice_df[y_column].tolist()
                    
                    if y_data and t_from_deal:
                        deal = filtered_deals.loc[idx]
                        instrument = deal['instrument']
                        
                        # Only show instrument name in legend (once per instrument)
                        show_legend = instrument not in legend_added
                        if show_legend:
                            legend_added.add(instrument)
                        
                        fig.add_trace(go.Scatter(
                            x=t_from_deal,
                            y=y_data,
                            mode='lines',
                            name=instrument,
                            legendgroup=instrument,
                            showlegend=show_legend,
                            opacity=0.6,
                            line=dict(width=1.5, color=color_map.get(instrument, '#636EFA')),
                            hovertemplate=f"<b>{instrument}</b><br>t=%{{x}}s<br>y=%{{y:.4f}}<extra></extra>"
                        ))
                        traces_added += 1
        
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

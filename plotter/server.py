import pandas as pd
import sys
from pathlib import Path
import io

import holoviews as hv
import panel as pn
import holoviews.operation.datashader as hd

import warnings
warnings.filterwarnings("ignore", category=UserWarning)


# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

hv.extension('bokeh')
pn.extension(sizing_mode='stretch_both')


def build_dashboard(start_ts = '2025-10-27 00:00:00.000000',
                    end_ts = '2025-10-27 23:59:59.999999',
                    path='decay_df.parquet', 
                    resample='1s'):

    # --- Load or generate decay_df ---
    decay_df = pd.read_parquet(path)

    def _to_df(x):
        if isinstance(x, pd.DataFrame):
            return x
        if isinstance(x, str) and x:
            try:
                return pd.read_json(io.StringIO(x))
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    if 'before' in decay_df.columns:
        decay_df['before'] = decay_df['before'].apply(_to_df)
    if 'after' in decay_df.columns:
        decay_df['after'] = decay_df['after'].apply(_to_df)
    if 'before_usd' in decay_df.columns:
        decay_df['before_usd'] = decay_df['before_usd'].apply(_to_df)
    if 'after_usd' in decay_df.columns:
        decay_df['after_usd'] = decay_df['after_usd'].apply(_to_df)

    if decay_df is None or decay_df.empty:
        return pn.pane.Markdown("**No deals data found for the specified time range.**")

    # --- Widgets ---
    def _first_existing(cols):
        for c in cols:
            if c in decay_df.columns:
                return c
        return None

    # Column name candidates
    col_time = _first_existing(['time', 'ts', 'timestamp'])
    col_instrument = _first_existing(['instrument', 'symbol'])
    col_side = _first_existing(['side', 'orderSide'])
    col_amt = _first_existing(['amt', 'amount', 'qty', 'quantity'])
    col_px = _first_existing(['px', 'price'])
    col_order_kind = _first_existing(['orderKind', 'kind'])
    col_order_type = _first_existing(['orderType', 'type'])
    col_tif = _first_existing(['tif', 'timeInForce'])
    col_order_status = _first_existing(['orderStatus', 'status'])

    # Instrument
    if col_instrument:
        instruments = sorted(decay_df[col_instrument].dropna().unique().tolist())
    else:
        instruments = []
    instrument_options = ['All'] + instruments if instruments else ['All']
    w_instrument = pn.widgets.Select(
        name='Instrument',
        options=instrument_options,
        value='All',
        width=220,
        height=40,
        margin=(5, 10, 5, 10)
    )

    # Time range
    if col_time:
        _dt_series = pd.to_datetime(decay_df[col_time], utc=True, errors='coerce')
        tmin = pd.to_datetime(_dt_series.min())
        tmax = pd.to_datetime(_dt_series.max())
        w_time = pn.widgets.DatetimeRangeSlider(
            name='Time',
            start=tmin,
            end=tmax,
            value=(tmin, tmax),
            width=450,
            height=40,
            margin=(5, 10, 5, 10)
        )
    else:
        w_time = None

    # Side
    if col_side:
        sides = ['All'] + sorted([str(x) for x in decay_df[col_side].dropna().unique().tolist()])
        w_side = pn.widgets.Select(name='Side', options=sides, value='All', width=160, height=40, margin=(5, 10, 5, 10))
    else:
        w_side = None

    # Amount
    if col_amt and pd.api.types.is_numeric_dtype(decay_df[col_amt]):
        amin = float(pd.to_numeric(decay_df[col_amt], errors='coerce').min())
        amax = float(pd.to_numeric(decay_df[col_amt], errors='coerce').max())
        w_amt = pn.widgets.RangeSlider(name='Amount', start=amin, end=amax, value=(amin, amax), step=(amax-amin)/100 if amax>amin else 1.0, width=300, height=40, margin=(5, 10, 5, 10))
    else:
        w_amt = None

    # Price
    if col_px and pd.api.types.is_numeric_dtype(decay_df[col_px]):
        pmin = float(pd.to_numeric(decay_df[col_px], errors='coerce').min())
        pmax = float(pd.to_numeric(decay_df[col_px], errors='coerce').max())
        w_px = pn.widgets.RangeSlider(name='Price', start=pmin, end=pmax, value=(pmin, pmax), step=(pmax-pmin)/100 if pmax>pmin else 1.0, width=300, height=40, margin=(5, 10, 5, 10))
    else:
        w_px = None

    # Order Kind
    if col_order_kind:
        kinds = ['All'] + sorted([str(x) for x in decay_df[col_order_kind].dropna().unique().tolist()])
        w_order_kind = pn.widgets.Select(name='Order Kind', options=kinds, value='All', width=180, height=40, margin=(5, 10, 5, 10))
    else:
        w_order_kind = None

    # Order Type
    if col_order_type:
        types_ = ['All'] + sorted([str(x) for x in decay_df[col_order_type].dropna().unique().tolist()])
        w_order_type = pn.widgets.Select(name='Order Type', options=types_, value='All', width=180, height=40, margin=(5, 10, 5, 10))
    else:
        w_order_type = None

    # TIF
    if col_tif:
        tifs = ['All'] + sorted([str(x) for x in decay_df[col_tif].dropna().unique().tolist()])
        w_tif = pn.widgets.Select(name='TIF', options=tifs, value='All', width=140, height=40, margin=(5, 10, 5, 10))
    else:
        w_tif = None

    # Order Status
    if col_order_status:
        statuses = ['All'] + sorted([str(x) for x in decay_df[col_order_status].dropna().unique().tolist()])
        w_order_status = pn.widgets.Select(name='Status', options=statuses, value='All', width=160, height=40, margin=(5, 10, 5, 10))
    else:
        w_order_status = None

    # --- Filtering helper ---
    @pn.depends(
        w_instrument.param.value,
        *( [w_time.param.value] if w_time else [] ),
        *( [w_side.param.value] if w_side else [] ),
        *( [w_amt.param.value] if w_amt else [] ),
        *( [w_px.param.value] if w_px else [] ),
        *( [w_order_kind.param.value] if w_order_kind else [] ),
        *( [w_order_type.param.value] if w_order_type else [] ),
        *( [w_tif.param.value] if w_tif else [] ),
        *( [w_order_status.param.value] if w_order_status else [] ),
    )
    def filtered_deals():
        df = decay_df

        # Instrument
        if col_instrument and w_instrument.value and w_instrument.value != 'All':
            df = df[df[col_instrument] == w_instrument.value]

        # Time
        if col_time and w_time:
            t0, t1 = w_time.value
            if t0 and t1:
                dtcol = pd.to_datetime(df[col_time], utc=True, errors='coerce')
                def _to_utc(ts):
                    ts = pd.to_datetime(ts)
                    if getattr(ts, 'tzinfo', None) is None:
                        return ts.tz_localize('UTC')
                    return ts.tz_convert('UTC')
                t0_utc = _to_utc(t0)
                t1_utc = _to_utc(t1)
                df = df[(dtcol >= t0_utc) & (dtcol <= t1_utc)]

        # Side
        if col_side and w_side and w_side.value != 'All':
            df = df[df[col_side].astype(str) == str(w_side.value)]

        # Amount
        if col_amt and w_amt:
            a0, a1 = w_amt.value
            df = df[(pd.to_numeric(df[col_amt], errors='coerce') >= a0) & (pd.to_numeric(df[col_amt], errors='coerce') <= a1)]

        # Price
        if col_px and w_px:
            p0, p1 = w_px.value
            df = df[(pd.to_numeric(df[col_px], errors='coerce') >= p0) & (pd.to_numeric(df[col_px], errors='coerce') <= p1)]

        # Order Kind
        if col_order_kind and w_order_kind and w_order_kind.value != 'All':
            df = df[df[col_order_kind].astype(str) == str(w_order_kind.value)]

        # Order Type
        if col_order_type and w_order_type and w_order_type.value != 'All':
            df = df[df[col_order_type].astype(str) == str(w_order_type.value)]

        # TIF
        if col_tif and w_tif and w_tif.value != 'All':
            df = df[df[col_tif].astype(str) == str(w_tif.value)]

        # Order Status
        if col_order_status and w_order_status and w_order_status.value != 'All':
            df = df[df[col_order_status].astype(str) == str(w_order_status.value)]

        return df

    # --- Plot builder (aggregate all deals) ---
    @pn.depends(
        w_instrument.param.value,
        *( [w_time.param.value] if w_time else [] ),
        *( [w_side.param.value] if w_side else [] ),
        *( [w_amt.param.value] if w_amt else [] ),
        *( [w_px.param.value] if w_px else [] ),
        *( [w_order_kind.param.value] if w_order_kind else [] ),
        *( [w_order_type.param.value] if w_order_type else [] ),
        *( [w_tif.param.value] if w_tif else [] ),
        *( [w_order_status.param.value] if w_order_status else [] ),
    )
    def plot_filtered(*_):
        df = filtered_deals()
        if df.empty:
            return pn.pane.Markdown("_No deals for current filters._")

        instrument = w_instrument.value

        instrument_plots = []

        # Determine axis unit scaling from resample (e.g., '1s' -> seconds)
        try:
            unit_ms = pd.to_timedelta(resample).total_seconds() * 1000.0
        except Exception:
            unit_ms = 1000.0  # fallback to seconds

        for _, row in df.iterrows():
            df_before = row['before']
            df_after = row['after']
            df_before_usd = row.get('before_usd', pd.DataFrame())
            df_after_usd = row.get('after_usd', pd.DataFrame())

            if not isinstance(df_before, pd.DataFrame) or not isinstance(df_after, pd.DataFrame):
                continue
            if df_before.empty or df_after.empty:
                continue
            if 'ts_server' not in df_before.columns or 'ts_server' not in df_after.columns:
                continue

            deal_ts = row['time']
            deal_price = row['px']
            deal_side = str(row['side'])

            df_b = df_before.copy()
            df_a = df_after.copy()

            df_b['ms_from_deal'] = (pd.to_datetime(df_b['ts_server']) - deal_ts).dt.total_seconds() * 1000
            df_a['ms_from_deal'] = (pd.to_datetime(df_a['ts_server']) - deal_ts).dt.total_seconds() * 1000

            # convert to requested resample units for x-axis
            if unit_ms and unit_ms != 0:
                df_b['unit_from_deal'] = df_b['ms_from_deal'] / unit_ms
                df_a['unit_from_deal'] = df_a['ms_from_deal'] / unit_ms
            else:
                df_b['unit_from_deal'] = df_b['ms_from_deal'] / 1000.0
                df_a['unit_from_deal'] = df_a['ms_from_deal'] / 1000.0

            df_b['pnl_long'] = deal_price - df_b['bid_px_0']
            df_b['pnl_sell'] = df_b['ask_px_0'] - deal_price
            df_a['pnl_long'] = deal_price - df_a['bid_px_0']
            df_a['pnl_sell'] = df_a['ask_px_0'] - deal_price

            # Multiply PnL by USD conversion rates if available
            if isinstance(df_before_usd, pd.DataFrame) and not df_before_usd.empty and 'ts_server' in df_before_usd.columns:
                # Merge USD conversion data on timestamp
                df_before_usd_copy = df_before_usd.copy()
                df_b = df_b.merge(
                    df_before_usd_copy[['ts_server', 'bid_px_0', 'ask_px_0']].rename(
                        columns={'bid_px_0': 'usd_bid_px_0', 'ask_px_0': 'usd_ask_px_0'}
                    ),
                    on='ts_server',
                    how='left'
                )
                # Use mid price for USD conversion
                df_b['usd_mid_px'] = (df_b['usd_bid_px_0'] + df_b['usd_ask_px_0']) / 2
                # Multiply PnL by USD conversion rate (fill NaN with 1.0 to avoid nullifying PnL where USD data is missing)
                df_b['usd_mid_px'] = df_b['usd_mid_px'].fillna(1.0)
                df_b['pnl_long'] = df_b['pnl_long'] * df_b['usd_mid_px']
                df_b['pnl_sell'] = df_b['pnl_sell'] * df_b['usd_mid_px']

            if isinstance(df_after_usd, pd.DataFrame) and not df_after_usd.empty and 'ts_server' in df_after_usd.columns:
                # Merge USD conversion data on timestamp
                df_after_usd_copy = df_after_usd.copy()
                df_a = df_a.merge(
                    df_after_usd_copy[['ts_server', 'bid_px_0', 'ask_px_0']].rename(
                        columns={'bid_px_0': 'usd_bid_px_0', 'ask_px_0': 'usd_ask_px_0'}
                    ),
                    on='ts_server',
                    how='left'
                )
                # Use mid price for USD conversion
                df_a['usd_mid_px'] = (df_a['usd_bid_px_0'] + df_a['usd_ask_px_0']) / 2
                # Multiply PnL by USD conversion rate (fill NaN with 1.0 to avoid nullifying PnL where USD data is missing)
                df_a['usd_mid_px'] = df_a['usd_mid_px'].fillna(1.0)
                df_a['pnl_long'] = df_a['pnl_long'] * df_a['usd_mid_px']
                df_a['pnl_sell'] = df_a['pnl_sell'] * df_a['usd_mid_px']

            df_bi = df_b.set_index('unit_from_deal')
            df_ai = df_a.set_index('unit_from_deal')

            if deal_side.lower() in ['buy', 'long']:
                pnl_before = hv.Curve(df_bi, 'unit_from_deal', 'pnl_long', label='PnL (Before)').opts(color='green', alpha=0.3)
                pnl_after = hv.Curve(df_ai, 'unit_from_deal', 'pnl_long', label='PnL (After)').opts(color='green')
            else:
                pnl_before = hv.Curve(df_bi, 'unit_from_deal', 'pnl_sell', label='PnL (Before)').opts(color='red', alpha=0.3)
                pnl_after = hv.Curve(df_ai, 'unit_from_deal', 'pnl_sell', label='PnL (After)').opts(color='red')

            plot = hv.Overlay([pnl_before, pnl_after])
            instrument_plots.append(plot)

        if not instrument_plots:
            return pn.pane.Markdown("_Nothing to plot for current filters._")

        combined = hv.Overlay(instrument_plots).opts(
            width=1000,
            height=500,
            xlabel=f'Time from Deal ({resample})',
            ylabel='PnL, USD',
            title=f'[PnL Decay] {instrument}',
            legend_position='top_right'
        )

        return pn.pane.HoloViews(combined, sizing_mode='stretch_both')

    # --- Calculate final PnL helper ---
    def _calculate_final_pnl(row):
        """Calculate final PnL from usd_after[-1] or after[-1] if USD not available."""
        deal_price = row.get('px', 0)
        deal_side = str(row.get('side', '')).lower()
        
        df_after_usd = row.get('after_usd', pd.DataFrame())
        df_after = row.get('after', pd.DataFrame())
        
        # Need at least one of the DataFrames
        if not isinstance(df_after, pd.DataFrame) or df_after.empty:
            return None
        
        if 'ts_server' not in df_after.columns:
            return None
        
        # Get the last row from after (sorted by timestamp)
        df_after_sorted = df_after.sort_values('ts_server')
        last_after = df_after_sorted.iloc[-1]
        
        # Calculate PnL in base currency based on side
        if deal_side in ['buy', 'long']:
            bid_px = last_after.get('bid_px_0', 0)
            pnl = deal_price - bid_px
        else:
            ask_px = last_after.get('ask_px_0', 0)
            pnl = ask_px - deal_price
        
        # Convert to USD if USD data is available
        if isinstance(df_after_usd, pd.DataFrame) and not df_after_usd.empty and 'ts_server' in df_after_usd.columns:
            # Get the last row from USD data (match timestamp with after data)
            df_after_usd_sorted = df_after_usd.sort_values('ts_server')
            last_after_usd = df_after_usd_sorted.iloc[-1]
            
            # Get USD conversion rate (mid price)
            usd_bid_px = last_after_usd.get('bid_px_0', 0)
            usd_ask_px = last_after_usd.get('ask_px_0', 0)
            # Check if values are valid (not None, not NaN, not 0)
            if (usd_bid_px and usd_ask_px and 
                pd.notna(usd_bid_px) and pd.notna(usd_ask_px) and
                usd_bid_px != 0 and usd_ask_px != 0):
                usd_mid_px = (usd_bid_px + usd_ask_px) / 2.0
                pnl = pnl * usd_mid_px
        
        return pnl

    # --- Best/Worst deals by PnL (collapsible) ---
    @pn.depends(
        w_instrument,
        *( [w_time] if w_time else [] ),
        *( [w_side] if w_side else [] ),
        *( [w_amt] if w_amt else [] ),
        *( [w_px] if w_px else [] ),
        *( [w_order_kind] if w_order_kind else [] ),
        *( [w_order_type] if w_order_type else [] ),
        *( [w_tif] if w_tif else [] ),
        *( [w_order_status] if w_order_status else [] ),
    )
    def pnl_extremes_table(*_):
        df = filtered_deals().copy()
        if df.empty:
            return pn.pane.Markdown("_No deals for current filters._")
        
        # Calculate final PnL for each deal
        df['final_pnl'] = df.apply(_calculate_final_pnl, axis=1)
        
        # Filter out deals where PnL couldn't be calculated
        df_with_pnl = df[df['final_pnl'].notna()].copy()
        
        if df_with_pnl.empty:
            return pn.pane.Markdown("_No deals with PnL data available._")
        
        # Sort by PnL
        df_sorted = df_with_pnl.sort_values('final_pnl', ascending=False)
        
        # Calculate 10% indices
        total = len(df_sorted)
        top_10_percent = max(1, int(total * 0.1))
        bottom_10_percent = max(1, int(total * 0.1))
        
        # Get best and worst deals
        best_deals = df_sorted.head(top_10_percent)
        worst_deals = df_sorted.tail(bottom_10_percent)
        
        # Prepare columns for display (exclude DataFrame columns)
        display_cols = [c for c in df.columns if c not in ['before', 'after', 'before_usd', 'after_usd']]
        # Make sure final_pnl is included and at the end
        if 'final_pnl' not in display_cols:
            display_cols.append('final_pnl')
        else:
            display_cols.remove('final_pnl')
            display_cols.append('final_pnl')
        
        # Create tables
        best_table = pn.widgets.Tabulator(
            best_deals[display_cols],
            sizing_mode='stretch_width',
            pagination=None,
            selectable=False,
            disabled=True,
            show_index=False,
            height=500
        )
        
        worst_table = pn.widgets.Tabulator(
            worst_deals[display_cols],
            sizing_mode='stretch_width',
            pagination=None,
            selectable=False,
            disabled=True,
            show_index=False,
            height=500
        )
        
        return pn.Column(
            pn.pane.Markdown(f"**Best Deals (Top 10%, n={len(best_deals)})**"),
            best_table,
            pn.Spacer(height=10),
            pn.pane.Markdown(f"**Worst Deals (Bottom 10%, n={len(worst_deals)})**"),
            worst_table,
            sizing_mode='stretch_both',
            min_height=850
        )

    # --- Deals table (collapsible) ---
    @pn.depends(
        w_instrument,
        *( [w_time] if w_time else [] ),
        *( [w_side] if w_side else [] ),
        *( [w_amt] if w_amt else [] ),
        *( [w_px] if w_px else [] ),
        *( [w_order_kind] if w_order_kind else [] ),
        *( [w_order_type] if w_order_type else [] ),
        *( [w_tif] if w_tif else [] ),
        *( [w_order_status] if w_order_status else [] ),
    )
    def deals_table(*_):
        df = filtered_deals().copy()
        if df.empty:
            return pn.pane.Markdown("_No deals for current filters._")
        cols = [c for c in df.columns if c not in ['before', 'after', 'before_usd', 'after_usd']]
        return pn.widgets.Tabulator(
            df[cols],
            sizing_mode='stretch_width',
            pagination=None,
            selectable=False,
            disabled=True,
            show_index=False,
            max_height=300
        )

    # --- Layout ---
    # Build filter groups: categorical vs numeric/time
    categorical_controls = [
        w_instrument,
        *( [w_side] if w_side else [] ),
        *( [w_order_kind] if w_order_kind else [] ),
        *( [w_order_type] if w_order_type else [] ),
        *( [w_tif] if w_tif else [] ),
        *( [w_order_status] if w_order_status else [] ),
    ]
    numeric_controls = [
        *( [w_time] if w_time else [] ),
        *( [w_amt] if w_amt else [] ),
        *( [w_px] if w_px else [] ),
    ]

    cat_column = pn.Row(
        *[c for c in categorical_controls if c is not None],
        sizing_mode='stretch_width',
        margin=(6, 8, 6, 8),
        styles={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '8px'}
    )
    num_column = pn.Column(
        *[c for c in numeric_controls if c is not None],
        sizing_mode='stretch_width',
        margin=(6, 8, 6, 8)
    )

    deals_accordion = pn.Accordion(
        ('Deals', deals_table),
        active=[],
        sizing_mode='stretch_width'
    )

    pnl_extremes_accordion = pn.Accordion(
        ('Best/Worst Deals by PnL', pnl_extremes_table),
        active=[],
        sizing_mode='stretch_both'
    )

    plot_container = pn.Column(plot_filtered, sizing_mode='stretch_both', min_height=600)

    filters_accordion = pn.Accordion(
        ('Categorical Filters', cat_column),
        ('Numeric/Time Filters', num_column),
        active=[0, 1],
        sizing_mode='stretch_width'
    )

    layout = pn.Column(
        filters_accordion,
        pn.Spacer(height=5),
        plot_container,
        pn.Spacer(height=5),
        deals_accordion,
        pn.Spacer(height=5),
        pnl_extremes_accordion,
        sizing_mode='stretch_both'
    )

    # --- Responsive height setup ---
    def _set_initial_height():
        vh = getattr(pn.state, "viewport_height", 900) or 900
        plot_container.min_height = int(vh * 0.7)

    pn.state.onload(_set_initial_height)

    return layout


if __name__ == '__main__':
    app = build_dashboard(path='decay_df_20251029.parquet', resample='1s')
    # pn.serve(app, title='PnL Decay Dashboard', show=True)
    pn.serve(
        app,
        title='PnL Decay Dashboard',
        port=10000,
        address='0.0.0.0',
        allow_websocket_origin=['*'],
        show=True
    )
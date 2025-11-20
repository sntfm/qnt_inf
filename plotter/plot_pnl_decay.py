import pandas as pd
import numpy as np
from datetime import datetime
import os, glob, gc
import pickle
import sys
from pathlib import Path
from collections import defaultdict

import holoviews as hv
import hvplot.pandas  # registers the .hvplot accessor on DataFrames
import panel as pn
from bokeh.palettes import Magma256
from holoviews import opts
hv.extension('bokeh')


class PnlDecay(): 
    def build_plots(self):
        for instrument in self.decay_df['instrument'].unique():
            df_temp = self.decay_df[self.decay_df['instrument'] == instrument]
            
            pnls_before = pd.Series(dtype=float)
            pnls_after = pd.Series(dtype=float)
            sum_amt = 0.0
            zero_line = hv.HLine(0).opts(color='black', line_dash='dashed', line_width=1, alpha=0.5)

            for _, row in df_temp.iterrows():
                df_before = row['before']
                df_after = row['after']

                # Skip if DataFrames are empty or don't have required columns
                if df_before.empty or df_after.empty:
                    continue
                if 'ts_server' not in df_before.columns or 'ts_server' not in df_after.columns:
                    continue

                weight = row.get('amt', row.get('qty', 0))
                if pd.isna(weight) or weight == 0:
                    continue
                weight = abs(weight)

                # Get the deal timestamp as reference point
                deal_ts = row['time']
                deal_price = row['px']
                deal_side = row['side']

                # Convert timestamps to milliseconds relative to deal time
                df_before = df_before.copy()
                df_after = df_after.copy()
                df_before['ms_from_deal'] = (pd.to_datetime(df_before['ts_server']) - deal_ts).dt.total_seconds() * 1000
                df_after['ms_from_deal'] = (pd.to_datetime(df_after['ts_server']) - deal_ts).dt.total_seconds() * 1000

                # Calculate PnL
                # For long: price - bid
                # For sell: ask - price
                df_before['pnl_long'] = deal_price - df_before['bid_px_0']
                df_before['pnl_sell'] = df_before['ask_px_0'] - deal_price
                df_after['pnl_long'] = deal_price - df_after['bid_px_0']
                df_after['pnl_sell'] = df_after['ask_px_0'] - deal_price

                # If value_mode is return(s), normalize by the deal price
                if self.value_mode in ('return', 'returns') and deal_price != 0:
                    df_before['pnl_long'] = df_before['pnl_long'] / deal_price
                    df_before['pnl_sell'] = df_before['pnl_sell'] / deal_price
                    df_after['pnl_long'] = df_after['pnl_long'] / deal_price
                    df_after['pnl_sell'] = df_after['pnl_sell'] / deal_price

                # Set index to ms_from_deal before accumulating
                df_before_indexed = df_before.set_index('ms_from_deal')
                df_after_indexed = df_after.set_index('ms_from_deal')


                weighted_before = (df_before_indexed['pnl_long'] * weight).groupby(level=0).last()
                weighted_after = (df_after_indexed['pnl_sell'] * weight).groupby(level=0).last()

                if pnls_before.empty:
                    pnls_before = weighted_before.copy()
                else:
                    combined_before_index = pnls_before.index.union(weighted_before.index)
                    pnls_before = pnls_before.groupby(level=0).last()
                    pnls_before = pnls_before.reindex(combined_before_index).ffill()
                    weighted_before = weighted_before.reindex(combined_before_index)
                    weighted_before = weighted_before.ffill().fillna(0)
                    pnls_before = pnls_before + weighted_before

                if pnls_after.empty:
                    pnls_after = weighted_after.copy()
                else:
                    combined_after_index = pnls_after.index.union(weighted_after.index)
                    pnls_after = pnls_after.groupby(level=0).last()
                    pnls_after = pnls_after.reindex(combined_after_index).ffill()
                    weighted_after = weighted_after.reindex(combined_after_index)
                    weighted_after = weighted_after.ffill().fillna(0)
                    pnls_after = pnls_after + weighted_after
                sum_amt += weight

                # Create curves based on deal side
                if deal_side.lower() in ['buy', 'long']:
                    # For buy deals, show PnL using long formula
                    label_before = 'Return (Before)' if self.value_mode in ('return', 'returns') else 'PnL (Before)'
                    label_after = 'Return (After)' if self.value_mode in ('return', 'returns') else 'PnL (After)'
                    pnl_before = hv.Curve(df_before_indexed, 'ms_from_deal', 'pnl_long', label=label_before).opts(color='green', alpha=0.3)
                    pnl_after = hv.Curve(df_after_indexed, 'ms_from_deal', 'pnl_long', label=label_after).opts(color='green')
                else:
                    # For sell deals, show PnL using sell formula
                    label_before = 'Return (Before)' if self.value_mode in ('return', 'returns') else 'PnL (Before)'
                    label_after = 'Return (After)' if self.value_mode in ('return', 'returns') else 'PnL (After)'
                    pnl_before = hv.Curve(df_before_indexed, 'ms_from_deal', 'pnl_sell', label=label_before).opts(color='red', alpha=0.3)
                    pnl_after = hv.Curve(df_after_indexed, 'ms_from_deal', 'pnl_sell', label=label_after).opts(color='red')



                # Overlay all curves with zero line
                plot = hv.Overlay([pnl_before, pnl_after, zero_line]).opts(
                width=1000,
                height=500,
                xlabel='Time from Deal (ms)',
                ylabel=('Return' if self.value_mode in ('return', 'returns') else 'PnL'),
                    title=(f"[Return Decay] {instrument} - {deal_side} @ {deal_price}" if self.value_mode in ('return', 'returns') else f"[PnL Decay] {instrument} - {deal_side} @ {deal_price}"),
                legend_position='top_right'
            )

                self.plots[instrument].append(plot)

            if sum_amt == 0 or pnls_before.empty or pnls_after.empty:
                continue

            pnls_before = (pnls_before / sum_amt).sort_index()
            pnls_after = (pnls_after / sum_amt).sort_index()

            pnls_before_df = pnls_before.reset_index()
            pnls_before_df.columns = ['ms_from_deal', 'pnl_long']
            pnls_after_df = pnls_after.reset_index()
            pnls_after_df.columns = ['ms_from_deal', 'pnl_sell']

            pnl_before = hv.Curve(pnls_before_df, 'ms_from_deal', 'pnl_long', label='PnL (Before)').opts(color='black', alpha=0.3)
            pnl_after = hv.Curve(pnls_after_df, 'ms_from_deal', 'pnl_sell', label='PnL (After)').opts(color='black')

            plot = hv.Overlay([pnl_before, pnl_after, zero_line]).opts(
                width=1000,
                height=500,
                xlabel='Time from Deal (ms)',
                ylabel=('Return' if self.value_mode in ('return', 'returns') else 'PnL'),
                title=(f"[Return Decay] {instrument}" if self.value_mode in ('return', 'returns') else f"[PnL Decay] {instrument}"),
                legend_position='top_right'
            )

            self.plots[instrument].append(plot)

    def save_plots(self):
        self._preprocess()

        # Check if we have data to process
        if self.decay_df.empty:
            return

        # Calculate number of deals and volume in USD
        num_deals = len(self.decay_df)
        total_volume_usd = 0.0

        for _, row in self.decay_df.iterrows():
            instrument = row.get('instrument', '')
            # Deals table uses 'amt' as the filled quantity
            qty = row.get('amt', 0)
            px = row.get('px', 0)

            # Calculate volume in instrument currency
            volume_instrument = qty * px

            # Check if instrument needs USD conversion
            usd_instrument = self.usd_conversion_map.get(instrument)

            if usd_instrument:
                # Need to convert to USD using conversion rate at deal time
                # Try both before_usd and after_usd DataFrames
                df_before_usd = row.get('before_usd', pd.DataFrame())
                df_after_usd = row.get('after_usd', pd.DataFrame())

                conversion_rate = None

                # Try to get conversion rate from after_usd first (closest to deal time)
                if isinstance(df_after_usd, pd.DataFrame) and not df_after_usd.empty:
                    first_row = df_after_usd.dropna(subset=['ask_px_0', 'bid_px_0'], how='all').head(1)
                    if not first_row.empty:
                        conv_ask = first_row['ask_px_0'].iloc[0] if 'ask_px_0' in first_row.columns else np.nan
                        conv_bid = first_row['bid_px_0'].iloc[0] if 'bid_px_0' in first_row.columns else np.nan
                        if not pd.isna(conv_ask) and not pd.isna(conv_bid):
                            conversion_rate = (conv_ask + conv_bid) / 2

                # Fallback to before_usd if after_usd didn't work
                if conversion_rate is None and isinstance(df_before_usd, pd.DataFrame) and not df_before_usd.empty:
                    last_row = df_before_usd.dropna(subset=['ask_px_0', 'bid_px_0'], how='all').tail(1)
                    if not last_row.empty:
                        conv_ask = last_row['ask_px_0'].iloc[0] if 'ask_px_0' in last_row.columns else np.nan
                        conv_bid = last_row['bid_px_0'].iloc[0] if 'bid_px_0' in last_row.columns else np.nan
                        if not pd.isna(conv_ask) and not pd.isna(conv_bid):
                            conversion_rate = (conv_ask + conv_bid) / 2

                volume_usd = volume_instrument * conversion_rate if conversion_rate is not None else 0
            else:
                # Not in conversion map, use original volume (already in second currency which is likely USD)
                volume_usd = volume_instrument

            total_volume_usd += volume_usd

        # Create overview summary
        overview_panes = [
            pn.pane.Markdown(f"## Overview"),
            pn.pane.Markdown(f"**Number of Deals:** {num_deals:,}"),
            pn.pane.Markdown(f"**Total Volume (USD):** ${total_volume_usd:,.2f}"),
            pn.pane.Markdown("---"),
        ]

        # Build a 5% best/worst deals summary using final after-window profit-positive return
        # Convention: positive is better (profit), negative is worse (loss)
        df_metrics = []
        for _, row in self.decay_df.iterrows():
            df_after = row.get('after', pd.DataFrame())
            if not isinstance(df_after, pd.DataFrame) or df_after.empty:
                continue
            deal_price = row.get('px', np.nan)
            deal_side = str(row.get('side', '')).lower()
            if pd.isna(deal_price):
                continue

            final_metric = np.nan
            try:
                # Use the last non-null observation in the after window
                last_row = df_after.dropna(subset=['ask_px_0', 'bid_px_0'], how='all').tail(1)
                if not last_row.empty:
                    final_bid = last_row['bid_px_0'].iloc[0] if 'bid_px_0' in last_row.columns else np.nan
                    final_ask = last_row['ask_px_0'].iloc[0] if 'ask_px_0' in last_row.columns else np.nan
                    if deal_side in ['buy', 'long'] and not pd.isna(final_bid):
                        # profit-positive: buy closes at bid
                        final_metric = (final_bid - deal_price)
                    elif deal_side not in ['buy', 'long'] and not pd.isna(final_ask):
                        # profit-positive: sell closes at ask
                        final_metric = (deal_price - final_ask)
                    # Normalize if showing returns
                    if self.value_mode in ('return', 'returns') and deal_price not in (0, np.nan):
                        final_metric = final_metric / deal_price if not pd.isna(final_metric) else final_metric
            except Exception:
                pass

            df_metrics.append({
                'time': row.get('time'),
                'instrument': row.get('instrument'),
                'side': row.get('side'),
                'px': deal_price,
                'final_metric': final_metric,  # more positive is better
            })

        metrics_df = pd.DataFrame(df_metrics)
        summary_panes = []
        if not metrics_df.empty:
            # Calculate asset-level statistics
            asset_stats = []
            for instrument in metrics_df['instrument'].unique():
                inst_df = self.decay_df[self.decay_df['instrument'] == instrument]
                inst_metrics = metrics_df[metrics_df['instrument'] == instrument]

                # Calculate volume in USD for this instrument
                inst_volume_usd = 0.0
                for _, row in inst_df.iterrows():
                    qty = row.get('amt', 0)
                    px = row.get('px', 0)
                    volume_instrument = qty * px

                    usd_instrument = self.usd_conversion_map.get(instrument)
                    if usd_instrument:
                        df_before_usd = row.get('before_usd', pd.DataFrame())
                        df_after_usd = row.get('after_usd', pd.DataFrame())
                        conversion_rate = None

                        if isinstance(df_after_usd, pd.DataFrame) and not df_after_usd.empty:
                            first_row = df_after_usd.dropna(subset=['ask_px_0', 'bid_px_0'], how='all').head(1)
                            if not first_row.empty:
                                conv_ask = first_row['ask_px_0'].iloc[0] if 'ask_px_0' in first_row.columns else np.nan
                                conv_bid = first_row['bid_px_0'].iloc[0] if 'bid_px_0' in first_row.columns else np.nan
                                if not pd.isna(conv_ask) and not pd.isna(conv_bid):
                                    conversion_rate = (conv_ask + conv_bid) / 2

                        if conversion_rate is None and isinstance(df_before_usd, pd.DataFrame) and not df_before_usd.empty:
                            last_row = df_before_usd.dropna(subset=['ask_px_0', 'bid_px_0'], how='all').tail(1)
                            if not last_row.empty:
                                conv_ask = last_row['ask_px_0'].iloc[0] if 'ask_px_0' in last_row.columns else np.nan
                                conv_bid = last_row['bid_px_0'].iloc[0] if 'bid_px_0' in last_row.columns else np.nan
                                if not pd.isna(conv_ask) and not pd.isna(conv_bid):
                                    conversion_rate = (conv_ask + conv_bid) / 2

                        volume_usd = volume_instrument * conversion_rate if conversion_rate is not None else 0
                    else:
                        volume_usd = volume_instrument

                    inst_volume_usd += volume_usd

                # Calculate weighted average return
                valid_metrics = inst_metrics.dropna(subset=['final_metric'])
                if len(valid_metrics) > 0:
                    weighted_avg_return = valid_metrics['final_metric'].mean()
                else:
                    weighted_avg_return = np.nan

                asset_stats.append({
                    'instrument': instrument,
                    'volume_usd': inst_volume_usd,
                    'num_deals': len(inst_df),
                    'weighted_avg_return': weighted_avg_return
                })

            asset_stats_df = pd.DataFrame(asset_stats)

            # Top 10% by volume
            k_assets = max(1, int(np.ceil(0.10 * len(asset_stats_df))))
            top_volume_df = asset_stats_df.nlargest(k_assets, 'volume_usd')[['instrument', 'volume_usd', 'num_deals', 'weighted_avg_return']]
            top_volume_df['volume_usd'] = top_volume_df['volume_usd'].apply(lambda x: f"${x:,.2f}")
            top_volume_df['weighted_avg_return'] = top_volume_df['weighted_avg_return'].apply(lambda x: f"{x:.6f}" if not pd.isna(x) else "N/A")

            # Top 10% best performing
            valid_perf_df = asset_stats_df.dropna(subset=['weighted_avg_return'])
            k_perf = max(1, int(np.ceil(0.10 * len(valid_perf_df))))
            best_performers_df = valid_perf_df.nlargest(k_perf, 'weighted_avg_return')[['instrument', 'weighted_avg_return', 'num_deals', 'volume_usd']]
            best_performers_df['volume_usd'] = best_performers_df['volume_usd'].apply(lambda x: f"${x:,.2f}")
            best_performers_df['weighted_avg_return'] = best_performers_df['weighted_avg_return'].apply(lambda x: f"{x:.6f}")

            # Top 10% worst performing
            worst_performers_df = valid_perf_df.nsmallest(k_perf, 'weighted_avg_return')[['instrument', 'weighted_avg_return', 'num_deals', 'volume_usd']]
            worst_performers_df['volume_usd'] = worst_performers_df['volume_usd'].apply(lambda x: f"${x:,.2f}")
            worst_performers_df['weighted_avg_return'] = worst_performers_df['weighted_avg_return'].apply(lambda x: f"{x:.6f}")

            # Add asset-level summaries
            summary_panes.append(pn.pane.Markdown("## Asset Performance Summary"))
            summary_panes.append(pn.pane.Markdown("### Top 10% Assets by Volume (USD)"))
            summary_panes.append(pn.pane.DataFrame(top_volume_df, index=False))
            summary_panes.append(
                pn.Row(
                    pn.Column(
                        pn.pane.Markdown("### Top 10% Best Performing Assets (Weighted Avg Return)"),
                        pn.pane.DataFrame(best_performers_df, index=False)
                    ),
                    pn.Column(
                        pn.pane.Markdown("### Top 10% Worst Performing Assets (Weighted Avg Return)"),
                        pn.pane.DataFrame(worst_performers_df, index=False)
                    ),
                )
            )
            summary_panes.append(pn.pane.Markdown("---"))

            # Best/Worst individual deals
            k = max(1, int(np.ceil(0.05 * len(metrics_df))))
            # Best deals: largest positive final return
            best_df = metrics_df.nlargest(k, 'final_metric', keep='all')[['time', 'instrument', 'side', 'px', 'final_metric']]
            # Worst deals: most negative final return
            worst_df = metrics_df.nsmallest(k, 'final_metric', keep='all')[['time', 'instrument', 'side', 'px', 'final_metric']]

            summary_panes.append(pn.pane.Markdown("## 5% Best / Worst Deals (Final Return, profit-positive)"))
            summary_panes.append(
                pn.Row(
                    pn.Column(pn.pane.Markdown("### Best (most favorable)"), pn.pane.DataFrame(best_df)),
                    pn.Column(pn.pane.Markdown("### Worst (most unfavorable)"), pn.pane.DataFrame(worst_df)),
                )
            )

        self.build_plots()

        items = []
        for instrument, instrument_plots in self.plots.items():
            combined = hv.Overlay(instrument_plots).opts(
                width=1000,
                height=500,
                xlabel='Time',
                ylabel=('Return' if self.value_mode in ('return', 'returns') else 'PnL'),
                title=(f"[Return Decay] {instrument}" if self.value_mode in ('return', 'returns') else f"[PnL Decay] {instrument}"),
                show_legend=True,
                legend_position='top_right'
            )
            items.append(pn.pane.HoloViews(combined))
            suffix = 'return' if self.value_mode in ('return', 'returns') else 'pnl'

        dashboard = pn.Column(
            f"## Kraken TOB Plots ({suffix}) {self.start_ts.split(' ')[0]}",
            *overview_panes,
            *summary_panes,
            *items
        )
        # Save a single HTML that contains all instruments' plots
        html_path = f"reports/PnlDecay-{self.start_ts.split(' ')[0]}-{suffix}.html"
        dashboard.save(html_path)
        
if __name__ == '__main__':

    # for date in ['2025-10-27', '2025-10-28', '2025-10-29', '2025-10-30',]:
    for date in ['2025-10-27']:
        start_ts = f'{date} 00:00:00.000000'
        end_ts   = f'{date} 23:59:59.999999'

        pnl_decay = PnlDecay(feed='feed_kraken_tob_5', start_ts=start_ts, end_ts=end_ts, lookback_minutes=15, value_mode='return')
        # pnl_decay._preprocess()
        # print(pnl_decay.decay_df.head())
        pnl_decay.save_plots()
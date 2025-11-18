import pandas as pd
import numpy as np
import gc
import sys
from pathlib import Path

from plotter.base_plotter import Plotter

import holoviews as hv
import hvplot.pandas  # registers the .hvplot accessor on DataFrames
from bokeh.palettes import Magma256
from holoviews import opts
hv.extension('bokeh')

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Auto-install chromedriver for Bokeh backend
try:
    import chromedriver_autoinstaller
    chromedriver_autoinstaller.install()
except ImportError:
    pass

class LatencyHist(Plotter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.md_paths = None
        self.bin_edges = None

    def preprocess(self, venue, date, bin_size=1.0):
        md_paths = self.fetcher.find_md_paths(venue, f'{date}-0000', f'{date}-2359')

        all_data = []

        for i, path in enumerate(md_paths):
            df_temp = pd.read_parquet(path)
            df_temp['lat'] = df_temp.ts_server - df_temp.ts_exch
            df_temp['ts'] = pd.to_datetime(df_temp['ts_server'], unit='ms')
            df_temp = df_temp[['ts', 'lat', 'name']]
            df_temp = df_temp[df_temp['lat'] < np.percentile(df_temp['lat'], 99)]
            source = path.split('/')[-1].split('-')[2]  # Add source identifier
            df_temp['source'] = source
            all_data.append(df_temp)

        combined_df = pd.concat(all_data, ignore_index=True)

        min_lat = combined_df['lat'].min()
        max_lat = combined_df['lat'].max()
        bin_edges = np.arange(min_lat, max_lat + bin_size, bin_size)
        self.md_storage = combined_df
        self.bin_edges = bin_edges
        del combined_df; gc.collect()

    def create_latency_hist(self, venue, date, data_df, add_title=''):
        data_df['source_code'] = data_df['source'].astype('category').cat.codes

        unique_sources = data_df['source'].unique()
        n_sources = len(unique_sources)

        color_indices = np.linspace(40, 230, n_sources, dtype=int)
        magma_dark = [Magma256[int(i)] for i in color_indices]

        combined_plot = data_df.hvplot.hist(
            y='lat',
            by='source',
            bins=self.bin_edges,
            width=800, height=650,
            alpha=0.3,
            title=f'{venue.upper()} {date}: Latency Dist (Hours)' + add_title,
            xlabel='Latency (ms)',
            ylabel='Count'
        ).opts(
            opts.Histogram(color=hv.Cycle(magma_dark)),
            opts.NdOverlay(legend_position='right')
        )

        # Calculate combined statistics
        combined_mean = data_df['lat'].mean()
        combined_median = data_df['lat'].median()

        # Add vertical lines for mean and median
        mean_line = hv.VLine(combined_mean).opts(
            color='red', 
            line_dash='dashed', 
            line_width=1
        )

        median_line = hv.VLine(combined_median).opts(
            color='green', 
            line_dash='dashed', 
            line_width=1
        )

        # Combine histogram with vertical lines and text
        final_plot = combined_plot * mean_line * median_line

        print(f"Combined Statistics:")
        print(f"Mean (red): {combined_mean:.2f}ms")
        print(f"Media (green): {combined_median:.2f}ms")
        print(f"Std: {data_df['lat'].std():.2f}ms")
        return final_plot

    def create_plots(self, venue, date, bin_size=1.0):
        self.preprocess(venue, date, bin_size)

        for i, name in enumerate(self.md_storage['name'].unique()):
            print(f"Creating plot {name}")
            df_temp = self.md_storage[self.md_storage['name'] == name].copy()
            self.plots[name] = self.create_latency_hist(venue, date, df_temp, add_title=f' {name}')
            del df_temp; gc.collect()
        return self.plots


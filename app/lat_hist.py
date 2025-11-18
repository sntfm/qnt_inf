import pickle
import sys
import os
import holoviews as hv

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from research.latency_plotter import LatencyHist

plotter = LatencyHist()

# Ensure pics directory exists
pics_dir = os.path.join(project_root, 'dev', 'pics')
os.makedirs(pics_dir, exist_ok=True)

for date in ['251014', '251015', '251016', '251017' ,'251018', '251019', '251020', '251021']:
    print(f'Processing {date}')
    lat_plots = plotter.create_plots('kraken', date, bin_size=1.0)

    for name, plot in lat_plots.items():
        safe_name = name.replace('/', '_')
        output_path = os.path.join(pics_dir, f'kraken_{date}_{safe_name}.png')
        hv.save(plot.opts(xlim=(-1, 60)), output_path, fmt='png', backend='bokeh') 
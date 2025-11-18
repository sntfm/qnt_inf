import pandas as pd
import numpy as np
import time, glob, os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.fetcher import Fetcher

if __name__ == "__main__":
    fetcher = Fetcher('34.250.225.205', dir_raw_cache='data/_tmp')

    fetcher.update_md_batch(fetch_new=True, parse_new=True, max_batch_size_gb=0.75, num_processes=8, book_type='tob', depth=5)
    fetcher.update_deals()

    # df_deals = fetcher.fetch_qdb('feed_kraken_tob_5', '2025-10-26 00:00', '2025-10-27 00:00')
    # print(df_deals.head())

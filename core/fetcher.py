import pandas as pd
import subprocess
import glob, time
import gc, os, sys
from datetime import datetime, timedelta
from multiprocessing import Pool
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.parsers import MdParser, DealsParser
from core.adapter_qdb import AdapterQdb

class DataLoader:
    def __init__(self, host): 
        self.host = host

    def _run_wget(self, base_url, download_dir):
        cmd = [
            "wget",
            "-r", "-np", "-nH",
            "--cut-dirs=1",
            "-A", "csv.xz,sqlite",
            "-N",                    # only new or updated files
            "-P", str(download_dir), # save directory
            base_url
        ]
        subprocess.run(cmd, check=True)

    def load_md(self, download_dir):
        base_url = f"http://{self.host}/History/"
        self._run_wget(base_url, download_dir)
    
    def load_deals(self, download_dir):
        base_url = f"http://{self.host}/database/"
        self._run_wget(base_url, download_dir)

    def load_all(self, download_dir):
        self.load_md(download_dir)
        self.load_deals(download_dir)


class Fetcher:
    def __init__(self, host, 
                dir_raw_cache='data/_tmp',
                host_db='localhost',
                ):

        self.data_loader = DataLoader(host)
        self.adapter_qdb = AdapterQdb(host_db)

        self.dir_raw_cache = dir_raw_cache
        self.paths_md_raw = glob.glob(f'{self.dir_raw_cache}/*.csv.xz')
        self.paths_deals_db = glob.glob(f'{self.dir_raw_cache}/*.sqlite')
        
        # Get project root directory
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def get_md_paths(self, fetch_new=True):
        if fetch_new:
            self.data_loader.load_all(self.dir_raw_cache)
            time.sleep(0.5)
            # Refresh file paths after download
            self.paths_md_raw = glob.glob(f'{self.dir_raw_cache}/*.csv.xz')
            self.paths_deals_db = glob.glob(f'{self.dir_raw_cache}/*.sqlite')
        return self.paths_md_raw

    def update_deals(self):
        parser = DealsParser()
        success = parser.build_deals()
        if success and parser.deals is not None:
            self.adapter_qdb.write_deals_to_qdb(parser.deals)
        else:
            print("Skipping deals update: no deals data available")
        del parser;gc.collect()
        return True

    @staticmethod
    def update_md(adapter_qdb, file_name, book_type='tob', depth=0, id=0):
        # Extract base filename for ingress table check (without path)
        base_file_name = file_name.split('/')[-1]
        
        if not adapter_qdb.check_ingress_table(base_file_name, book_type, depth):
            parser = MdParser(file_name)
            df_md = parser.build_book(book_type, depth, id=id)
            del parser;gc.collect()

            if df_md is None:
                print(f"Failed to build book for {file_name}")
                return False

            # Extract venue from base filename
            venue = base_file_name.split('-')[0]
            adapter_qdb.write_md_to_qdb(df_md, venue, book_type, depth, id=id)
            adapter_qdb.update_ingress_table(base_file_name, f'{book_type}_{depth}', df_md.shape[0])
            del df_md;gc.collect()
            time.sleep(0.3)
            return True
        return False

    def update_md_batch(self, fetch_new=True, parse_new=True, max_batch_size_gb=0.25, num_processes=8, book_type='tob', depth=0):
        t0 = time.time()

        processed_file_names = self.adapter_qdb.fetch_ingress_table(f'{book_type}_{depth}')
        md_paths = self.get_md_paths(fetch_new=fetch_new)

        new_md_paths = [path for path in md_paths if path.split("/")[-1] not in processed_file_names]

        if not parse_new: return new_md_paths
        if not new_md_paths: print("No new files to process"); return 0

        print(f"{len(new_md_paths)} new MD files to process")
        MAX_BATCH_SIZE_BYTES = max_batch_size_gb * 1024 * 1024 * 1024

        md_paths_with_size = [(path, os.path.getsize(path)) for path in new_md_paths]
        md_paths_with_size.sort(key=lambda x: x[1], reverse=False)

        batches = []
        current_batch = []
        current_batch_size = 0
        
        for path, size in md_paths_with_size:
            # Check if adding this file would exceed size limit OR process count limit
            if ((current_batch_size + size > MAX_BATCH_SIZE_BYTES or len(current_batch) >= num_processes) 
                and current_batch):
                # Start new batch
                batches.append(current_batch)
                current_batch = [(path, size)]
                current_batch_size = size
            else:
                # Add to current batch
                current_batch.append((path, size))
                current_batch_size += size
        
        # Add the last batch
        if current_batch: batches.append(current_batch)
        print(f"Created {len(batches)} batches (max {max_batch_size_gb}GB and {num_processes} files per batch)")
        
        # Assign files to workers based on load balancing by size
        worker_load = [0] * num_processes  # Track cumulative size per worker
        worker_assignments = [0] * len(md_paths_with_size)
        
        # Assign each file to the worker with the smallest current load
        for idx, (path, size) in enumerate(md_paths_with_size):
            # Find worker with smallest load
            worker_id = min(range(num_processes), key=lambda i: worker_load[i])
            worker_load[worker_id] += size
            worker_assignments[idx] = worker_id
        
        all_args = [(self.adapter_qdb, path, book_type, depth, worker_assignments[idx]) 
                    for idx, (path, size) in enumerate(md_paths_with_size)]
        
        print(f"Processing {len(all_args)} files with {num_processes} workers")
        print("Workers will pick up files as they become available (no strict batching)")
        
        # Process all files with multiprocessing - workers will grab new files as they finish
        with Pool(processes=num_processes, maxtasksperchild=1) as pool:
            all_results = pool.starmap(Fetcher.update_md, all_args, chunksize=1)
        
        t1 = time.time()
        successful = sum(1 for r in all_results if r)
        failed = len(all_results) - successful
        print(f"All processing completed in: {(t1 - t0):.2f} sec")
        print(f"Successfully processed: {successful}/{len(all_results)} files")
        if failed > 0:
            print(f"Failed/Skipped: {failed} files")
        
        return successful

    def fetch_qdb(self, table_name, start_date=None, end_date=None, instrument=None, columns='*', timestamp_col=None):
        return self.adapter_qdb.fetch(table_name, start_date, end_date, instrument, columns, timestamp_col)
        

if __name__ == "__main__":
    fetcher = Fetcher('34.250.225.205', dir_raw_cache='data/test_tmp')
    # fetcher.update_deals()
    # fetcher.update_md_batch(fetch_new=False, parse_new=True, max_batch_size_gb=0.75, num_processes=8, book_type='tob', depth=20)

    df_deals = fetcher.fetch_qdb('feed_binance_tob_5', '2025-10-26T00:00:00.000000Z', '2025-10-26T00:00:14.999999Z')
    print(df_deals.head())
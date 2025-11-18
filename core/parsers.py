import polars as pl
import pyarrow as pa
import pandas as pd

import json
from datetime import datetime
import time, os, sys, io
import sqlite3
import gc
import ctypes
import lzma


def aggressive_gc():
    for _ in range(3): gc.collect()
    gc.collect(generation=2)
    
    try:
        pool = pa.default_memory_pool()
        pool.release_unused()
    except: pass
    
    # Force Python to release memory back to OS (macOS)
    try: ctypes.CDLL("libc.dylib").malloc_zone_pressure_relief(0, 0)
    except: pass
    
    # Force Python to release memory back to OS (Linux)
    try: ctypes.CDLL("libc.so.6").malloc_trim(0)
    except: pass 

class MdParser:
    def __init__(self, path_md_cache):
        self.path_md_cache = path_md_cache
        self.md_raw = None
        self.orderbook = None
        self.is_valid = False
        
        # Validate and read the LZMA file with error handling
        try:
            # First, validate the LZMA file by fully decompressing it
            # This ensures we catch corruption before Polars tries to read it
            decompressed_data = None
            with lzma.open(self.path_md_cache, mode='rb') as f:
                # Read entire file to validate it's not corrupted
                decompressed_data = f.read()
            
            # If validation passed, read with Polars from the decompressed bytes
            self.md_raw = pl.read_csv(
                io.BytesIO(decompressed_data),
                has_header=False,
                new_columns=['raw_line'],
                separator='\n',
                quote_char=None
            )
            self.is_valid = True
        except EOFError as e:
            print(f"ERROR: Corrupted LZMA file (incomplete): {self.path_md_cache}")
            print(f"       {str(e)}")
            self.is_valid = False
        except Exception as e:
            print(f"ERROR: Failed to read file: {self.path_md_cache}")
            print(f"       {type(e).__name__}: {str(e)}")
            self.is_valid = False

    def build_book_mbo(self):
        if not self.is_valid or self.md_raw is None:
            print(f"Skipping build_book_mbo for invalid file: {self.path_md_cache}")
            return False
            
        # Step 1: Split raw lines by semicolon and extract all fields
        md_tmp = self.md_raw.with_columns([
            pl.col('raw_line').str.split(';').alias('parts')
        ]).with_columns([
            pl.col('parts').list.get(0).cast(pl.Int64).alias('ts_server'),
            pl.col('parts').list.get(1).cast(pl.Int64).alias('seq_server'),
            pl.col('parts').list.get(2).cast(pl.Int64).alias('ts_exch'),
            pl.col('parts').list.get(3).alias('instrument'),
            pl.col('parts').list.get(4).cast(pl.Int32).alias('asks_cnt'),
        ]).with_columns([
            # Extract asks data: starts at index 5, length = asks_cnt * 2
            pl.col('parts').list.slice(5, pl.col('asks_cnt') * 2).alias('asks_parts'),
            # Extract bids_cnt: at index 5 + asks_cnt * 2
            pl.col('parts').list.get(5 + pl.col('asks_cnt') * 2).cast(pl.Int32).alias('bids_cnt'),
        ]).with_columns([
            # Extract bids data: starts at index 5 + asks_cnt * 2 + 1, length = bids_cnt * 2
            pl.col('parts').list.slice(5 + pl.col('asks_cnt') * 2 + 1, pl.col('bids_cnt') * 2).alias('bids_parts'),
        ]).with_columns([
            # Join the parts back into semicolon-separated strings for compatibility
            pl.col('asks_parts').list.join(';').alias('asks'),
            pl.col('bids_parts').list.join(';').alias('bids'),
        ]).drop(['raw_line', 'parts', 'asks_parts', 'bids_parts'])
        
        # Release md_raw immediately after extraction
        del self.md_raw
        self.md_raw = None
        aggressive_gc()
        
        # Step 2: Split and convert to float lists
        # Filter out empty strings before casting to handle trailing semicolons
        md_tmp = md_tmp.with_columns([
            pl.col("asks").str.split(";").list.eval(
                pl.element().filter(pl.element() != "").cast(pl.Float64)
            ).alias("asks_f"),
            pl.col("bids").str.split(";").list.eval(
                pl.element().filter(pl.element() != "").cast(pl.Float64)
            ).alias("bids_f"),
        ]).drop(["asks", "bids"])
        
        aggressive_gc()
        
        # Step 3: Extract amounts and prices using slice operations on ragged arrays
        # Use list.eval with gather to extract even/odd indices per row
        self.orderbook = md_tmp.with_columns([
            # Extract amounts (even indices: 0, 2, 4, ...) 
            pl.col("asks_f").list.eval(
                pl.element().gather(pl.int_range(0, pl.element().len(), step=2))
            ).alias("asks_amt"),
            # Extract prices (odd indices: 1, 3, 5, ...)
            pl.col("asks_f").list.eval(
                pl.element().gather(pl.int_range(1, pl.element().len(), step=2))
            ).alias("asks_px"),
            # Same for bids
            pl.col("bids_f").list.eval(
                pl.element().gather(pl.int_range(0, pl.element().len(), step=2))
            ).alias("bids_amt"),
            pl.col("bids_f").list.eval(
                pl.element().gather(pl.int_range(1, pl.element().len(), step=2))
            ).alias("bids_px"),
        ]).drop(["asks_f", "bids_f"])
        
        # Release intermediate df
        del md_tmp
        aggressive_gc()
        
        return True

    def _aggregate_orderbook(self):
        def per_side(df, px_col_name, amt_col_name, ascending):
            # Memory-optimized version: chain operations and delete intermediates
            # Add row index and explode in one pass
            exploded = df.with_row_index("row_idx").explode([px_col_name, amt_col_name])
            
            # Single sort + group operation (combine both sorts)
            sort_cols = ["row_idx", px_col_name]
            result = (
                exploded
                .sort(sort_cols, descending=[False, not ascending])
                .group_by(["row_idx", px_col_name], maintain_order=True)  # maintain_order avoids re-sort
                .agg([pl.col(amt_col_name).sum().alias("agg_amt")])
                .group_by("row_idx", maintain_order=True)
                .agg([
                    pl.col(px_col_name).alias("agg_px"),
                    pl.col("agg_amt").alias("agg_amt")
                ])
                .sort("row_idx")
            )
            
            # Explicitly delete exploded DataFrame
            del exploded
            
            agg_px = result["agg_px"]
            agg_amt = result["agg_amt"]
            
            # Delete result before returning
            del result
            
            return agg_px, agg_amt

        # Process only the required columns to reduce memory footprint
        asks_df = self.orderbook.select(["asks_px", "asks_amt"])
        asks_px, asks_amt = per_side(asks_df, "asks_px", "asks_amt", True)
        del asks_df
        aggressive_gc()
        
        bids_df = self.orderbook.select(["bids_px", "bids_amt"])
        bids_px, bids_amt = per_side(bids_df, "bids_px", "bids_amt", False)
        del bids_df
        aggressive_gc() 
        
        return {
            "asks_agg_px": asks_px,
            "asks_agg_amt": asks_amt,
            "bids_agg_px": bids_px,
            "bids_agg_amt": bids_amt,
        }

    def build_book_mbp(self):
        if self.orderbook is None:
            if not self.build_book_mbo():
                return False
        agg = self._aggregate_orderbook()
        
        # First, add aggregated data
        self.orderbook = self.orderbook.with_columns([
            pl.Series('asks_agg_px', agg["asks_agg_px"]),
            pl.Series('asks_agg_amt', agg["asks_agg_amt"]),
            pl.Series('bids_agg_px', agg["bids_agg_px"]),
            pl.Series('bids_agg_amt', agg["bids_agg_amt"]),
        ])
        
        # Then, add level count columns that depend on the aggregated columns
        self.orderbook = self.orderbook.with_columns([
            pl.col('asks_agg_px').list.len().alias('asks_lvl_cnt'),
            pl.col('bids_agg_px').list.len().alias('bids_lvl_cnt'),
        ])
        
        # Drop MBO columns (original order book data)
        self.orderbook = self.orderbook.drop(['asks_px', 'asks_amt', 'bids_px', 'bids_amt'])
        
        # Clean up aggregation data immediately after adding columns
        del agg
        aggressive_gc()
                
        return True

    def build_book_tob(self, depth=10):
        if self.orderbook is None: 
            if not self.build_book_mbp():
                return False
        
        if 'asks_agg_px' not in self.orderbook.columns:
            if not self.build_book_mbp():
                return False
        
        # Filter out rows with null or empty lists before processing
        self.orderbook = self.orderbook.filter(
            (pl.col('asks_agg_px').is_not_null()) & 
            (pl.col('asks_agg_px').list.len() > 0) &
            (pl.col('bids_agg_px').is_not_null()) & 
            (pl.col('bids_agg_px').list.len() > 0)
        )
        
        # If all rows were filtered out, return empty result
        if len(self.orderbook) == 0:
            print("Warning: All rows filtered out (null or empty order books)")
            return False
        
        # Pad lists to exactly 'depth' length and convert to struct
        # Generate field names for the struct
        ask_px_fields = [f'ask_px_{i}' for i in range(depth)]
        ask_amt_fields = [f'ask_amt_{i}' for i in range(depth)]
        bid_px_fields = [f'bid_px_{i}' for i in range(depth)]
        bid_amt_fields = [f'bid_amt_{i}' for i in range(depth)]
        
        # Pad each list to depth length using a simple concatenation approach
        # This avoids the complexity of calculating padding size inside list.eval
        def pad_list(lst):
            # Convert to Python list if it's a Series or other type
            if hasattr(lst, 'to_list'): lst = lst.to_list()
            elif not isinstance(lst, list): lst = list(lst) if lst is not None else []
            
            if lst is None or len(lst) == 0: return [0.0] * depth
            if len(lst) >= depth: return lst[:depth]
            return lst + [0.0] * (depth - len(lst))
        
        self.orderbook = self.orderbook.with_columns([
            pl.col('asks_agg_px').map_elements(
                pad_list,
                return_dtype=pl.List(pl.Float64)
            ).list.to_struct(fields=ask_px_fields),
            
            pl.col('asks_agg_amt').map_elements(
                pad_list,
                return_dtype=pl.List(pl.Float64)
            ).list.to_struct(fields=ask_amt_fields),
            
            pl.col('bids_agg_px').map_elements(
                pad_list,
                return_dtype=pl.List(pl.Float64)
            ).list.to_struct(fields=bid_px_fields),
            
            pl.col('bids_agg_amt').map_elements(
                pad_list,
                return_dtype=pl.List(pl.Float64)
            ).list.to_struct(fields=bid_amt_fields),
        ])
        
        # Unnest the struct columns to create individual columns
        self.orderbook = self.orderbook.unnest(['asks_agg_px', 'asks_agg_amt', 'bids_agg_px', 'bids_agg_amt'])
        
        aggressive_gc() 
        
        return True

    def build_book(self, book_type, depth=20, id=0):
        if book_type == 'mbo':
            t0 = time.time(); print(f"Worker {id} READ: {self.path_md_cache}")
            print(f"Worker {id} [build_book_mbo]: building...")
            if not self.build_book_mbo():
                print(f"Worker {id} [build_book_mbo]: FAILED")
                return None
            aggressive_gc()  # Immediately free memory after MBO build
            t1 = time.time()
            print(f"Worker {id} [build_book_mbo] {self.path_md_cache.split('/')[-1]}: {(t1 - t0):.2f} sec")

        elif book_type == 'mbp':
            t0 = time.time()
            print(f"Worker {id} [build_book_mbp]: building...")
            if not self.build_book_mbp():
                print(f"Worker {id} [build_book_mbp]: FAILED")
                return None
            aggressive_gc()  # Immediately free memory after MBP build
            t1 = time.time()
            print(f"Worker {id} [build_book_mbp] {self.path_md_cache.split('/')[-1]}: {(t1 - t0):.2f} sec")

        elif book_type == 'tob':
            t0 = time.time(); 
            print(f"Worker {id} [build_book_tob]: depth={depth}, building...")
            if not self.build_book_tob(depth):
                print(f"Worker {id} [build_book_tob]: FAILED")
                return None
            aggressive_gc()  # Immediately free memory after TOB build
            t1 = time.time()
            print(f"Worker {id} [build_book_tob] {self.path_md_cache.split('/')[-1]}: {(t1 - t0):.2f} sec")
            # return self.orderbook is not None and not self.orderbook.is_empty()

        return self.orderbook

class DealsParser:
    def __init__(self, 
                db_file='CryptoTrader.sqlite', 
                dir_raw_cache='data/_tmp',
                cfg_file='core/cfg/md_raw_mappings.json'):

        with open(cfg_file, 'r') as file:
            data = json.load(file)
            self.mappings = {}
            self.mappings['fields'] = {key: {int(sub_key): value for sub_key, value in sub_dict.items()}
                                       for key, sub_dict in data["fields"].items()}
            self.mappings['instruments_short_names'] = {int(k): v.replace('/', '_') for k, v in data['instruments_short_names'].items()}
            self.mappings['instruments_long_names'] = {int(k): v for k, v in data['instruments_long_names'].items()}
        
        self.deals = None
        self.dir_raw_cache = dir_raw_cache
        self.sqlite_file_path = f'{dir_raw_cache}/{db_file}'

    def _load_deals(self, ):
        connection = sqlite3.connect(self.sqlite_file_path)
        
        # Check if Deal table exists
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Deal'")
        if not cursor.fetchone():
            print("Warning: Deal table not found in database. Skipping deals processing.")
            cursor.close()
            connection.close()
            del cursor, connection
            gc.collect()
            return False
        
        # Read from SQLite using Polars with lazy evaluation
        query = f"SELECT * FROM Deal"
        self.deals = pl.read_database(query, connection)
        cursor.close()
        connection.close()
        
        # Explicitly delete cursor and connection references
        del cursor, connection
        gc.collect()
        return True

    def build_deals(self, map_enums=True):
        if self.deals is None:
            if not self._load_deals():
                return False

        # Convert timestamp columns using Polars
        timestamp_cols = ['time', 'createTime', 'updateTime', 'valueDate']
        timestamp_exprs = []
        for col in timestamp_cols:
            if col in self.deals.columns:
                timestamp_exprs.append(pl.col(col).cast(pl.Datetime("ms")))
        
        if timestamp_exprs:
            self.deals = self.deals.with_columns(timestamp_exprs)
        
        if map_enums:
            # Create mapping expressions for enum fields
            enum_exprs = []
            for col in self.mappings['fields']:
                if col in self.deals.columns:
                    # Create a mapping dictionary for Polars with integer keys
                    mapping_dict = {int(k): v for k, v in self.mappings['fields'][col].items()}
                    # Use replace with proper type handling
                    enum_exprs.append(pl.col(col).cast(pl.Utf8).replace(mapping_dict))
            
            if enum_exprs:
                self.deals = self.deals.with_columns(enum_exprs)

        # Drop columns
        drop_cols = ['ID$', 'TIME$', 'SOURCE$', 'id', 'orderId', 'trader', 'account', 'extOrderId', 'extDealId', 'orderFlags', 'valueDate', 'rateLimit']
        existing_drop_cols = [col for col in drop_cols if col in self.deals.columns]
        if existing_drop_cols:
            self.deals = self.deals.drop(existing_drop_cols)
        
        # Map instrument names
        if "instrument" in self.deals.columns:
            self.deals = self.deals.with_columns([
                pl.col("instrument").cast(pl.Utf8).replace(self.mappings['instruments_long_names'])
            ])
        
        return self.deals is not None and not self.deals.is_empty()
        
if __name__ == "__main__":
    # deals_parser = DealsParser()
    # deals_parser.run_worker()
    # print(deals_parser.deals.head())
    
    import glob, gc
    for file in glob.glob('data/_tmp/*.csv.xz'):
        print(file)
        parser = MdParser(file)
        parser.build_book('mbo')
        pq_name = file.split("/")[-1].replace('.csv.xz', '.parquet')
        pq_path = f'data/pq_mbo/{pq_name}'
        parser.orderbook.write_parquet(pq_path, compression='zstd', compression_level=10)
        del parser;gc.collect()
        time.sleep(0.3)
    



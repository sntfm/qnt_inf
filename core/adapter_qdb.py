import requests
import os
import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime
from questdb.ingress import Sender, IngressError, TimestampNanos

QUESTDB_HOST = "localhost"
QUESTDB_PORT_HTTP = 9000
QUESTDB_PORT_ILP = 9009  # ILP port
QUESTDB_TABLE_INGRESS = "index_ingress"

class AdapterQdb:
    def __init__(self, host):
        self.host = host
        self.http_port = QUESTDB_PORT_HTTP
        self.ilp_port = QUESTDB_PORT_ILP
        self.ingress_table = QUESTDB_TABLE_INGRESS

    def table_exists(self, table_name):
        """Check if a table exists in QuestDB"""
        try:
            # Query QuestDB's system tables to check if table exists
            # Use a COUNT query which will return 0 if table doesn't exist
            query = f"SELECT COUNT(*) FROM '{table_name}'"
            response = requests.get(
                f"http://{self.host}:{self.http_port}/exec",
                params={'query': query},
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code == 400:
                # Table doesn't exist
                return False
            else:
                print(f"Error checking if table exists: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return False
        except requests.exceptions.ConnectionError as e:
            print(f"Make sure QuestDB is running. Connection error: {e}")
            return False
        except Exception as e:
            print(f"Error checking if table exists: {e}")
            return False

    def create_table(self, table_name, sql_statement):
        """Execute a CREATE TABLE statement"""
        try:
            # QuestDB uses GET for queries, including DDL
            response = requests.get(
                f"http://{self.host}:{self.http_port}/exec",
                params={'query': sql_statement},
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                print(f"Table '{table_name}' created successfully")
                return True
            else:
                print(f"Error creating table '{table_name}': HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return False
        except requests.exceptions.ConnectionError as e:
            print(f"Make sure QuestDB is running. Connection error: {e}")
            return False
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
            return False

    def check_ingress_table(self, file_name, book_type, depth=0):
        """Check if the file has already been ingested into QuestDB table"""
        # Format book_type consistently with how it's stored in update_ingress_table
        book_type_formatted = f'{book_type}_{depth}' if depth > 0 else book_type
        try:
            if not self.table_exists(self.ingress_table):
                query = """
                        CREATE TABLE IF NOT EXISTS index_ingress (
                        file SYMBOL CAPACITY 100000 CACHE,
                        venue SYMBOL,
                        book_type SYMBOL,
                        rows LONG,
                        ts TIMESTAMP) TIMESTAMP(ts);
                        """
                self.create_table(self.ingress_table, query)

            query = f"SELECT file FROM {QUESTDB_TABLE_INGRESS} WHERE file = '{file_name}' AND book_type = '{book_type_formatted}'"
            response = requests.get(
                f"http://{self.host}:{QUESTDB_PORT_HTTP}/exec",
                params={'query': query},
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'dataset' in data and len(data['dataset']) > 0:
                    print(f"'{file_name}' already exists in qdb table: '{QUESTDB_TABLE_INGRESS}'")
                    return True
                else:
                    print(f"'{file_name}' not found in qdb table: '{QUESTDB_TABLE_INGRESS}'")
                    return False
            else:
                print(f"Error checking QuestDB: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return False
        except requests.exceptions.ConnectionError as e:
            print(f"Make sure QuestDB is running. Connection error: {e}")
            return False
        except Exception as e:
            print(f"Error checking QuestDB: {e}")
            return False

    def fetch_ingress_table(self, book_type):
        # Create the table if it doesn't exist
        if not self.table_exists(self.ingress_table):
            create_query = """
                    CREATE TABLE IF NOT EXISTS index_ingress (
                    file SYMBOL CAPACITY 100000 CACHE,
                    venue SYMBOL,
                    book_type SYMBOL,
                    rows LONG,
                    ts TIMESTAMP) TIMESTAMP(ts);
                    """
            self.create_table(self.ingress_table, create_query)
        
        query = f"SELECT file FROM {QUESTDB_TABLE_INGRESS} WHERE book_type = '{book_type}'"
        response = requests.get(
            f"http://{self.host}:{QUESTDB_PORT_HTTP}/exec",
            params={'query': query},
            headers={'Accept': 'application/json'}
        )
        if response.status_code == 200:
            data = response.json()
            if 'dataset' in data and len(data['dataset']) > 0: return list(data['dataset'])
            else: return []
        else:
            print(f"Error fetching ingress table: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            return []

    def update_ingress_table(self, file_name, book_type, row_count):
        """Update the ingress table with file processing information"""
        try:
            current_time = datetime.now()
            venue = file_name.split('-')[0]
            # Insert record into ingress table using ILP
            conf = f'tcp::addr={self.host}:{self.ilp_port};'
            
            with Sender.from_conf(conf) as sender:
                sender.row(
                    table_name=self.ingress_table,
                    symbols={
                        "file": file_name,
                        "book_type": book_type,
                        "venue": venue
                    },
                    columns={"rows": row_count},
                    at=TimestampNanos(int(current_time.timestamp() * 1_000_000_000))
                )
                sender.flush()
            
            print(f"Updated ingress table {self.ingress_table} with file: {file_name}, type: {book_type}, rows: {row_count}")
            return True
            
        except IngressError as e:
            print(f"Failed to update ingress table: {e}")
            return False
        except Exception as e:
            print(f"Error updating ingress table: {e}")
            return False

    def write_deals_to_qdb(self, df):
        """Write deals data to QuestDB deals table"""
        try:
            table_name = 'deals'
            
            # Convert Polars to Pandas if needed
            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()
            
            # Ensure table exists
            if not self.table_exists(table_name):
                query = """
                        CREATE TABLE IF NOT EXISTS deals (
                            time TIMESTAMP,
                            instrument SYMBOL CAPACITY 1000 CACHE,
                            side SYMBOL CAPACITY 10 CACHE,
                            amt DOUBLE,
                            px DOUBLE,
                            orderKind SYMBOL CAPACITY 20 CACHE,
                            orderType SYMBOL CAPACITY 20 CACHE,
                            tif SYMBOL CAPACITY 10 CACHE,
                            orderStatus SYMBOL CAPACITY 20 CACHE
                        ) TIMESTAMP(time) PARTITION BY DAY WAL
                        DEDUP UPSERT KEYS(time, instrument, side, amt, px);
                        """
                self.create_table(table_name, query)
            
            # Ingest data to QuestDB via ILP
            conf = f'tcp::addr={self.host}:{self.ilp_port};'
            row_count = 0
            
            with Sender.from_conf(conf) as sender:
                for _, row in df.iterrows():
                    # Build columns and symbols dictionaries
                    columns = {}
                    symbols = {}
                    
                    # Identify symbol columns
                    symbol_cols = ['instrument', 'side', 'orderKind', 'orderType', 'tif', 'orderStatus']
                    for col in df.columns:
                        val = row.get(col)
                        if pd.notna(val):  # Only add non-null values
                            if col in symbol_cols:
                                symbols[col] = val
                            elif col != 'time':  # time goes to 'at' parameter
                                columns[col] = val
                    
                    # Get timestamp
                    if 'time' in df.columns:
                        ts_val = row['time']
                        # Handle both integer microseconds/milliseconds and datetime objects
                        if pd.api.types.is_integer(ts_val) or isinstance(ts_val, np.integer):
                            # Assume milliseconds for deals (based on DealsParser line 389 casting to Datetime("ms"))
                            at = TimestampNanos(int(ts_val * 1_000_000))  # Convert ms to nanos
                        else:
                            # Already a datetime
                            ts = pd.to_datetime(ts_val)
                            at = TimestampNanos(ts.value)
                    else:
                        at = TimestampNanos(int(datetime.now().timestamp() * 1_000_000_000))
                    
                    sender.row(
                        table_name=table_name,
                        symbols=symbols if symbols else None,
                        columns=columns,
                        at=at
                    )
                    row_count += 1
                
                sender.flush()
                print(f"Sent {row_count} rows to QuestDB table '{table_name}'")
                return row_count
            
        except IngressError as e:
            print(f"Failed to write deals to QuestDB: {e}")
            return 0
        except Exception as e:
            print(f"Error writing deals to QuestDB: {e}")
            return 0

    def create_md_query(self, venue, book_type, depth=0):
        table_name = f"feed_{venue}_{book_type}{f'_{depth}' if depth > 0 else ''}"
        query_prefix = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_server TIMESTAMP,
            seq_server LONG,
            ts_exch LONG,
            instrument SYMBOL,
            asks_cnt INT,
            bids_cnt INT,
        """

        query_suffix = """
        ) TIMESTAMP(ts_server) PARTITION BY DAY WAL
        DEDUP UPSERT KEYS(ts_server, instrument);
        """
        
        if book_type == 'mbo':
            query_body = \
            """
            asks_mbo_amt DOUBLE, asks_mbo_px DOUBLE, bids_mbo_amt DOUBLE, bids_mbo_px DOUBLE
            """
        elif book_type == 'mbp':
            query_body = \
            """
            asks_mbp_px DOUBLE, asks_mbp_amt DOUBLE, bids_mbp_px DOUBLE, bids_mbp_amt DOUBLE
            """
        elif book_type == 'tob':
            query_body =\
            """
            asks_lvl_cnt INT,
            bids_lvl_cnt INT,
            """
            for i in range(depth):
                comma = "," if i < depth - 1 else ""
                query_body += \
                f"""
                ask_px_{i} DOUBLE, ask_amt_{i} DOUBLE, bid_px_{i} DOUBLE, bid_amt_{i} DOUBLE{comma}
                """
        else:
            raise ValueError(f"Invalid book type: {book_type}")

        query = query_prefix + query_body + query_suffix
        return query

    def write_md_to_qdb(self, df, venue, book_type, depth=0, batch_size: int = 100_000, id=0):
        table_name = f"feed_{venue}_{book_type}{f'_{depth}' if depth > 0 else ''}"
        """Write market data to QuestDB in batches using ILP."""
        try:
            # Convert Polars to Pandas if needed
            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()
            
            # Ensure table exists
            if not self.table_exists(table_name):
                query = self.create_md_query(venue, book_type, depth)
                self.create_table(table_name, query)

            # Determine timestamp column
            timestamp_col = 'ts_server' if 'tob' in table_name.lower() else None

            conf = f'tcp::addr={self.host}:{self.ilp_port};'
            total_rows = 0
            batch_rows = 0

            with Sender.from_conf(conf) as sender:
                # Iterate in batches
                for batch_start in range(0, len(df), batch_size):
                    batch_df = df.iloc[batch_start:batch_start + batch_size]

                    for _, row in batch_df.iterrows():
                        # Symbols (tag columns)
                        symbols = {}
                        exclude_cols = set()
                        if 'instrument' in df.columns:
                            symbols['instrument'] = row.get('instrument', 'unknown')
                            exclude_cols.add('instrument')

                        # Columns (fields)
                        columns = {}
                        for col in df.columns:
                            if col not in exclude_cols and col != timestamp_col:
                                val = row.get(col)
                                if pd.notna(val):
                                    columns[col] = val

                        # Timestamp
                        if timestamp_col and timestamp_col in df.columns:
                            ts_val = row[timestamp_col]
                            # Handle both integer timestamps and datetime objects
                            if pd.api.types.is_integer(ts_val) or isinstance(ts_val, np.integer):
                                # Integer in milliseconds (from parsers)
                                at = TimestampNanos(int(ts_val * 1_000_000))  # Convert ms to nanos
                            else:
                                # Already a datetime
                                ts = pd.to_datetime(ts_val)
                                at = TimestampNanos(ts.value)
                        else:
                            at = TimestampNanos(int(datetime.now().timestamp() * 1_000_000_000))

                        sender.row(
                            table_name=table_name,
                            symbols=symbols if symbols else None,
                            columns=columns,
                            at=at
                        )

                        batch_rows += 1
                        total_rows += 1

                    # Flush after each batch
                    sender.flush()
                    print(f"Worker {id} [write_md_to_qdb]: Flushed {batch_rows} rows to '{table_name}' (total {total_rows})")
                    batch_rows = 0

            print(f"Finished sending {total_rows} rows to QuestDB table '{table_name}'")
            return total_rows

        except IngressError as e:
            print(f"QuestDB ingress error: {e}")
            return 0
        except Exception as e:
            print(f"Unexpected error: {e}")
            return 0

    def fetch(self, table_name, start_date=None, end_date=None, instrument=None, columns='*', timestamp_col=None):
        """Fetch data from QuestDB table between dates, optionally filtered by instrument.
        
        Args:
            table_name: Name of the QuestDB table
            start_date: Start date in QuestDB format (YYYY-MM-DDTHH:MM:SS.mmmZ) or datetime object.
                       Defaults to 1970-01-01 if not provided.
            end_date: End date in QuestDB format (YYYY-MM-DDTHH:MM:SS.mmmZ) or datetime object. 
                     Defaults to current time if not provided.
            instrument: Optional instrument filter
            columns: Columns to select (default: '*')
            timestamp_col: Timestamp column name. Auto-detects if None based on table pattern.
        """
        if start_date is None: start_date = datetime(1970, 1, 1)
        elif isinstance(start_date, str):
            # Parse QuestDB date format (YYYY-MM-DDTHH:MM:SS.mmmZ)
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        
        # Default to current time if end_date not provided
        if end_date is None:
            end_date = datetime.now()
        elif isinstance(end_date, str):
            # Parse QuestDB date format (YYYY-MM-DDTHH:MM:SS.mmmZ)
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        # Auto-detect timestamp column based on table name
        if timestamp_col is None:
            if table_name == 'deals':
                timestamp_col = 'time'
            elif table_name.startswith('feed_'):
                timestamp_col = 'ts_server'
            else:
                timestamp_col = 'ts'  # fallback
        
        # Build WHERE clause
        # QuestDB expects ISO format with microseconds and Z suffix
        def format_for_questdb(dt):
            # Format as YYYY-MM-DDTHH:MM:SS.ffffffZ
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        where_clauses = [
            f"{timestamp_col} BETWEEN '{format_for_questdb(start_date)}' AND '{format_for_questdb(end_date)}'"
        ]
        
        if instrument is not None:
            if isinstance(instrument, str):
                where_clauses.append(f"instrument = '{instrument}'")
            elif isinstance(instrument, list):
                instruments_str = "', '".join(instrument)
                where_clauses.append(f"instrument IN ('{instruments_str}')")
        
        where_clause = " AND ".join(where_clauses)
        query = f"SELECT {columns} FROM {table_name} WHERE {where_clause}"
        # print(f"Executing query: {query}")  # Debug output
        
        try:
            response = requests.get(
                f"http://{self.host}:{self.http_port}/exec",
                params={'query': query},
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'dataset' in data and len(data['dataset']) > 0:
                    # Extract column names from response
                    column_names = [col['name'] for col in data.get('columns', [])]
                    
                    # Create DataFrame with proper column names
                    if column_names:
                        df = pd.DataFrame(data['dataset'], columns=column_names)
                    else:
                        df = pd.DataFrame(data['dataset'])
                    
                    # Convert timestamp columns to datetime
                    if 'ts_server' in df.columns:
                        df['ts_server'] = pd.to_datetime(df['ts_server'])
                    elif 'ts' in df.columns:
                        df['ts'] = pd.to_datetime(df['ts'])
                    elif 'time' in df.columns:
                        df['time'] = pd.to_datetime(df['time'])
                    print(f"Fetched {len(df)} rows from '{table_name}'")
                    return df
                else:
                    print(f"No data found in '{table_name}'")
                    return pd.DataFrame()
            else:
                print(f"Error querying QuestDB: HTTP {response.status_code}")
                return pd.DataFrame()
                
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching from QuestDB: {e}")
            return pd.DataFrame()


if __name__ == "__main__":
    adapter = AdapterQdb('localhost')


    df_deals = pd.read_parquet('/Users/sntfm/github/p_collab/data/deals.parquet')
    adapter.write_deals_to_qdb(df_deals)

    path = '/Users/sntfm/github/p_collab/data/md/binance/binance-251014-0000-tob_20.parquet'
    df_md = pd.read_parquet(path)
    print(df_md.head(3))
    
    path = '/Users/sntfm/github/p_collab/data/_tmp/binance-251015-0000.csv.xz'
    file_name = path.split("/")[-1]
    venue = file_name.split('-')[0]
    book_type = 'tob'
    depth = 20
    # print(file_name)

    if not adapter.check_ingress_table(file_name, book_type, depth):
        adapter.write_md_to_qdb(df_md, venue, book_type, depth)
        adapter.update_ingress_table(file_name, f'{book_type}_{depth}', df_md.shape[0])

    files = adapter.fetch_ingress_table('binance', 'tob_20')
    print(files)
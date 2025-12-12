import os
import pandas as pd
from sqlalchemy import create_engine

# QuestDB connection
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "16.171.14.188")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

connection_string = f"postgresql://{QUESTDB_USER}:{QUESTDB_PASSWORD}@{QUESTDB_HOST}:{QUESTDB_PORT}/{QUESTDB_DB}"
engine = create_engine(connection_string, connect_args={"connect_timeout": 30})

# Query ADA instruments
query = """
SELECT * FROM map_decomposition_usd
WHERE instrument LIKE '%ADA%'
ORDER BY instrument
"""

df = pd.read_sql(query, engine)
print("ADA Instrument Mappings:")
print(df.to_string())

engine.dispose()

import streamlit as st
import pandas as pd
import requests
import os

# Use environment variable if set (for Docker), otherwise use default
QDB_HTTP = os.getenv("QDB_HTTP", "http://16.171.14.188:9000")
TABLE = "feed_kraken_tob_5"

st.title("QuestDB Latency Histogram")

def query_questdb(sql: str):
    resp = requests.get(f"{QDB_HTTP}/exec", params={"query": sql})
    resp.raise_for_status()
    data = resp.json()
    col_names = [c["name"] for c in data["columns"]]
    return pd.DataFrame(data["dataset"], columns=col_names)

bin_size = st.number_input("Histogram bin size (ms)", 1, 1000, 10)
limit = st.number_input("Rows to analyze (QuestDB LIMIT)", 100, 1_000_000, 50000)

if st.button("Build Histogram"):
    sql_hist = f"""
        SELECT 
            floor(latency_ms / {bin_size}) * {bin_size} AS bucket,
            count() AS count
        FROM (
            SELECT ts_server - ts_exch AS latency_ms
            FROM {TABLE}
            ORDER BY ts_server DESC
            LIMIT {limit}
        )
        GROUP BY bucket
        ORDER BY bucket
    """

    st.code(sql_hist)

    df_hist = query_questdb(sql_hist)
    df_hist = df_hist.astype({"bucket": float, "count": int})

    st.subheader("Latency Histogram")
    st.dataframe(df_hist)

    st.subheader("Histogram Chart")
    st.bar_chart(df_hist, x="bucket", y="count")

# streamlit run st_latency_hist.py
# http://localhost:8501
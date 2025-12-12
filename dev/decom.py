import requests

QUESTDB_HOST = "16.171.14.188"
QUESTDB_PORT_HTTP = 9000
TABLE_NAME = "map_decomposition_usd"

instruments = {
    "800373204": "Kraken.Spot.ADA/BTC_SPOT",
    "800373304": "Kraken.Spot.ADA/ETH_SPOT",
    "800370404": "Kraken.Spot.ADA/EUR_SPOT",
    "800370504": "Kraken.Spot.ADA/GBP_SPOT",
    "800373104": "Kraken.Spot.ADA/USDC_SPOT",
    "800373004": "Kraken.Spot.ADA/USDT_SPOT",
    "800370004": "Kraken.Spot.ADA/USD_SPOT",
    "800383204": "Kraken.Spot.BCH/BTC_SPOT",
    "800383304": "Kraken.Spot.BCH/ETH_SPOT",
    "800380404": "Kraken.Spot.BCH/EUR_SPOT",
    "800380504": "Kraken.Spot.BCH/GBP_SPOT",
    "800383104": "Kraken.Spot.BCH/USDC_SPOT",
    "800383004": "Kraken.Spot.BCH/USDT_SPOT",
    "800380004": "Kraken.Spot.BCH/USD_SPOT",
    "800320204": "Kraken.Spot.BTC/CHF_SPOT",
    "800320404": "Kraken.Spot.BTC/EUR_SPOT",
    "800320504": "Kraken.Spot.BTC/GBP_SPOT",
    "800323104": "Kraken.Spot.BTC/USDC_SPOT",
    "800323004": "Kraken.Spot.BTC/USDT_SPOT",
    "800320004": "Kraken.Spot.BTC/USD_SPOT",
    "800393204": "Kraken.Spot.DOGE/BTC_SPOT",
    "800390404": "Kraken.Spot.DOGE/EUR_SPOT",
    "800390504": "Kraken.Spot.DOGE/GBP_SPOT",
    "800393104": "Kraken.Spot.DOGE/USDC_SPOT",
    "800393004": "Kraken.Spot.DOGE/USDT_SPOT",
    "800390004": "Kraken.Spot.DOGE/USD_SPOT",
    "800333204": "Kraken.Spot.ETH/BTC_SPOT",
    "800330204": "Kraken.Spot.ETH/CHF_SPOT",
    "800330404": "Kraken.Spot.ETH/EUR_SPOT",
    "800330504": "Kraken.Spot.ETH/GBP_SPOT",
    "800333104": "Kraken.Spot.ETH/USDC_SPOT",
    "800333004": "Kraken.Spot.ETH/USDT_SPOT",
    "800330004": "Kraken.Spot.ETH/USD_SPOT",
    "800040204": "Kraken.Spot.EUR/CHF_SPOT",
    "800040504": "Kraken.Spot.EUR/GBP_SPOT",
    "800040004": "Kraken.Spot.EUR/USD_SPOT",
    "800050004": "Kraken.Spot.GBP/USD_SPOT",
    "800353204": "Kraken.Spot.LTC/BTC_SPOT",
    "800353304": "Kraken.Spot.LTC/ETH_SPOT",
    "800350404": "Kraken.Spot.LTC/EUR_SPOT",
    "800350504": "Kraken.Spot.LTC/GBP_SPOT",
    "800353104": "Kraken.Spot.LTC/USDC_SPOT",
    "800353004": "Kraken.Spot.LTC/USDT_SPOT",
    "800350004": "Kraken.Spot.LTC/USD_SPOT",
    "800343204": "Kraken.Spot.SOL/BTC_SPOT",
    "800343304": "Kraken.Spot.SOL/ETH_SPOT",
    "800340404": "Kraken.Spot.SOL/EUR_SPOT",
    "800340504": "Kraken.Spot.SOL/GBP_SPOT",
    "800343104": "Kraken.Spot.SOL/USDC_SPOT",
    "800343004": "Kraken.Spot.SOL/USDT_SPOT",
    "800340004": "Kraken.Spot.SOL/USD_SPOT",
    "800000204": "Kraken.Spot.USD/CHF_SPOT",
    "800310204": "Kraken.Spot.USDC/CHF_SPOT",
    "800310404": "Kraken.Spot.USDC/EUR_SPOT",
    "800310504": "Kraken.Spot.USDC/GBP_SPOT",
    "800313004": "Kraken.Spot.USDC/USDT_SPOT",
    "800310004": "Kraken.Spot.USDC/USD_SPOT",
    "800300204": "Kraken.Spot.USDT/CHF_SPOT",
    "800300404": "Kraken.Spot.USDT/EUR_SPOT",
    "800300504": "Kraken.Spot.USDT/GBP_SPOT",
    "800300004": "Kraken.Spot.USDT/USD_SPOT",
    "800363204": "Kraken.Spot.XRP/BTC_SPOT",
    "800363304": "Kraken.Spot.XRP/ETH_SPOT",
    "800360404": "Kraken.Spot.XRP/EUR_SPOT",
    "800360504": "Kraken.Spot.XRP/GBP_SPOT",
    "800363104": "Kraken.Spot.XRP/USDC_SPOT",
    "800363004": "Kraken.Spot.XRP/USDT_SPOT",
    "800360004": "Kraken.Spot.XRP/USD_SPOT"
}

priority = ["USDT", "USDC", "USD", "EUR", "GBP", "CHF"]

# Build lookup table for all major pairs
majors = {}
for v in instruments.values():
    base, quote = v.split(".")[2].replace("_SPOT", "").split("/")
    if quote in priority:
        majors.setdefault(base, []).append((quote, v))

# choose best major for a given asset
def best_major(asset):
    if asset not in majors:
        return None
    ranked = sorted(majors[asset], key=lambda x: priority.index(x[0]))
    return ranked[0][1]

# build USD-quote lookup
usd_quote = {}
for v in instruments.values():
    base, quote = v.split(".")[2].replace("_SPOT", "").split("/")
    if quote == "USD":
        usd_quote[base] = v

# final decomposition dict
full_dict = {}
for key, name in instruments.items():
    pair = name.split(".")[2].replace("_SPOT", "")
    base, quote = pair.split("/")

    # stable-quoted → no split
    if quote in ("USDT", "USDC", "USD"):
        full_dict[name] = True
        continue
    
    # USD/CHF is a major pair itself, no decomposition needed
    if base == "USD" and quote == "CHF":
        full_dict[name] = True
        continue

    base_major = best_major(base)
    
    # For CHF pairs, use USD/CHF instead of looking for CHF/USD
    if quote == "CHF":
        quote_major = "Kraken.Spot.USD/CHF_SPOT"
    else:
        quote_major = usd_quote.get(quote)

    full_dict[name] = (base_major, quote_major)


# full_dict now contains your complete mapping
def write_to_questdb(decomposition_dict):
    """Write decomposition mappings to QuestDB table map_decomposition_usd"""
    try:
        # First, truncate the table to clear old data
        truncate_query = f"TRUNCATE TABLE {TABLE_NAME};"
        response = requests.get(
            f"http://{QUESTDB_HOST}:{QUESTDB_PORT_HTTP}/exec",
            params={'query': truncate_query},
            headers={'Accept': 'application/json'}
        )

        if response.status_code != 200:
            print(f"Warning: Could not truncate table: {response.text}")

        # Build INSERT statements
        insert_values = []
        for instrument, value in decomposition_dict.items():
            # Determine if it's a major pair
            is_major = (value is True)

            # For major pairs, base and quote are NULL
            # For decomposed pairs, extract the base and quote instruments
            if is_major:
                instrument_base = instrument
                instrument_quote = None
                instrument_usd = None
                inst_usd_is_inverted = False
            else:
                # value is a tuple (base_major, quote_major)
                instrument_base = value[0] if value[0] is not None else None
                instrument_quote = value[1] if value[1] is not None else None

                # Determine USD conversion instrument
                # For pairs like ADA/BTC, we need BTC/USD for conversion
                pair = instrument.split(".")[2].replace("_SPOT", "")
                base, quote = pair.split("/")

                # Check if quote currency has a USD pair
                instrument_usd = usd_quote.get(quote, None)

                # Determine if the USD conversion needs to be inverted
                # The key question: Given our pair X/Y, if the reference is Y/USD,
                # should we multiply or divide by Y/USD to get X in USD?
                #
                # Mathematical analysis:
                #   X/Y = A (Y per X)
                #   Y/USD = B (USD per Y)
                #   To get X in USD: (X/Y) * (Y/USD) = A * B = USD per X
                #   So we should MULTIPLY (NOT inverted)
                #
                # HOWEVER, if the reference pair naming is Y/USD but the actual
                # price semantics in the feed data are inverted (USD/Y), then:
                #   X/Y = A (Y per X)
                #   "Y/USD" feed = B (actually Y per USD, not USD per Y!)
                #   To get X in USD: (X/Y) / (Y/USD) = A / B = USD per X
                #   So we should DIVIDE (IS inverted)
                #
                # Based on the bug report: GBP pairs need division, not multiplication
                # This means GBP/USD feed contains GBP per USD, not USD per GBP
                # Similarly for EUR and CHF pairs with fiat quote currencies
                #
                # Rule: Fiat quote currencies (GBP, EUR, CHF) have inverted USD pairs
                # Crypto quote currencies (BTC, ETH) have normal USD pairs
                if instrument_usd:
                    fiat_currencies = {"GBP", "EUR", "CHF"}
                    inst_usd_is_inverted = (quote in fiat_currencies)
                else:
                    inst_usd_is_inverted = False

            # Escape single quotes in strings and format for SQL
            instrument_esc = instrument.replace("'", "''")

            # Format string fields: None -> NULL, string -> 'string'
            def format_sql_string(value):
                if value is None:
                    return "NULL"
                return f"'{value.replace(chr(39), chr(39)*2)}'"

            instrument_base_sql = format_sql_string(instrument_base)
            instrument_quote_sql = format_sql_string(instrument_quote)
            instrument_usd_sql = format_sql_string(instrument_usd)

            # Build the VALUES clause
            insert_values.append(
                f"('{instrument_esc}', {str(is_major).lower()}, {instrument_base_sql}, {instrument_quote_sql}, {instrument_usd_sql}, {str(inst_usd_is_inverted).lower()})"
            )
        
        # Execute INSERT in batches (QuestDB can handle large queries, but let's be safe)
        batch_size = 50
        total_inserted = 0
        
        for i in range(0, len(insert_values), batch_size):
            batch = insert_values[i:i+batch_size]
            insert_query = f"INSERT INTO {TABLE_NAME} (instrument, is_major, instrument_base, instrument_quote, instrument_usd, inst_usd_is_inverted) VALUES {', '.join(batch)};"
            
            response = requests.get(
                f"http://{QUESTDB_HOST}:{QUESTDB_PORT_HTTP}/exec",
                params={'query': insert_query},
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                total_inserted += len(batch)
            else:
                print(f"Error inserting batch: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return False
        
        print(f"Successfully wrote {total_inserted} rows to {TABLE_NAME}")
        return True
        
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
        print(f"Make sure QuestDB is running at {QUESTDB_HOST}:{QUESTDB_PORT_HTTP}")
        return False
    except Exception as e:
        print(f"Error writing to QuestDB: {e}")
        return False


if __name__ == "__main__":
    # Print the decomposition for debugging
    for k, v in full_dict.items():
        print(k, v)
    
    # Write to QuestDB
    print("\nWriting to QuestDB...")
    write_to_questdb(full_dict) 
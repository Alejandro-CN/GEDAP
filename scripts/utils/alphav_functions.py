from dotenv import load_dotenv
import os
import requests
import sqlite3
import time




# load .env by searching upward from this file until one is found
load_dotenv()

# Getting API key from environment variable
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
if not ALPHAVANTAGE_API_KEY:
    raise RuntimeError("Missing ALPHAVANTAGE_API_KEY environment variable")




# ------------------------------------------------------------------------------------ 
# Function 1: Return max_data_date or None if no metadata exists.
def get_metadata(conn, source_type, symbol, market, interval):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT max_data_date
        FROM alphav_metadata
        WHERE source_type = ?
          AND symbol = ?
          AND market = ?
          AND interval = ?
    """, (source_type, symbol, market, interval))

    result = cursor.fetchone()
    return result[0] if result else None




# ------------------------------------------------------------------------------------ 
# Function 2: Fetch data from AlphaVantage.
def fetch_alpha_vantage(alphav_params: dict):
    url = "https://www.alphavantage.co/query"
    alphav_params["apikey"] = ALPHAVANTAGE_API_KEY  
    response = requests.get(url, params=alphav_params)
    data = response.json()

    # ---- CASE 1: stocks, fx, crypto ----
    meta = data.get("Meta Data", {})
    # Detect series key automatically
    series_key = next((k for k in data if "Time Series" in k), None)
    if series_key:
        return meta, data.get(series_key, {})

    # ---- CASE 2: commodity endpoints ----
    if "data" in data and "name" in data:
        meta = {
            "name": data.get("name"),
            "interval": data.get("interval"),
            "unit": data.get("unit")
        }
        series = data.get("data", [])
        return meta, series

    # ---- CASE 3: API error or unexpected response ----
    return meta, {}




# ------------------------------------------------------------------------------------ 
# Function 3.1.1: Parse stocks rows    
def parse_stocks_row(symbol, date, values):
    return (
        symbol,
        date,
        float(values["1. open"]),
        float(values["2. high"]),
        float(values["3. low"]),
        float(values["4. close"]),
        int(values.get("5. volume"))
    )

# Function 3.1.2: Parse fx rows
def parse_fx_row(from_currency, to_currency, date, values):
    return (
        from_currency,
        to_currency,
        date,
        float(values["1. open"]),
        float(values["2. high"]),
        float(values["3. low"]),
        float(values["4. close"])
    )

# Function 3.1.3: Parse crypto rows
def parse_crypto_row(crypto_code, fiat_currency, date, values):
    return (
        crypto_code,
        fiat_currency,
        date,
        float(values["1. open"]),
        float(values["2. high"]),
        float(values["3. low"]),
        float(values["4. close"]),
        float(values.get("5. volume"))
    )

# Function 3.1.4: Parse commodity rows
def parse_commodity_row(commodity_id, date, values):  
    def _safe_float(val):
        if val is None:
            return None
        s = str(val).strip()
        # common placeholders for missing values
        if s in ("", ".", "-", "—", "N/A", "na", "None"):
            return None
        # remove thousands separators and spaces
        s = s.replace(",", "").replace(" ", "")
        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    return (
        commodity_id,
        date,
        _safe_float(values.get("value"))
    )




# ------------------------------------------------------------------------------------ 
# Function 3.2.0: Insert rows into tables -> helper function.
def insert_rows(conn, insert_sql, rows):
    cursor = conn.cursor()
    cursor.executemany(insert_sql, rows)
    conn.commit()
    
# Function 3.2.1: Insert stocks rows into alphav_stocks_daily table.
def insert_stocks_rows(conn, rows):
    insert_sql = """
        INSERT OR IGNORE INTO alphav_stocks_daily
        (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    insert_rows(conn, insert_sql, rows)

# Function 3.2.2: Insert fx rows into alphav_fx_daily table.
def insert_fx_rows(conn, rows):
    insert_sql = """
        INSERT OR IGNORE INTO alphav_fx_daily
        (from_currency, to_currency, date, open, high, low, close)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    insert_rows(conn, insert_sql, rows)

# Function 3.2.3: Insert crypto rows into alphav_crypto_daily table.
def insert_crypto_rows(conn, rows):
    insert_sql = """
        INSERT OR IGNORE INTO alphav_crypto_daily
        (crypto_code, fiat_currency, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_rows(conn, insert_sql, rows)

# Function 3.2.4: Insert commodity rows into alphav_commodity table.
def insert_commodities_rows(conn, rows):
    insert_sql = """
        INSERT OR IGNORE INTO alphav_commodity
        (commodity_id, date, value)
        VALUES (?, ?, ?)
    """
    insert_rows(conn, insert_sql, rows)


def upsert_commodity_lookup(conn, commodity_id, commodity_name, interval, unit):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO alphav_commodity_lookup
        (commodity_id, commodity_name, interval, unit)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(commodity_id) DO UPDATE SET
            commodity_name = excluded.commodity_name,
            interval = excluded.interval,
            unit = excluded.unit
    """, (commodity_id, commodity_name, interval, unit))
    conn.commit()




# ------------------------------------------------------------------------------------ 
# Function 4.0: Filter and sort series data
def filter_and_sort(series, max_data_date, full_load):
    print(f"DEBUG filter_and_sort: full_load={full_load}, max_data_date={max_data_date}")
    
    # Case 1: dict-based time series (stocks, fx, crypto)
    if isinstance(series, dict):
        print(f"DEBUG: Processing dict-based series with {len(series)} items")
        rows = [
            (date, values)
            for date, values in series.items()
            if full_load or date > max_data_date
        ]
        print(f"DEBUG: After filtering, {len(rows)} rows remain")
        return sorted(rows, key=lambda x: x[0])

    # Case 2: list-based series (commodities)
    print(f"DEBUG: Processing list-based series with {len(series)} items")
    rows = [
        (item["date"], item)
        for item in series
        if full_load or item["date"] > max_data_date
    ]
    print(f"DEBUG: After filtering, {len(rows)} rows remain")
    return sorted(rows, key=lambda x: x[0])



# ------------------------------------------------------------------------------------ 
# Function 5: Extract date from row based on source type  
def extract_date(row, source_type):
    if source_type in ("stocks", "commodity"):
        return row[1]
    else:
        return row[2]




# ------------------------------------------------------------------------------------ 
# Function 6: Upsert metadata for Alphavantage
def upsert_metadata(conn, source_type, symbol, market, interval, max_data_date, api_last_refresh):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO alphav_metadata (
            source_type, symbol, market, interval, max_data_date, api_last_refresh
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_type, symbol, market, interval)
        DO UPDATE SET 
            max_data_date = excluded.max_data_date,
            api_last_refresh = excluded.api_last_refresh,
            update_date = CURRENT_TIMESTAMP
    """, (source_type, symbol, market, interval, max_data_date, api_last_refresh))

    conn.commit()




# ------------------------------------------------------------------------------------ 
# Function X: alphav_loader
def alphav_loader(alphav_params, source_type, symbol, market="USD", interval="daily", history_sweep=False):
    conn = sqlite3.connect("./GEDAP_DB.db")
    conn.execute("PRAGMA foreign_keys = ON;")

    print(f"=== Loading {source_type}, {symbol} , {market} ===")

    # ----------------------------------------------------
    # 1. Look up metadata to determine full vs incremental
    if history_sweep:
        max_data_date = None
        full_load = True
    else:
        max_data_date = get_metadata(conn, source_type, symbol, market, interval)
        full_load = max_data_date is None

    print(f"Max data date = {max_data_date}")
    print("Performing FULL LOAD" if full_load else "Performing INCREMENTAL LOAD")

    # ----------------------------------------------------
    # 2. Fetch data from AlphaVantage
    meta, series = fetch_alpha_vantage(alphav_params)

    api_last_refresh = next(
        (v for k, v in meta.items() if "Last Refreshed" in k),
        None
    )

    # ----------------------------------------------------
    # 3. Parse API into rows
    if source_type == "stocks":
        parser = lambda date, values: parse_stocks_row(symbol, date, values)
        inserter = insert_stocks_rows
        
    elif source_type == "fx":
        parser = lambda date, values: parse_fx_row(symbol, market, date, values)
        inserter = insert_fx_rows

    elif source_type == "crypto":
        parser = lambda date, values: parse_crypto_row(symbol, market, date, values)
        inserter = insert_crypto_rows

    elif source_type == "commodity":
        parser = lambda date, values: parse_commodity_row(symbol, date, values)
        inserter = insert_commodities_rows

        # Upsert metadata into the commodity lookup table
        upsert_commodity_lookup(
            conn,
            symbol,
            meta.get("name"),
            meta.get("interval"),
            meta.get("unit")
        )

    else:
        conn.close()
        raise ValueError(f"Invalid source_type: {source_type}")

    # ----------------------------------------------------
    # 4. Filter → sort → parse

    filtered_rows = filter_and_sort(series, max_data_date, full_load)

    new_rows = [
        parser(date, values)
        for date, values in filtered_rows
    ]

    if new_rows:
        print(f"Parsed {len(new_rows)} rows.")

    # ----------------------------------------------------
    # 5. Insert into appropriate table
    if new_rows:
        inserter(conn, new_rows)
        print(f"Inserted {len(new_rows)} new rows.")

        max_data_date = extract_date(new_rows[-1], source_type)
    else:
        print("No new rows found.")

    # ----------------------------------------------------
    # 6. Upsert metadata
    if new_rows:
        upsert_metadata(
            conn,
            source_type=source_type,
            symbol=symbol,
            market=market,
            interval=interval,
            max_data_date=max_data_date,
            api_last_refresh=api_last_refresh
        )
        print("Metadata updated.")
    else:
        print("Metadata NOT updated; no new data retrieved.")

    conn.close()

    time.sleep(15)  # throttle calls to avoid rate limits

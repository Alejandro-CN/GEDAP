import sqlite3
import requests
import os
from pathlib import Path
from dotenv import load_dotenv


# load .env from repo root (one level above scripts/)
env_path = Path(__file__).resolve().parents[1] / ".env"
if env_path.exists():
    load_dotenv(env_path)


### General setup for CoinGecko API

# Define the API URL
base_url = "https://api.coingecko.com/api/v3/"

# Getting API key from environment variable
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY")
if not COINGECKO_API_KEY:
    raise RuntimeError("Missing COINGECKO_API_KEY environment variable")

# Define the headers with API key
headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": COINGECKO_API_KEY
}


### coingecko_market_data

# Define endpoint
endpoint = '/coins/markets'

# Define params
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": "false"
}

# Make the GET request
url = base_url + endpoint
response = requests.get(url, headers=headers, params=params)

#parse JSON
parsed_json = response.json()


### Insert into DB

# Path to the database file 
db_path = "./GEDAP_DB.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Prepare the insert query
insert_query = """
INSERT OR IGNORE INTO coingecko_market_data (
    id, symbol, name, image, current_price, market_cap, market_cap_rank,
    fully_diluted_valuation, total_volume, high_24h, low_24h, price_change_24h,
    price_change_percentage_24h, market_cap_change_24h, market_cap_change_percentage_24h,
    circulating_supply, total_supply, max_supply, ath, ath_change_percentage,
    ath_date, atl, atl_change_percentage, atl_date, roi_times, roi_currency,
    roi_percentage, last_updated
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""

# Inserting data
for coin in parsed_json:
    roi = coin.get("roi") or {}
    values = (
        coin.get("id"),
        coin.get("symbol"),
        coin.get("name"),
        coin.get("image"),
        coin.get("current_price"),
        coin.get("market_cap"),
        coin.get("market_cap_rank"),
        coin.get("fully_diluted_valuation"),
        coin.get("total_volume"),
        coin.get("high_24h"),
        coin.get("low_24h"),
        coin.get("price_change_24h"),
        coin.get("price_change_percentage_24h"),
        coin.get("market_cap_change_24h"),
        coin.get("market_cap_change_percentage_24h"),
        coin.get("circulating_supply"),
        coin.get("total_supply"),
        coin.get("max_supply"),
        coin.get("ath"),
        coin.get("ath_change_percentage"),
        coin.get("ath_date"),
        coin.get("atl"),
        coin.get("atl_change_percentage"),
        coin.get("atl_date"),
        roi.get("times"),
        roi.get("currency"),
        roi.get("percentage"),
        coin.get("last_updated")
    )
    cursor.execute(insert_query, values)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data inserted successfully!")
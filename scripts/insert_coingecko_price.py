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


### coingecko_price

# Define endpoint
endpoint = '/simple/price'

# Define params
params = {
    "ids": "bitcoin,ethereum,ripple,tether,dogecoin,solana",  # Cryptocurrency ID
    "vs_currencies": "usd,cad,mxn,gbp",  # Convert price to differnt currencies
    "include_market_cap": "true",
    "include_24hr_vol": "true",
    "include_24hr_change": "true",
    "include_last_updated_at": "true",
    "precision": "2"  # Decimal precision
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

# Inserting data
for crypto, data in parsed_json.items():
    last_updated = data.get("last_updated_at")

    for currency in ["usd", "cad", "mxn", "gbp"]:
        price = data.get(currency)
        market_cap = data.get(f"{currency}_market_cap")
        vol_24h = data.get(f"{currency}_24h_vol")
        change_24h = data.get(f"{currency}_24h_change")

        if price is None or last_updated is None:
            continue  # skip incomplete records

        cursor.execute("""
            INSERT OR REPLACE INTO coingecko_price
            (crypto, currency, price, market_cap, "24h_vol", "24h_change", last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (crypto, currency, price, market_cap, vol_24h, change_24h, last_updated))

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data inserted successfully!")
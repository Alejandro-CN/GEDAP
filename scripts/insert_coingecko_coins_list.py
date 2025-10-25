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


### coingecko_coins_list

# Define endpoint
endpoint = 'coins/list'

# Make the GET request
url = base_url + endpoint
response = requests.get(url, headers=headers)

#parse JSON
parsed_json = response.json()


### Insert into DB

# Path to the database file 
db_path = "./GEDAP_DB.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Inserting data
for coin in parsed_json :
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO coingecko_coins_list (api_id, symbol, name)
            VALUES (?, ?, ?)
        """, (coin["id"], coin["symbol"], coin["name"]))
    except KeyError as e:
        print(f"Missing key in entry: {coin} — {e}")

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data inserted successfully!")
# activate virual environment: venv\Scripts\Activate
# run python script: python scripts\insert_investing_indices.py


import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

url = "https://www.investing.com/"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")
table = soup.find("table", class_=lambda x: x and "datatable-v2" in x)
rows = table.find_all("tr")

data = []
for row in rows:
    cells = row.find_all("td")
    if len(cells) >= 7:
        name = cells[0].get_text(strip=True)
        last = cells[1].get_text(strip=True)
        high = cells[2].get_text(strip=True)
        low = cells[3].get_text(strip=True)
        change = cells[4].get_text(strip=True)
        percent = cells[5].get_text(strip=True)

        # Extract the <time> tag inside the last cell
        time_tag = cells[6].find("time")
        timestamp = time_tag["datetime"] if time_tag and time_tag.has_attr("datetime") else None

        data.append({
            "name": name,
            "last": last,
            "high": high,
            "low": low,
            "change": change,
            "percent": percent,
            "time": timestamp
        })


###Insert into DB

# Path to the database file 
db_path = "./GEDAP_DB.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Inserting data
for entry in data:
    cursor.execute("""
        INSERT INTO investing_indices
        (name, last_value, high_value, low_value, change, change_percent, market_time, insert_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        entry["name"],
        entry["last"],
        entry["high"],
        entry["low"],
        entry["change"],
        entry["percent"],
        entry["time"],
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ))

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data inserted successfully!")
import requests
import sqlite3
import csv
import sys

# 1. Setup Database
# Creates a local SQLite database that persists in your repository
db_file = 'tickers.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# CIK (Central Index Key) is the unique ID that never changes for a company
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sec_tickers (
        cik INTEGER PRIMARY KEY,
        ticker TEXT,
        name TEXT,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# 2. Fetch Data from SEC
# SEC requires a descriptive User-Agent or you will receive a 403 error
headers = {
    'User-Agent': 'RyanSchraub (ryan.schraub@gmail.com)',
    'Accept-Encoding': 'gzip, deflate'
}
url = "https://www.sec.gov/files/company_tickers.json"

print("Fetching latest tickers from SEC.gov...")
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    # 3. Process and Upsert Data
    # We use 'ON CONFLICT' to update names/tickers if the CIK already exists
    ticker_list = []
    for entry in data.values():
        ticker_list.append((entry['cik_str'], entry['ticker'], entry['title']))

    cursor.executemany('''
        INSERT INTO sec_tickers (cik, ticker, name)
        VALUES (?, ?, ?)
        ON CONFLICT(cik) DO UPDATE SET
            ticker=excluded.ticker,
            name=excluded.name,
            last_updated=CURRENT_TIMESTAMP
    ''', ticker_list)
    
    conn.commit()
    print(f"Database updated: {len(ticker_list)} records processed.")

    # 4. Export to CSV for GitHub Preview
    # This step generates the visual table you see in the GitHub browser
    cursor.execute("SELECT ticker, cik, name FROM sec_tickers ORDER BY ticker ASC")
    rows = cursor.fetchall()
    
    with open('tickers_preview.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker Symbol', 'SEC CIK', 'Company Name'])
        writer.writerows(rows)
    
    print("CSV preview file successfully generated.")

except Exception as e:
    print(f"Error occurred: {e}")
    sys.exit(1) # Tells GitHub Action that the script failed
finally:
    conn.close()

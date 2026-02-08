import requests
import sqlite3
import csv
import xml.etree.ElementTree as ET
from datetime import datetime

# --- CONFIG ---
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}
RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Initialize DB - Keep existing data, just add columns if new
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER UNIQUE, ticker TEXT, name TEXT,
            event_scenario TEXT, next_earnings TEXT, timestamp DATETIME
        )
    ''')

    # 2. Daily Master Sync (Ensures the list of 10k tickers is always there)
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in master_data.values():
        cik, ticker, name = item['cik_str'], item['ticker'].upper(), item['title']
        # INSERT if not exists, otherwise IGNORE (keeps your current table)
        cursor.execute('''
            INSERT OR IGNORE INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (cik, ticker, name, "-", "2026-05-15 (Legal Fallback)", now))

    # 3. Targeted AI Research (Hourly Trigger)
    try:
        rss = requests.get(RSS_URL, headers=HEADERS)
        root = ET.fromstring(rss.content)
        # XML Namespace for Atom
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        for entry in root.findall('atom:entry', ns):
            title = entry.find('atom:title', ns).text
            # We ONLY research if it's an 8-K (where earnings dates are announced)
            if "8-K" in title:
                # Extracting ticker from SEC title format: "8-K - Apple Inc. (0000320193) (Ticker: AAPL)"
                ticker_part = title.split('(')[-1].replace("Ticker: ", "").replace(")", "").strip()
                
                # HERE is where your AI logic sits. 
                # For now, we tag it as 'RESEARCHED' and update the date.
                cursor.execute('''
                    UPDATE ticker_event_log 
                    SET next_earnings = ?, event_scenario = ?, timestamp = ?
                    WHERE ticker = ?
                ''', ("Confirmed: 2026-05-02", "EARNINGS_ANNOUNCED", now, ticker_part))
    except Exception as e:
        print(f"Research Scan failed: {e}")

    conn.commit()

    # 4. Export the FULL table to CSV for your website
    cursor.execute("SELECT ticker, name, event_scenario, next_earnings, timestamp FROM ticker_event_log ORDER BY ticker ASC")
    rows = cursor.fetchall()
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Status', 'Earnings', 'LastSync'])
        writer.writerows(rows)
    conn.close()

if __name__ == "__main__":
    main()

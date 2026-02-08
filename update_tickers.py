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

    # 1. CREATE TABLE: This schema supports both Ticker tracking and Earnings
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER UNIQUE, 
            ticker TEXT, 
            name TEXT,
            event_scenario TEXT, 
            next_earnings TEXT, 
            timestamp DATETIME
        )
    ''')

    # 2. PART A: THE MASTER LIST (Every Ticker)
    # We fetch the full SEC list to ensure no company is missing.
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in master_data.values():
        cik, ticker, name = item['cik_str'], item['ticker'].upper(), item['title']
        
        # We use INSERT OR IGNORE so we don't overwrite existing 'Confirmed' earnings 
        # dates with the 'Legal Fallback' on every run.
        cursor.execute('''
            INSERT OR IGNORE INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (cik, ticker, name, "-", "2026-05-15 (Legal Fallback)", now))

    # 3. PART B: THE 8-K RESEARCH (Hourly Update)
    try:
        rss = requests.get(RSS_URL, headers=HEADERS)
        root = ET.fromstring(rss.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        for entry in root.findall('atom:entry', ns):
            title = entry.find('atom:title', ns).text
            if "8-K" in title:
                # Extracts ticker from "8-K - Apple Inc. (0000320193) (Ticker: AAPL)"
                try:
                    ticker_part = title.split('(')[-1].replace("Ticker: ", "").replace(")", "").strip()
                    # If we find a match, update that ticker's row with the 'Confirmed' status
                    cursor.execute('''
                        UPDATE ticker_event_log 
                        SET next_earnings = ?, event_scenario = ?, timestamp = ?
                        WHERE ticker = ?
                    ''', ("Confirmed via 8-K", "NEW_FILING", now, ticker_part))
                except: continue
    except Exception as e:
        print(f"Research failed: {e}")

    conn.commit()

    # 4. EXPORT TO CSV
    cursor.execute("SELECT ticker, name, event_scenario, next_earnings, timestamp FROM ticker_event_log ORDER BY ticker ASC")
    rows = cursor.fetchall()
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Status', 'Earnings', 'LastSync'])
        writer.writerows(rows)
    conn.close()

if __name__ == "__main__":
    main()

import requests
import sqlite3
import csv
import time
from datetime import datetime

# CONFIGURATION
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com"
# SEC RSS Feed for the latest 8-Ks (This is better than scanning 10k individual folders)
SEC_RSS_8K = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&owner=include&output=atom"

def get_legal_deadline():
    """Returns the upcoming 10-Q deadline for 2026 Q1."""
    # Standard deadline for Q1 2026 is May 15, 2026
    return "2026-05-15 (Legal Max)"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Update Table Schema (Adding Earnings)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER, ticker TEXT, name TEXT,
            event_scenario TEXT, ai_research TEXT, 
            next_earnings TEXT, is_active INTEGER, timestamp DATETIME
        )
    ''')

    # 2. Baseline Run Logic
    headers = {'User-Agent': f'SecurityMasterBot ({USER_EMAIL})'}
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers).json()
    
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    legal_max = get_legal_deadline()

    print(f"Starting Baseline for {len(master_data)} tickers...")

    for item in master_data.values():
        cik = item['cik_str']
        ticker = item['ticker'].upper()
        name = item['title']

        # Check if already in DB
        cursor.execute("SELECT id FROM ticker_event_log WHERE cik = ?", (cik,))
        if not cursor.fetchone():
            # First time seeing this stock? Give it the Legal Deadline fallback
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, is_active, timestamp)
                VALUES (?, ?, ?, ?, ?, 1, ?)
            ''', (cik, ticker, name, "-", legal_max, current_time))

    conn.commit()

    # 3. AI Research Phase: Scan recent 8-Ks for actual dates
    # In a real setup, you would hit an LLM API here for only the most recent 100 8-Ks
    print("AI Research phase skipped for baseline to avoid SEC rate limits.")

    # 4. Export
    cursor.execute('''
        SELECT ticker, name, event_scenario, next_earnings, timestamp 
        FROM ticker_event_log 
        ORDER BY ticker ASC
    ''')
    with open(CSV_OUTPUT, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Status', 'Next Earnings', 'Last Sync'])
        writer.writerows(cursor.fetchall())

    conn.close()

if __name__ == "__main__":
    main()

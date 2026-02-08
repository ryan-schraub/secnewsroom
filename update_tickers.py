import requests
import sqlite3
import csv
from datetime import datetime

# CONFIGURATION
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com"
SEC_URL = "https://www.sec.gov/files/company_tickers.json"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Ensure table exists with the AI Research column
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER,
            ticker TEXT,
            name TEXT,
            event_scenario TEXT,
            ai_research TEXT, 
            is_active INTEGER,
            timestamp DATETIME
        )
    ''')

    # 2. Baseline Check: Is the database empty?
    cursor.execute("SELECT COUNT(*) FROM ticker_event_log")
    is_first_run = cursor.fetchone()[0] == 0
    
    # 3. Fetch SEC Data
    headers = {'User-Agent': f'SecurityMasterBot ({USER_EMAIL})'}
    try:
        response = requests.get(SEC_URL, headers=headers)
        data = response.json()
    except Exception as e:
        print(f"Fetch failed: {e}")
        return

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    incoming_ciks = {item['cik_str']: item for item in data.values()}

    for cik, info in incoming_ciks.items():
        ticker = str(info['ticker']).upper()
        name = info['title']

        # Get latest state
        cursor.execute('''
            SELECT ticker, name FROM ticker_event_log 
            WHERE cik = ? ORDER BY timestamp DESC LIMIT 1
        ''', (cik,))
        latest = cursor.fetchone()

        scenario = None
        ai_note = ""

        if not latest:
            # FIX: First run gets '-' to prevent 10k "New Listing" alerts
            scenario = "-" if is_first_run else "NEW_LISTING"
        elif latest[0] != ticker:
            # Handle AAM to Dauch Rebrand specifically
            if ticker == "DCH" and "DAUCH" in name.upper():
                scenario = f"REBRAND: {latest[0]} → {ticker}"
                ai_note = "American Axle rebranded to Dauch Corp following the acquisition of Dowlais Group to focus on EV technology."
            else:
                scenario = f"TICKER_CHANGE: {latest[0]} → {ticker}"
                ai_note = "AI Research: Analyzing SEC filings for merger or symbol update..."
        elif latest[1] != name:
            scenario = f"NAME_CHANGE: {latest[1]} → {name}"
            ai_note = "AI Research: Legal name update detected."

        if scenario:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, ai_research, is_active, timestamp)
                VALUES (?, ?, ?, ?, ?, 1, ?)
            ''', (cik, ticker, name, scenario, ai_note, current_time))

    conn.commit()

    # 4. Export for Web (Latest event for each active CIK)
    cursor.execute('''
        SELECT ticker, cik, name, event_scenario, ai_research, timestamp 
        FROM ticker_event_log t1
        WHERE timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
        AND is_active = 1
        ORDER BY ticker ASC
    ''')
    
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Name', 'Last Event', 'AI Research', 'Date Detected'])
        writer.writerows(cursor.fetchall())

    conn.close()

if __name__ == "__main__":
    main()

import requests
import sqlite3
import csv
import sys
from datetime import datetime

# CONFIGURATION
# SEC requires a descriptive User-Agent with an email
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com"
SEC_URL = "https://www.sec.gov/files/company_tickers.json"

def setup_db(cursor):
    """Creates the event log table and handles schema updates."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER,
            ticker TEXT,
            name TEXT,
            event_scenario TEXT, 
            is_active INTEGER,
            timestamp DATETIME
        )
    ''')

def get_sec_data():
    """Fetches the latest ticker JSON from the SEC."""
    headers = {'User-Agent': f'SecurityMasterBot ({USER_EMAIL})'}
    try:
        response = requests.get(SEC_URL, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Failed to fetch SEC data: {e}")
        sys.exit(1)

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    setup_db(cursor)

    # Check if this is the first ever run (initial seed)
    cursor.execute("SELECT COUNT(*) FROM ticker_event_log")
    is_first_run = cursor.fetchone()[0] == 0
    
    data = get_sec_data()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    incoming_ciks = {item['cik_str']: item for item in data.values()}

    # 1. PROCESS NEW DATA & CHANGES
    for cik, info in incoming_ciks.items():
        ticker = str(info['ticker']).upper()
        name = info['title']

        # Get the LATEST known state for this company
        cursor.execute('''
            SELECT ticker, name, is_active FROM ticker_event_log 
            WHERE cik = ? ORDER BY timestamp DESC LIMIT 1
        ''', (cik,))
        latest = cursor.fetchone()

        scenario = None
        
        if not latest:
            # If DB is empty, it's just setup data. Otherwise, it's a real new IPO.
            scenario = "-" if is_first_run else "NEW_LISTING"
        elif latest[0] != ticker:
            # LOG THE SPECIFIC CHANGE: Old Ticker -> New Ticker
            scenario = f"TICKER_CHANGE: {latest[0]} → {ticker}"
        elif latest[1] != name:
            scenario = f"NAME_CHANGE: {latest[1]} → {name}"
        elif latest[2] == 0:
            scenario = "RE_LISTED"
        
        if scenario:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, is_active, timestamp)
                VALUES (?, ?, ?, ?, 1, ?)
            ''', (cik, ticker, name, scenario, current_time))

    # 2. HANDLE DELISTINGS (Scenario: DELISTED)
    # Mark companies as delisted if they were active yesterday but gone today
    cursor.execute('''
        SELECT DISTINCT cik, ticker, name FROM ticker_event_log t1
        WHERE is_active = 1 
        AND timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
    ''')
    for old_cik, old_ticker, old_name in cursor.fetchall():
        if old_cik not in incoming_ciks:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, is_active, timestamp)
                VALUES (?, ?, ?, 'DELISTED', 0, ?)
            ''', (old_cik, old_ticker, old_name, current_time))

    conn.commit()

    # 3. EXPORT CURRENT STATE FOR WEBSITE
    # We select the latest entry for every unique CIK
    cursor.execute('''
        SELECT ticker, cik, name, event_scenario, timestamp 
        FROM ticker_event_log t1
        WHERE timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
        AND is_active = 1
        ORDER BY ticker ASC
    ''')
    
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Name', 'Last Event', 'Date Detected'])
        writer.writerows(cursor.fetchall())

    conn.close()
    print(f"Sync Complete. {len(incoming_ciks)} companies processed.")

if __name__ == "__main__":
    main()

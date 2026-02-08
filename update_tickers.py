import requests
import sqlite3
import csv
import xml.etree.ElementTree as ET
from datetime import datetime

# --- CONFIGURATION ---
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com" 
SEC_HEADERS = {'User-Agent': f'SecurityMasterBot ({USER_EMAIL})'}
RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom"

def migrate_db(cursor):
    """Safely adds the next_earnings column if it's missing."""
    cursor.execute("PRAGMA table_info(ticker_event_log)")
    columns = [column[1] for column in cursor.fetchall()]
    if 'next_earnings' not in columns:
        print("Migrating database: Adding 'next_earnings' column...")
        cursor.execute("ALTER TABLE ticker_event_log ADD COLUMN next_earnings TEXT DEFAULT '-'")

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Setup Table & Migrate
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER, ticker TEXT, name TEXT,
            event_scenario TEXT, ai_research TEXT, 
            next_earnings TEXT, timestamp DATETIME
        )
    ''')
    migrate_db(cursor)

    # 2. Daily Ticker Sync (Compare SEC Master to Local DB)
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_HEADERS).json()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in master_data.values():
        cik, ticker, name = item['cik_str'], item['ticker'].upper(), item['title']
        cursor.execute("SELECT ticker FROM ticker_event_log WHERE cik=? ORDER BY timestamp DESC LIMIT 1", (cik,))
        last = cursor.fetchone()
        
        if not last:
            cursor.execute("INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, timestamp) VALUES (?,?,?,?,?,?)",
                           (cik, ticker, name, "-", "2026-05-15 (Legal Max)", current_time))
        elif last[0] != ticker:
            cursor.execute("INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, timestamp) VALUES (?,?,?,?,?,?)",
                           (cik, ticker, name, f"CHANGE: {last[0]}→{ticker}", "Pending AI Scan...", current_time))

    # 3. Hourly RSS Scan (Find 8-Ks and update earnings)
    try:
        rss_resp = requests.get(RSS_URL, headers=SEC_HEADERS)
        root = ET.fromstring(rss_resp.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        for entry in root.findall('atom:entry', ns):
            title = entry.find('atom:title', ns).text
            if "8-K" in title:
                # Extract Ticker from title "8-K - COMPANY NAME (0001234567) (Ticker: XYZ)"
                ticker_part = title.split('(')[-1].split(')')[0].replace("Ticker: ", "").strip()
                if ticker_part:
                    # Logic: Mark as 'Confirmed' if we see a fresh 8-K
                    cursor.execute("UPDATE ticker_event_log SET next_earnings = ? WHERE ticker = ?", 
                                   ("Confirmed via 8-K", ticker_part))
    except Exception as e:
        print(f"RSS Pulse check failed: {e}")

    conn.commit()

    # 4. Generate Web Preview (Grouping by CIK to show only the latest status)
    cursor.execute('''
        SELECT ticker, name, event_scenario, next_earnings, timestamp 
        FROM ticker_event_log t1
        WHERE timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
        ORDER BY ticker ASC
    ''')
    
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Status', 'Earnings', 'Last Sync'])
        writer.writerows(cursor.fetchall())
    conn.close()

if __name__ == "__main__":
    main()

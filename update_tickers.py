import requests
import sqlite3
import csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# --- CONFIGURATION ---
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com"  # Replace with your actual email
SEC_HEADERS = {'User-Agent': f'SecurityMasterBot ({USER_EMAIL})'}
RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom"

def get_legal_deadline():
    """Returns the Q1 2026 legal filing deadline as a fallback."""
    return "2026-05-15 (Legal Max)"

def ai_extract_earnings_date(filing_url):
    """
    Placeholder for AI Extraction.
    In a real scenario, you would use requests.get(filing_url) to get the text
    and send it to GPT-4 or Gemini to find the date string.
    """
    # For now, we simulate finding a date in an 8-K
    return "2026-05-02 (Confirmed via 8-K)"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Initialize Schema
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER, ticker TEXT, name TEXT,
            event_scenario TEXT, ai_research TEXT, 
            next_earnings TEXT, timestamp DATETIME
        )
    ''')

    # 2. Daily Master Sync (Ticker changes)
    master_url = "https://www.sec.gov/files/company_tickers.json"
    master_data = requests.get(master_url, headers=SEC_HEADERS).json()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in master_data.values():
        cik, ticker, name = item['cik_str'], item['ticker'].upper(), item['title']
        cursor.execute("SELECT ticker FROM ticker_event_log WHERE cik=? ORDER BY timestamp DESC LIMIT 1", (cik,))
        last = cursor.fetchone()
        
        if not last: # Baseline entry
            cursor.execute("INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, timestamp) VALUES (?,?,?,?,?,?)",
                           (cik, ticker, name, "-", get_legal_deadline(), current_time))
        elif last[0] != ticker: # Ticker change detected
            cursor.execute("INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, timestamp) VALUES (?,?,?,?,?,?)",
                           (cik, ticker, name, f"CHANGE: {last[0]}→{ticker}", "Re-scanning...", current_time))

    # 3. RSS Scan (Hourly logic for 8-Ks)
    try:
        rss_resp = requests.get(RSS_URL, headers=SEC_HEADERS)
        root = ET.fromstring(rss_resp.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        for entry in root.findall('atom:entry', ns):
            title = entry.find('atom:title', ns).text
            link = entry.find('atom:link', ns).attrib['href']
            # If 8-K is found, trigger AI research for that specific CIK
            if "8-K" in title:
                ticker_part = title.split('(')[-1].split(')')[0] if '(' in title else None
                if ticker_part:
                    confirmed_date = ai_extract_earnings_date(link)
                    cursor.execute("UPDATE ticker_event_log SET next_earnings = ? WHERE ticker = ?", (confirmed_date, ticker_part))
    except Exception as e:
        print(f"RSS Scan skipped: {e}")

    conn.commit()

    # 4. Export CSV for Web
    cursor.execute("SELECT ticker, cik, name, event_scenario, next_earnings, timestamp FROM ticker_event_log GROUP BY cik HAVING MAX(timestamp) ORDER BY ticker ASC")
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Name', 'Status', 'Next Earnings', 'Last Sync'])
        writer.writerows(cursor.fetchall())
    conn.close()

if __name__ == "__main__":
    main()

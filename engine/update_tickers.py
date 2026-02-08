import requests
import sqlite3
import csv
import time
import sys
import os
from datetime import datetime

# --- AUTOMATIC PATH ROUTING ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'tickers.db')
CSV_OUTPUT = os.path.join(BASE_DIR, 'tickers_preview.csv')

USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}

def main():
    print(f"[{datetime.now()}] Initializing SEC Sync with Metadata...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. ENHANCED DATABASE SETUP
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER UNIQUE, 
            ticker TEXT, 
            name TEXT,
            sic TEXT,
            industry TEXT,
            location TEXT,
            incorporated TEXT,
            fye TEXT,
            last_10k TEXT, link_10k TEXT,
            last_10q TEXT, link_10q TEXT,
            timestamp DATETIME
        )
    ''')
    
    # Migration: Ensure new columns exist if DB was already created
    cursor.execute("PRAGMA table_info(ticker_event_log)")
    cols = [info[1] for info in cursor.fetchall()]
    new_cols = ['sic', 'industry', 'location', 'incorporated', 'fye']
    for col in new_cols:
        if col not in cols:
            cursor.execute(f'ALTER TABLE ticker_event_log ADD COLUMN {col} TEXT DEFAULT "N/A"')

    # 2. FETCH MASTER LIST
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    tickers = list(master_data.values())

    # 3. SYNC LOOP
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for i, item in enumerate(tickers):
        cik_str = str(item['cik_str']).zfill(10)
        ticker = str(item['ticker']).upper()
        
        if i % 100 == 0:
            sys.stdout.write(f"\rProcessing: {i}/{len(tickers)} ({ticker})")
            sys.stdout.flush()

        try:
            time.sleep(0.11) 
            resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik_str}.json", headers=HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                
                # EXTRACTING EDGAR METADATA
                sic = data.get('sic', 'N/A')
                industry = data.get('sicDescription', 'N/A')
                incorp = data.get('stateOfIncorporation', 'N/A')
                fye = data.get('fiscalYearEnd', 'N/A')
                
                # Format Location: "City, State"
                biz = data.get('addresses', {}).get('business', {})
                location = f"{biz.get('city', '')}, {biz.get('stateProvince', '')}".strip(", ")

                # Filing Logic
                recent = data.get('filings', {}).get('recent', {})
                k_date, k_link, q_date, q_link = "N/A", "", "N/A", ""
                for j, form in enumerate(recent.get('form', [])):
                    acc = recent['accessionNumber'][j].replace('-', '')
                    doc = recent['primaryDocument'][j]
                    link = f"https://www.sec.gov/Archives/edgar/data/{int(cik_str)}/{acc}/{doc}"
                    if form == '10-K' and k_date == "N/A": k_date, k_link = recent['reportDate'][j], link
                    if form == '10-Q' and q_date == "N/A": q_date, q_link = recent['reportDate'][j], link
                    if k_date != "N/A" and q_date != "N/A": break
                
                cursor.execute('''
                    INSERT INTO ticker_event_log (cik, ticker, name, sic, industry, location, incorporated, fye, last_10k, link_10k, last_10q, link_10q, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cik) DO UPDATE SET
                        ticker=excluded.ticker, name=excluded.name, sic=excluded.sic, industry=excluded.industry,
                        location=excluded.location, incorporated=excluded.incorporated, fye=excluded.fye,
                        last_10k=excluded.last_10k, link_10k=excluded.link_10k, 
                        last_10q=excluded.last_10q, link_10q=excluded.link_10q, timestamp=excluded.timestamp
                ''', (int(cik_str), ticker, item['title'], sic, industry, location, incorp, fye, k_date, k_link, q_date, q_link, now))
            
            if i % 500 == 0: conn.commit()
        except Exception: continue

    conn.commit()
    
    # 4. EXPORT (Including the new "EDGAR" columns)
    cursor.execute("SELECT ticker, name, location, incorporated, fye, industry, last_10k, link_10k, last_10q, link_10q FROM ticker_event_log ORDER BY ticker ASC")
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Location', 'Inc', 'FYE', 'Industry', '10K_Date', '10K_Link', '10Q_Date', '10Q_Link'])
        writer.writerows(cursor.fetchall())
    conn.close()

if __name__ == "__main__": main()

import requests
import sqlite3
import csv
import time
import os
from datetime import datetime

# --- PATHS ---
# Ensures files are saved in the root directory relative to the 'engine' folder
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'tickers.db')
CSV_OUTPUT = os.path.join(BASE_DIR, 'tickers_preview.csv')

USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}

def main():
    print(f"[{datetime.now()}] Initializing SEC Intelligence Engine...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. DATABASE SETUP
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            cik INTEGER UNIQUE, 
            ticker TEXT, 
            name TEXT, 
            industry TEXT,
            location TEXT, 
            last_10k TEXT, 
            link_10k TEXT, 
            revenue REAL, 
            timestamp DATETIME
        )
    ''')
    
    # Ensure revenue column exists for older database versions
    try:
        cursor.execute("ALTER TABLE ticker_event_log ADD COLUMN revenue REAL")
    except sqlite3.OperationalError:
        pass

    # 2. FETCH TICKER LIST
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    tickers = list(master_data.values())

    print(f"Total Tickers to process: {len(tickers)}")

    for i, item in enumerate(tickers):
        cik_str = str(item['cik_str']).zfill(10)
        ticker = str(item['ticker']).upper()
        
        # Rate limit compliance
        time.sleep(0.12) 

        try:
            # --- CALL 1: SUBMISSIONS (Metadata & Links) ---
            sub_resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik_str}.json", headers=HEADERS).json()
            
            # --- CALL 2: COMPANY FACTS (Revenue Data) ---
            facts_resp = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json", headers=HEADERS).json()
            
            # REVENUE EXTRACTION WATERFALL
            revenue = 0
            tags = [
                ('us-gaap', 'RevenueFromContractWithCustomerExcludingAssessedTax'),
                ('us-gaap', 'SalesRevenueNet'),
                ('us-gaap', 'Revenues'),
                ('ifrs-full', 'RevenueFromContractsWithCustomers')
            ]

            for namespace, tag in tags:
                try:
                    units = facts_resp['facts'][namespace][tag]['units']
                    currency = 'USD' if 'USD' in units else list(units.keys())[0]
                    points = units[currency]
                    
                    # Filter for Annual Data (FY) for accuracy
                    annual_points = [p for p in points if p.get('fp') == 'FY']
                    if annual_points:
                        revenue = sorted(annual_points, key=lambda x: x['end'])[-1]['val']
                    else:
                        revenue = sorted(points, key=lambda x: x['end'])[-1]['val']
                    
                    if revenue: break 
                except (KeyError, IndexError):
                    continue

            # METADATA EXTRACTION
            industry = sub_resp.get('sicDescription', 'N/A')
            biz = sub_resp.get('addresses', {}).get('business', {})
            location = f"{biz.get('city', '')}, {biz.get('stateProvince', '')}".strip(", ")
            
            recent = sub_resp.get('filings', {}).get('recent', {})
            k_date, k_link = "N/A", ""
            
            if '10-K' in recent.get('form', []):
                idx = recent['form'].index('10-K')
                acc = recent['accessionNumber'][idx].replace('-', '')
                doc = recent['primaryDocument'][idx]
                k_date = recent['reportDate'][idx]
                k_link = f"https://www.sec.gov/Archives/edgar/data/{int(cik_str)}/{acc}/{doc}"

            # 3. SAVE TO DB
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, industry, location, last_10k, link_10k, revenue, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cik) DO UPDATE SET
                    ticker=excluded.ticker, 
                    industry=excluded.industry, 
                    revenue=excluded.revenue,
                    last_10k=excluded.last_10k, 
                    link_10k=excluded.link_10k, 
                    timestamp=excluded.timestamp
            ''', (int(cik_str), ticker, item['title'], industry, location, k_date, k_link, revenue, datetime.now()))
            
            if i % 100 == 0:
                print(f"Processed {i}/{len(tickers)}: {ticker} | Revenue: ${revenue:,.0f}")
                conn.commit()

        except Exception:
            continue

    # 4. EXPORT TO CSV (Corrected for Website Mapping)
    print("\nGenerating CSV for frontend...")
    # Explicitly selecting lowercase column names to match table schema
    cursor.execute("""
        SELECT ticker, name, location, industry, revenue, last_10k, link_10k 
        FROM ticker_event_log 
        ORDER BY revenue DESC
    """)
    
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # These headers match the 'data' keys in your index.html
        writer.writerow(['Ticker', 'Company', 'Location', 'Industry', 'Revenue', '10K_Date', '10K_Link'])
        writer.writerows(cursor.fetchall())
    
    conn.commit()
    conn.close()
    print(f"[{datetime.now()}] Engine Finished. Revenue and metadata are now synced.")

if __name__ == "__main__":
    main()

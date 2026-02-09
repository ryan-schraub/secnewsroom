import sqlite3
import requests
import time

# Use the same headers as your main sync to avoid getting blocked
HEADERS = {'User-Agent': "YourName your@email.com"}
DB_PATH = 'tickers.db'

# The priority list for revenue tags
TAG_WATERFALL = [
    ('us-gaap', 'Revenues'),
    ('us-gaap', 'SalesRevenueNet'),
    ('ifrs-full', 'RevenueFromContractsWithCustomers'), # International
    ('us-gaap', 'SalesRevenueGoodsNet'),
    ('us-gaap', 'InterestAndDividendIncomeOperating') # Banks
]

def get_revenue_from_facts(cik):
    # SEC CIKs must be 10 digits padded with zeros
    padded_cik = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded_cik}.json"
    
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200: return None
        data = response.json()
        
        # Try each tag in our waterfall
        for namespace, tag in TAG_WATERFALL:
            try:
                # Look for the tag in the JSON
                points = data['facts'][namespace][tag]['units']['USD']
                # Get the most recent data point (latest 'end' date)
                latest = sorted(points, key=lambda x: x['end'], reverse=True)[0]
                return latest['val']
            except KeyError:
                continue
    except Exception as e:
        print(f"Error CIK {cik}: {e}")
    return None

def run_layer():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Add revenue column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE ticker_event_log ADD COLUMN revenue REAL")
    except sqlite3.OperationalError:
        pass # Already exists

    # Fetch all tickers to update
    cursor.execute("SELECT ticker, cik FROM ticker_event_log")
    rows = cursor.fetchall()
    
    print(f"Enriching {len(rows)} tickers with Revenue data...")
    
    for ticker, cik in rows:
        rev = get_revenue_from_facts(cik)
        if rev:
            cursor.execute("UPDATE ticker_event_log SET revenue = ? WHERE ticker = ?", (rev, ticker))
            print(f"✓ {ticker}: ${rev:,.0f}")
        
        # RESPECT THE SEC LIMIT (10 requests per second)
        time.sleep(0.1) 
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_layer()

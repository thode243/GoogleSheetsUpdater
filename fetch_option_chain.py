import requests
import pandas as pd
import gspread
import json
import time
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== CONFIG ==================
NSE_API_URL = "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry=21-Aug-2025"
SHEET_ID = os.getenv("SHEET_ID")  # Google Sheet ID from environment variable
SHEET_NAME = "Sheet1"  # Tab name in your sheet
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))  # Update interval

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/option-chain",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive"
}

# ================== FUNCTIONS ==================
def fetch_option_chain():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Step 1: Get cookies
    session.get("https://www.nseindia.com/", headers=HEADERS, timeout=10)
    session.get("https://www.nseindia.com/option-chain", headers=HEADERS, timeout=10)

    # Step 2: API call
    resp = session.get(NSE_API_URL, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    option_data = data.get("records", {}).get("data", [])
    rows = []

    for entry in option_data:
        strike = entry.get("strikePrice")
        expiry = entry.get("expiryDate")
        ce = entry.get("CE", {})
        pe = entry.get("PE", {})

        rows.append({
            "CE OI": ce.get("openInterest"),
            "CE Chng OI": ce.get("changeinOpenInterest"),
            "CE Volume": ce.get("totalTradedVolume"),
            "CE IV": ce.get("impliedVolatility"),
            "CE LTP": ce.get("lastPrice"),
            "CE Chng LTP": ce.get("change"),
            "CE Bid Qty": ce.get("bidQty"),
            "CE Bid Price": ce.get("bidprice"),
            "CE Ask Price": ce.get("askPrice"),
            "CE Ask Qty": ce.get("askQty"),
            "Strike Price": strike,
            "Expiry Date": expiry,
            "PE Bid Qty": pe.get("bidQty"),
            "PE Bid Price": pe.get("bidprice"),
            "PE Ask Price": pe.get("askPrice"),
            "PE Ask Qty": pe.get("askQty"),
            "PE Chng LTP": pe.get("change"),
            "PE LTP": pe.get("lastPrice"),
            "PE IV": pe.get("impliedVolatility"),
            "PE Volume": pe.get("totalTradedVolume"),
            "PE Chng OI": pe.get("changeinOpenInterest"),
            "PE OI": pe.get("openInterest")
        })

    return pd.DataFrame(rows)

def update_google_sheet(df):
    credentials_json = os.getenv("GOOGLE_CREDENTIALS")
    if not credentials_json:
        raise Exception("Google credentials not found in environment variables")
    
    creds_dict = json.loads(credentials_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    sheet.clear()
    
    data = [df.columns.values.tolist()] + df.values.tolist()
    sheet.update("A1", data)
    
    print(f"✅ Updated Google Sheet at {datetime.now()} - Rows: {len(df)}")

def is_market_open():
    now = datetime.now().time()
    market_start = datetime.strptime("09:15", "%H:%M").time()
    market_end = datetime.strptime("18:30", "%H:%M").time()
    return market_start <= now <= market_end

# ================== MAIN LOOP ==================
def main():
    while True:
        try:
            if is_market_open():
                df = fetch_option_chain()
                update_google_sheet(df)
            else:
                print("⏸ Market closed, skipping update...")
            print(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds...\n")
            time.sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            print(f"Retrying after {POLLING_INTERVAL_SECONDS} seconds...\n")
            time.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

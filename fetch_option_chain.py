import requests
import pandas as pd
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time as dt_time
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Google Sheets setup
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = "Sheet1"
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))

# NSE API details
EXPIRY = "21-Aug-2025"
NSE_API_URL = f"https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry={EXPIRY}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/option-chain",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive"
}

def is_market_open():
    now = datetime.now().time()
    market_open = dt_time(9, 10)
    market_close = dt_time(18, 35)
    return market_open <= now <= market_close

def fetch_option_chain():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Warm up cookies
    session.get("https://www.nseindia.com/", headers=HEADERS, timeout=10)
    session.get("https://www.nseindia.com/option-chain", headers=HEADERS, timeout=10)

    # Call API
    resp = session.get(NSE_API_URL, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    option_data = data.get("records", {}).get("data", [])
    if not option_data:
        raise Exception("No option chain data found for this expiry")

    rows = []
    for entry in option_data:
        strike = entry.get("strikePrice")
        ce = entry.get("CE", {})
        pe = entry.get("PE", {})
        rows.append({
            "Strike Price": strike,
            "Expiry Date": EXPIRY,
            "CE OI": ce.get("openInterest"),
            "CE LTP": ce.get("lastPrice"),
            "PE OI": pe.get("openInterest"),
            "PE LTP": pe.get("lastPrice"),
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
    
    print(f"✅ Updated Google Sheet at {datetime.now()}")

def main():
    while True:
        try:
            if is_market_open():
                df = fetch_option_chain()
                update_google_sheet(df)
            else:
                print("⏸ Market closed, skipping update...")
            print(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds...")
            time.sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            print(f"Retrying after {POLLING_INTERVAL_SECONDS} seconds...")
            time.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    df = fetch_option_chain()
    print(df.head())

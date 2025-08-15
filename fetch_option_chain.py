import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time as dt_time
import os
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# NSE Option Chain API
NSE_API_URL = "https://www.nseindia.com/api/option-chain-contract-info?symbol=NIFTY"

# Headers for NSE API
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/option-chain",
}

# Google Sheets setup
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = "Sheet1"
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))  # default 30 sec


def is_market_open():
    """Check if current time is within NSE market hours (9:10 AM to 3:35 PM IST)."""
    now = datetime.now().time()
    return dt_time(9, 10) <= now <= dt_time(15, 35)


def fetch_option_chain():
    """Fetch NIFTY option chain from NSE JSON API with proper session cookies."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Step 1: Get cookies from homepage
    homepage_url = "https://www.nseindia.com/"
    session.get(homepage_url, headers=headers, timeout=10)

    # Step 2: Call the API with same session
    resp = session.get(NSE_API_URL, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    option_data = data.get("records", {}).get("data", [])
    if not option_data:
        raise Exception("No option chain data found in API response")

    rows = []
    for entry in option_data:
        strike_price = entry.get("strikePrice")
        expiry_date = entry.get("expiryDate")
        ce_data = entry.get("CE", {})
        pe_data = entry.get("PE", {})

        rows.append({
            "Strike Price": strike_price,
            "Expiry Date": expiry_date,
            "CE OI": ce_data.get("openInterest"),
            "CE LTP": ce_data.get("lastPrice"),
            "PE OI": pe_data.get("openInterest"),
            "PE LTP": pe_data.get("lastPrice"),
        })

    df = pd.DataFrame(rows)
    return df



def update_google_sheet(df):
    """Update Google Sheet with option chain data."""
    if os.getenv("GOOGLE_CREDENTIALS"):
        creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    else:
        with open("service_account.json") as f:
            creds_dict = json.load(f)

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
    main()

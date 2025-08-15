import requests
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time as dt_time
import os
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import StringIO

# Base URL for NIFTY option chain
URL = "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-08-21"

# Google Sheets setup
SHEET_ID = os.getenv("SHEET_ID")  # Env variable in GitHub Actions
SHEET_NAME = "Sheet1"
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))  # Default: 30 sec


def is_market_open():
    """Check if current time is within NSE market hours (9:10 AM to 3:35 PM IST)."""
    now = datetime.now().time()
    market_open = dt_time(9, 10)
    market_close = dt_time(15, 35)
    return market_open <= now <= market_close


def get_latest_expiry_url():
    """Fetch the nearest expiry date URL from Moneycontrol."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.moneycontrol.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }

    resp = session.get(URL, headers=headers)  # using URL instead of undefined BASE_URL
    if resp.status_code != 200:
        raise Exception(f"Failed to load base page: {resp}")

    soup = BeautifulSoup(resp.text, "html.parser")
    options = soup.select("select#fno_expiry option")
    if not options:
        raise Exception("No expiry dates found on Moneycontrol")

    nearest_expiry = options[0]["value"].strip()
    expiry_url = f"{URL}/{nearest_expiry}"
    return expiry_url




def fetch_option_chain():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    response = session.get(URL, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response}")

    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")

    if len(tables) < 2:
        raise Exception("Option chain table not found")

    html_str = StringIO(str(tables[1]))
    df = pd.read_html(html_str, flavor="lxml")[0]
    return df


def update_google_sheet(df):
    """Update Google Sheet with option chain data."""
    # ✅ Try env var first (GitHub Actions), else local file
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

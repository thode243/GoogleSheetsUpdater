import requests
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time as dt_time  # renamed to avoid conflict
import os
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# URL of the NIFTY option chain
URL = "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-08-14"

# Google Sheets setup
SHEET_ID = os.getenv("SHEET_ID")  # Store your sheet ID as env variable in GitHub Actions
SHEET_NAME = "Sheet1"  # Name of the worksheet
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))  # Default to 30 seconds

def is_market_open():
    """Check if current time is within NSE market hours (9:10 AM to 3:35 PM IST)."""
    now = datetime.now().time()
    market_open = dt_time(9, 10)
    market_close = dt_time(23, 35)
    return market_open <= now <= market_close

def fetch_option_chain():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = session.get(URL, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: Status code {response.status_code}")
    
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")
    
    if len(tables) < 2:
        raise Exception("Option chain table not found")
    
    table = tables[1]
    df = pd.read_html(str(table))[0]
    
    return df

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
    
    print(f"Updated Google Sheet at {datetime.now()}")

def main():
    while True:
        try:
            if is_market_open():
                df = fetch_option_chain()
                update_google_sheet(df)
            else:
                print("Market closed, skipping update...")
            print(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds...")
            time.sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            print(f"Error: {str(e)}")
            print(f"Retrying after {POLLING_INTERVAL_SECONDS} seconds...")
            time.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

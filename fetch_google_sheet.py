import requests
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import json

# URL of the NIFTY option chain
URL = "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-08-14"

# Google Sheets setup
SHEET_ID = os.getenv("1vCvyVA_eOFT8nAyLjywo0EgyGtHqynUWCY4_O2VHc_w")  # Store your sheet ID as env variable in GitHub Actions
SHEET_NAME = "Sheet1"  # Name of the worksheet

def fetch_option_chain():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(URL, headers=headers)
    
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
    # Load credentials from environment variable
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
    try:
        df = fetch_option_chain()
        update_google_sheet(df)
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()


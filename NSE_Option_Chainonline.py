import requests
import pandas as pd
import gspread
import os
from time import sleep
from datetime import datetime, date
from datetime import time as dtime
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys
import uuid
import time

#

# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "service_account.json")  # GitHub Actions secret path

SHEET_CONFIG = [
    {"sheet_name": "sheet111", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-09-16"},
    {"sheet_name": "sheet222", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-09-23"},
    {"sheet_name": "sheet333", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-09-30"},
    {"sheet_name": "sheet444", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-10-07"},
    {"sheet_name": "sheet555", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/BANKNIFTY/2025-09-30"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ===== FUNCTIONS =====
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    return session

def fetch_option_chain_html(session, url):
    """Fetch option chain table from Moneycontrol using pandas.read_html"""
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(resp.text)
    if not tables:
        logger.warning(f"No tables found at {url}")
        return pd.DataFrame()
    df = tables[0]  # Option chain is usually the 1st table
    logger.info(f"✅ Fetched {len(df)} rows from {url}")
    return df

def update_google_sheet(sheet_dfs):
    """Update Google Sheets with the fetched DataFrames."""
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)

    for sheet_name, df in sheet_dfs.items():
        if df.empty:
            logger.warning(f"No data for {sheet_name}, skipping...")
            continue
        try:
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="200", cols="30")

            # Convert headers + rows to list for gspread
            worksheet.update([df.columns.astype(str).tolist()] + df.astype(str).values.tolist())
            logger.info(f"Updated {sheet_name} with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to update {sheet_name}: {e}")

def is_market_open():
    """Check if the market is open based on IST time."""
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    current_time = now.time()
    current_date = now.date()
    market_start = dtime(9, 10)   # 9:15 AM IST
    market_end = dtime(15, 35)    # 3:30 PM IST
    
    is_weekday = current_date.weekday() < 5  # Monday (0) to Friday (4)
    is_open = is_weekday and market_start <= current_time <= market_end
    logger.debug(f"Market open check: {is_open} (Time: {current_time}, Date: {current_date}, IST)")
    return is_open

# ===== MAIN =====
if __name__ == "__main__":
    logger.info("Starting Option Chain Updater (Moneycontrol HTML)...")
    session = create_session()

    while True:
        if is_market_open():
            try:
                sheet_dfs = {}
                for cfg in SHEET_CONFIG:
                    df = fetch_option_chain_html(session, cfg["url"])
                    sheet_dfs[cfg["sheet_name"]] = df

                update_google_sheet(sheet_dfs)
                logger.info("✅ All sheets updated successfully from Moneycontrol!")
            except Exception as e:
                logger.error(f"Error during fetch-update cycle: {e}")
        else:
            logger.info("📉 Market closed, skipping fetch.")

        # wait 60 seconds before next fetch
        time.sleep(60)








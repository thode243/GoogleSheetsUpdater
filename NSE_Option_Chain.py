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

# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
SHEET_CONFIG = [
    {"sheet_name": "Sheet9", "index": "NIFTY", "expiry_index": 0},  # First expiry
]
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-f68510b7dbdb.json"
)

BASE_URL = "https://webapi.niftytrader.in/webapi/option/option-chain-data"
OPTION_CHAIN_URL = f"{BASE_URL}?symbol={{index}}&exchange=nse&expiryDate={{expiry}}&atmBelow=0&atmAbove=0"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/127.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.niftytrader.in/",
    "Origin": "https://www.niftytrader.in",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(stream=sys.stdout),
        logging.FileHandler("option_chain_updater.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ===== FUNCTIONS =====
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def get_latest_expiries(session, index, num_expiries=4):
    """Fetch next expiry dates from NiftyTrader API"""
    try:
        url = f"https://webapi.niftytrader.in/webapi/option/option-chain-indices?symbol={index}"
        res = session.get(url, headers=HEADERS, timeout=30)
        res.raise_for_status()
        data = res.json()
        expiries = data.get("records", {}).get("expiryDates", [])
        return expiries[:num_expiries]
    except Exception as e:
        logger.error(f"Expiry fetch failed: {e}")
        return []

def fetch_option_chain(session, index, expiry):
    """Fetch option chain for given index & expiry"""
    url = OPTION_CHAIN_URL.format(index=index, expiry=expiry)
    res = session.get(url, headers=HEADERS, timeout=30)
    res.raise_for_status()
    data = res.json()
    # Convert to DataFrame for easier handling
    ce = pd.DataFrame(data.get("records", {}).get("data", []))
    return ce

def update_google_sheet(df, sheet_name):
    """Push dataframe into Google Sheet"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)

        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        logger.info(f"✅ Updated {sheet_name} with {len(df)} rows")
    except Exception as e:
        logger.error(f"Google Sheet update failed: {e}")

def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    market_start = dtime(9, 10)
    market_end = dtime(15, 35)
    return now.weekday() in [0, 1, 2, 3, 4, 6] and market_start <= now.time() <= market_end
    # return now.weekday() < 7 and market_start <= now.time() <= market_end

# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("🚀 Starting option chain updater...")
    session = create_session()
    while True:
        try:
            if is_market_open():
                for cfg in SHEET_CONFIG:
                    expiries = get_latest_expiries(session, cfg["index"])
                    if not expiries:
                        continue
                    expiry = expiries[cfg["expiry_index"]]
                    df = fetch_option_chain(session, cfg["index"], expiry)
                    if not df.empty:
                        update_google_sheet(df, cfg["sheet_name"])
            else:
                logger.info("⏸ Market closed, skipping...")
            sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            sleep(POLLING_INTERVAL_SECONDS)

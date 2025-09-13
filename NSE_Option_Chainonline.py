import requests
import pandas as pd
import gspread
from time import sleep
from datetime import datetime
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys
import os

# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
CREDENTIALS_PATH = r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-f68510b7dbdb.json"

SHEET_CONFIG = [
    {"sheet_name": "sheet111", "index": "NIFTY", "expiry_index": 0},
    {"sheet_name": "sheet222", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "sheet333", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "sheet444", "index": "NIFTY", "expiry_index": 3},
    {"sheet_name": "sheet555", "index": "BANKNIFTY", "expiry_index": 0},
]

# Hardcoded upcoming expiries
EXPIRIES = {
    "NIFTY": ["16-Sep-2025", "23-Sep-2025", "30-Sep-2025", "07-Oct-2025"],
    "BANKNIFTY": ["30-Sep-2025"]
}

BASE_URL = "https://www.niftytrader.in"
OPTION_CHAIN_URL = (
    "https://webapi.niftytrader.in/webapi/option/option-chain-data"
    "?symbol={index}&exchange=nse&expiryDate={expiry}&atmBelow=0&atmAbove=0"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{BASE_URL}/option-chain",
    "Connection": "keep-alive",
}

POLLING_INTERVAL_SECONDS = 60

# ===== LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ===== FUNCTIONS =====
def create_session():
    """Create a requests session with retries."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    try:
        session.get(BASE_URL, timeout=30)
    except requests.RequestException as e:
        logger.error(f"Failed to initialize session: {e}")
        raise
    return session

def fetch_option_chain(session, index, expiry):
    """Fetch option chain data for a given index and expiry."""
    url = OPTION_CHAIN_URL.format(index=index, expiry=expiry)
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records", {}).get("data", [])
    df_rows = []
    for item in records:
        if item.get("expiryDate") != expiry:
            continue
        ce = item.get("CE", {})
        pe = item.get("PE", {})
        df_rows.append({
            "Strike Price": item.get("strikePrice"),
            "CE OI": ce.get("openInterest", 0),
            "CE Chng OI": ce.get("changeinOpenInterest", 0),
            "CE LTP": ce.get("lastPrice", 0),
            "PE LTP": pe.get("lastPrice", 0),
            "PE Chng OI": pe.get("changeinOpenInterest", 0),
            "PE OI": pe.get("openInterest", 0),
            "Expiry Date": expiry
        })
    df = pd.DataFrame(df_rows)
    logger.info(f"✅ Fetched {len(df)} rows for {index} expiry {expiry}")
    return df

def build_sheet_dfs(session):
    """Build DataFrames for all sheets."""
    sheet_dfs = {}
    for cfg in SHEET_CONFIG:
        index = cfg["index"]
        expiry_idx = cfg["expiry_index"]
        expiry = EXPIRIES[index][expiry_idx]
        df = fetch_option_chain(session, index, expiry)
        sheet_dfs[cfg["sheet_name"]] = df
    return sheet_dfs

def update_google_sheet(sheet_dfs):
    """Update Google Sheets with the fetched DataFrames."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
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
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="200", cols="20")
            worksheet.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Updated {sheet_name} with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to update {sheet_name}: {e}")

# ===== MAIN =====
if __name__ == "__main__":
    logger.info("Starting Option Chain Updater...")
    session = create_session()
    sheet_dfs = build_sheet_dfs(session)
    update_google_sheet(sheet_dfs)
    logger.info("All sheets updated successfully!")

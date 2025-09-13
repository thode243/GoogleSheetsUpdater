import requests
import pandas as pd
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import logging
import sys
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "service_account.json")  # GitHub Actions secret path

SHEET_CONFIG = [
    {"sheet_name": "sheet111", "index": "NIFTY", "expiry": "2025-09-16"},
    {"sheet_name": "sheet222", "index": "NIFTY", "expiry": "2025-09-23"},
    {"sheet_name": "sheet333", "index": "NIFTY", "expiry": "2025-09-30"},
    {"sheet_name": "sheet444", "index": "NIFTY", "expiry": "2025-10-07"},
    {"sheet_name": "sheet555", "index": "BANKNIFTY", "expiry": "2025-09-30"},
]

BASE_URL = "https://www.niftytrader.in"
OPTION_CHAIN_URL = (
    "https://webapi.niftytrader.in/webapi/option/option-chain-data"
    "?symbol={index}&exchange=nse&expiryDate={expiry}&atmBelow=0&atmAbove=0"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": f"{BASE_URL}/option-chain",
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
    try:
        session.get(BASE_URL, timeout=30)
    except requests.RequestException as e:
        logger.error(f"Failed to initialize session: {e}")
        raise
    return session

def fetch_option_chain(session, index, expiry):
    url = OPTION_CHAIN_URL.format(index=index, expiry=expiry)
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json().get("resultData", {}).get("opDatas", [])

    df_rows = []
    for item in data:
        df_rows.append({
            "Strike": item.get("strike_price", 0),
            "Call OI": item.get("calls_oi", 0),
            "Call LTP": item.get("calls_ltp", 0),
            "Call VWAP": item.get("calls_average_price", 0),
            "Call LTP - VWAP": item.get("calls_ltp", 0) - item.get("calls_average_price", 0),
            "Put OI": item.get("puts_oi", 0),
            "Put LTP": item.get("puts_ltp", 0),
            "Put VWAP": item.get("puts_average_price", 0),
            "Put LTP - VWAP": item.get("puts_ltp", 0) - item.get("puts_average_price", 0),
            "Change Call OI": item.get("calls_chng_oi", 0),
            "Change Put OI": item.get("puts_chng_oi", 0)
        })

    df = pd.DataFrame(df_rows)
    logger.info(f"✅ Fetched {len(df)} rows for {index} expiry {expiry}")
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

            worksheet.update([df.columns.tolist()] + df.values.tolist())
            logger.info(f"Updated {sheet_name} with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to update {sheet_name}: {e}")

# ===== MAIN =====
if __name__ == "__main__":
    logger.info("Starting Option Chain Updater with VWAP...")
    session = create_session()
    sheet_dfs = {}

    for cfg in SHEET_CONFIG:
        df = fetch_option_chain(session, cfg["index"], cfg["expiry"])
        sheet_dfs[cfg["sheet_name"]] = df  # store only DataFrame

    update_google_sheet(sheet_dfs)
    logger.info("All sheets updated successfully with VWAP!")


    







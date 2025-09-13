import requests
import pandas as pd
import gspread
import os
from time import sleep
from datetime import datetime
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys

# ===== CONFIG =====
SHEET_ID = "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow"
CREDENTIALS_PATH = r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-f68510b7dbdb.json"

SHEET_CONFIG = [
    {"sheet_name": "sheet111", "index": "NIFTY", "expiry_index": 0},
    {"sheet_name": "sheet222", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "sheet333", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "sheet444", "index": "NIFTY", "expiry_index": 3},
    {"sheet_name": "sheet555", "index": "BANKNIFTY", "expiry_index": 0},
]

BASE_URL = "https://www.nseindia.com"
OPTION_CHAIN_URL = "https://www.nseindia.com/api/option-chain-indices?symbol={index}"

POLLING_INTERVAL_SECONDS = 60  # 1 minute

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{BASE_URL}/option-chain",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ===== FUNCTIONS =====
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    # Get cookies from NSE homepage
    try:
        resp = session.get(BASE_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to get cookies from NSE: {e}")
        raise
    return session

def get_expiries(session, index, num_expiries=4):
    """Fetch upcoming expiry dates for the index."""
    url = OPTION_CHAIN_URL.format(index=index)
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    expiries = data.get("records", {}).get("expiryDates", [])
    if not expiries:
        raise ValueError(f"No expiry dates found for {index}")
    # Convert to datetime for sorting
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()
    expiry_dates = []
    for e in expiries:
        try:
            dt = datetime.strptime(e, "%d-%b-%Y").date()
            if dt >= today:
                expiry_dates.append(e)
        except Exception:
            continue
    expiry_dates.sort(key=lambda x: datetime.strptime(x, "%d-%b-%Y"))
    if index == "NIFTY":
        return expiry_dates[:4]
    else:
        return [expiry_dates[0]]  # BANKNIFTY, only 1 expiry

def fetch_option_chain(session, index):
    url = OPTION_CHAIN_URL.format(index=index)
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records", {}).get("data", [])
    return records

def build_sheet_dfs(session):
    """Return a dict of DataFrames keyed by sheet name."""
    # Prepare expiry map
    indices = list(set([cfg["index"] for cfg in SHEET_CONFIG]))
    expiry_map = {}
    for idx in indices:
        expiry_map[idx] = get_expiries(session, idx)

    sheet_dfs = {}
    for cfg in SHEET_CONFIG:
        index = cfg["index"]
        expiry_idx = cfg["expiry_index"]
        expiry = expiry_map[index][expiry_idx]
        records = fetch_option_chain(session, index)
        df_rows = []
        for item in records:
            if item.get("expiryDate") != expiry:
                continue
            strike = item.get("strikePrice")
            ce = item.get("CE", {})
            pe = item.get("PE", {})
            df_rows.append({
                "Strike Price": strike,
                "CE OI": ce.get("openInterest", 0),
                "CE Chng OI": ce.get("changeinOpenInterest", 0),
                "CE LTP": ce.get("lastPrice", 0),
                "PE LTP": pe.get("lastPrice", 0),
                "PE Chng OI": pe.get("changeinOpenInterest", 0),
                "PE OI": pe.get("openInterest", 0),
                "Expiry Date": expiry
            })
        df = pd.DataFrame(df_rows)
        sheet_dfs[cfg["sheet_name"]] = df
        logger.info(f"✅ Fetched {len(df)} rows for {index} expiry {expiry}")
    return sheet_dfs

def update_google_sheet(sheet_dfs):
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
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())
            logger.info(f"Updated {sheet_name} with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to update {sheet_name}: {e}")

# ===== MAIN =====
if __name__ == "__main__":
    logger.info("Starting Option Chain Updater...")
    session = create_session()
    dfs = build_sheet_dfs(session)
    update_google_sheet(dfs)
    logger.info("All sheets updated successfully!")


import requests
import pandas as pd
import gspread
import os
from time import sleep
from datetime import datetime
from datetime import time as dtime
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys

# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "1vCvyVA_eOFT8nAyLjywo0EgyGtHqynUWCY4_O2VHc_w")
SHEET_NAME = os.getenv("SHEET_NAME", "Sheet2")
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\fetching-data-468910-02079de166c4.json"
)

BASE_URL = "https://www.nseindia.com"
OPTION_CHAIN_URL = f"{BASE_URL}/api/option-chain-indices?symbol=NIFTY"

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

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(stream=sys.stdout),
        logging.FileHandler("option_chain_updater.log", encoding="utf-8"),
    ],
)
class UnicodeSafeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            msg = msg.encode("ascii", errors="replace").decode("ascii")
            self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

logger = logging.getLogger(__name__)
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        logger.removeHandler(handler)
logger.addHandler(UnicodeSafeStreamHandler(stream=sys.stdout))

# ===== FUNCTIONS =====
def create_session():
    """Create a requests session with retry logic and proper cookie handling."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    try:
        # Fetch cookies from the option chain page
        response = session.get(f"{BASE_URL}/option-chain", headers=HEADERS, timeout=10)
        response.raise_for_status()
        cookies = session.cookies.get_dict()
        logger.debug(f"Cookies fetched: {cookies}")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch cookies from {BASE_URL}/option-chain: {e}")
        raise
    return session

def get_latest_expiry(session):
    """Fetch the latest expiry date from the NSE API."""
    try:
        response = session.get(OPTION_CHAIN_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        expiries = data.get("records", {}).get("expiryDates", [])
        if not expiries:
            raise ValueError("No expiry dates found from NSE.")
        return expiries[0]  # Return the earliest expiry date
    except requests.RequestException as e:
        logger.error(f"Failed to fetch expiry dates: {e}")
        raise

def fetch_option_chain():
    """Fetch and process NIFTY option chain data."""
    try:
        session = create_session()
        sleep(1)  # Delay to avoid rate-limiting
        # Get the latest expiry date
        latest_expiry = get_latest_expiry(session)
        logger.info(f"Using expiry date: {latest_expiry}")
        
        response = session.get(OPTION_CHAIN_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()

        option_data = data.get("records", {}).get("data", [])
        if not option_data:
            raise ValueError("No option chain data found.")

        rows = []
        for entry in option_data:
            strike = entry.get("strikePrice")
            expiry = entry.get("expiryDate")
            ce = entry.get("CE", {})
            pe = entry.get("PE", {})

            rows.append({
                "CE OI": ce.get("openInterest", 0),
                "CE Chng OI": ce.get("changeinOpenInterest", 0),
                "CE LTP": ce.get("lastPrice", 0),
                "Strike Price": strike,
                "Expiry Date": expiry,
                "PE LTP": pe.get("lastPrice", 0),
                "PE Chng OI": pe.get("changeinOpenInterest", 0),
                "PE OI": pe.get("openInterest", 0),
            })

        df = pd.DataFrame(rows)
        # Filter for the latest expiry date
        df = df[df["Expiry Date"] == latest_expiry]
        if df.empty:
            raise ValueError(f"No data found for expiry date {latest_expiry}")
        logger.info(f"Fetched {len(df)} rows from NSE for expiry {latest_expiry}")
        return df

    except requests.HTTPError as e:
        if e.response.status_code == 401:
            logger.error("401 Unauthorized: Check cookies, headers, or NSE API restrictions.")
        raise
    except requests.RequestException as e:
        logger.error(f"HTTP error while fetching option chain: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        raise

def update_google_sheet(df):
    """Update Google Sheet with the provided DataFrame."""
    try:
        if not os.path.exists(CREDENTIALS_PATH):
            raise FileNotFoundError(f"Google credentials file not found at {CREDENTIALS_PATH}")

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

        sheet.clear()
        data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update("A1", data)
        logger.info(f"Updated Google Sheet with {len(df)} rows at {datetime.now(pytz.timezone('Asia/Kolkata'))}")

    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error updating Google Sheet: {e}")
        raise

def is_market_open():
    """Check if the market is open based on IST time."""
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    current_time = now.time()
    current_date = now.date()
    market_start = dtime(1, 15)  # 9:15 AM IST
    market_end = dtime(23, 00)  # 3:30 PM IST
    
    is_weekday = current_date.weekday() < 7
    is_open = is_weekday and market_start <= current_time <= market_end
    logger.debug(f"Market open check: {is_open} (Current time: {current_time}, Date: {current_date}, IST)")
    return is_open

# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("Starting NIFTY option chain updater...")
    while True:
        try:
            if is_market_open():
                logger.info("Market is open, fetching option chain data...")
                df = fetch_option_chain()
                update_google_sheet(df)
            else:
                logger.info("Market is closed, skipping update...")
            logger.info(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds...")
            sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            logger.info(f"Retrying after {POLLING_INTERVAL_SECONDS} seconds...")
            sleep(POLLING_INTERVAL_SECONDS)

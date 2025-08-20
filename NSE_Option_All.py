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
SHEET_NAMES = ["Sheet1", "Sheet2", "Sheet3", "Sheet4"]  # Worksheets for different expiries
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"
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
        response = session.get(f"{BASE_URL}/option-chain", headers=HEADERS, timeout=10)
        response.raise_for_status()
        cookies = session.cookies.get_dict()
        logger.debug(f"Cookies fetched: {cookies}")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch cookies from {BASE_URL}/option-chain: {e}")
        raise
    return session

def get_latest_expiries(session, num_expiries=4):
    """Fetch the next 'num_expiries' future expiry dates from the NSE API."""
    try:
        response = session.get(OPTION_CHAIN_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        expiries = data.get("records", {}).get("expiryDates", [])
        if not expiries:
            raise ValueError("No expiry dates found from NSE.")

        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist).date()
        expiry_dates = []
        for expiry in expiries:
            try:
                expiry_date = datetime.strptime(expiry, "%d-%b-%Y").date()
                if expiry_date > today:
                    expiry_dates.append((expiry_date, expiry))
            except ValueError:
                logger.warning(f"Invalid expiry date format: {expiry}")
                continue

        if not expiry_dates:
            raise ValueError(f"No future expiry dates found. Available dates: {expiries}")
        
        # Sort by date and take the next 'num_expiries' expiries
        expiry_dates.sort(key=lambda x: x[0])
        return [expiry[1] for expiry in expiry_dates[:num_expiries]]

    except requests.RequestException as e:
        logger.error(f"Failed to fetch expiry dates: {e}")
        raise

def fetch_option_chain():
    """Fetch and process NIFTY option chain data for multiple expiries."""
    try:
        session = create_session()
        sleep(1)
        expiries = get_latest_expiries(session, num_expiries=4)  # Get 4 future expiries
        logger.info(f"Using expiry dates: {expiries}")
        
        response = session.get(OPTION_CHAIN_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()

        option_data = data.get("records", {}).get("data", [])
        if not option_data:
            raise ValueError("No option chain data found.")

        # Dictionary to store DataFrames for each expiry
        expiry_dfs = {}
        for entry in option_data:
            expiry = entry.get("expiryDate")
            if expiry in expiries:
                strike = entry.get("strikePrice")
                ce = entry.get("CE", {})
                pe = entry.get("PE", {})

                row = {
                    "CE OI": ce.get("openInterest", 0),
                    "CE Chng OI": ce.get("changeinOpenInterest", 0),
                    "CE LTP": ce.get("lastPrice", 0),
                    "Strike Price": strike,
                    "Expiry Date": expiry,
                    "PE LTP": pe.get("lastPrice", 0),
                    "PE Chng OI": pe.get("changeinOpenInterest", 0),
                    "PE OI": pe.get("openInterest", 0),
                }
                if expiry not in expiry_dfs:
                    expiry_dfs[expiry] = []
                expiry_dfs[expiry].append(row)

        # Convert to DataFrames
        dfs = {expiry: pd.DataFrame(rows) for expiry, rows in expiry_dfs.items()}
        for expiry, df in dfs.items():
            if df.empty:
                raise ValueError(f"No data found for expiry date {expiry}")
            logger.info(f"Fetched {len(df)} rows from NSE for expiry {expiry}")
        return dfs

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

def update_google_sheet(dfs):
    """Update Google Sheet with multiple DataFrames in different worksheets."""
    try:
        if not os.path.exists(CREDENTIALS_PATH):
            raise FileNotFoundError(f"Google credentials file not found at {CREDENTIALS_PATH}")

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
        client = gspread.authorize(creds)

        # Ensure the spreadsheet has enough worksheets
        spreadsheet = client.open_by_key(SHEET_ID)
        while len(spreadsheet.worksheets()) < len(SHEET_NAMES):
            spreadsheet.add_worksheet(title=f"Sheet{len(spreadsheet.worksheets()) + 1}", rows="100", cols="20")
        worksheets = [spreadsheet.worksheet(sheet_name) for sheet_name in SHEET_NAMES]

        for df, worksheet in zip(dfs.values(), worksheets):
            worksheet.clear()
            data = [df.columns.values.tolist()] + df.values.tolist()
            worksheet.update(range_name="A1", values=data)
            logger.info(f"Updated {worksheet.title} with {len(df)} rows at {datetime.now(pytz.timezone('Asia/Kolkata'))}")

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
    market_start = dtime(8, 40)  # 9:15 AM IST
    market_end = dtime(15, 35)   # 3:30 PM IST
    
    is_weekday = current_date.weekday() < 6  # Monday to Friday only
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
                dfs = fetch_option_chain()
                update_google_sheet(dfs)
            else:
                logger.info("Market is closed, skipping update...")
            logger.info(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds...")
            sleep(POLLING_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            logger.info(f"Retrying after {POLLING_INTERVAL_SECONDS} seconds...")
            sleep(POLLING_INTERVAL_SECONDS)

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

# ===== CONFIG =====
# ===== CONFIG =====
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},  # First expiry
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},  # Second expiry
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},  # Third expiry
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3},  # Fourth expiry
   {"sheet_name": "Sheet5", "index": "BANKNIFTY", "expiry_index": None},
   # {"sheet_name": "Sheet6", "index": "MIDCPNIFTY", "expiry_index": None},
  # {"sheet_name": "Sheet7", "index": "FINNIFTY", "expiry_index": None},
   {"sheet_name": "Sheet8", "index": "NIFTY", "expiry_index": 3},  # Fifth expiry
    
]
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 60))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-f68510b7dbdb"
)

BASE_URL = "https://www.nseindia.com"
OPTION_CHAIN_URL = f"{BASE_URL}/api/option-chain-indices?symbol={{index}}"

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
        logging.FileHandler("option_chain_updater1.log", encoding="utf-8"),
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
    session.headers.update(HEADERS)

    try:
        # 👇 Step 1: Hit NSE homepage, not /option-chain
        response = session.get(BASE_URL, timeout=60)
        response.raise_for_status()
        cookies = session.cookies.get_dict()
        logger.debug(f"Cookies fetched: {cookies}")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch cookies from {BASE_URL}: {e}")
        raise
    return session


def get_latest_expiries(session, index, num_expiries=4):
    """Fetch the next 'num_expiries' future expiry dates for a given index from the NSE API."""
    try:
        url = OPTION_CHAIN_URL.format(index=index)
        response = session.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        data = response.json()
        expiries = data.get("records", {}).get("expiryDates", [])
        if not expiries:
            raise ValueError(f"No expiry dates found for {index}.")

        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist).date()
        expiry_dates = []
        for expiry in expiries:
            try:
                expiry_date = datetime.strptime(expiry, "%d-%b-%Y").date()
                if expiry_date >= today:
                    expiry_dates.append((expiry_date, expiry))
            except ValueError:
                logger.warning(f"Invalid expiry date format for {index}: {expiry}")
                continue

        if not expiry_dates:
            raise ValueError(f"No future expiry dates found for {index}. Available dates: {expiries}")
        
        # Sort by date and take the next 'num_expiries' expiries
        expiry_dates.sort(key=lambda x: x[0])
        return [expiry[1] for expiry in expiry_dates[:num_expiries]]

    except requests.RequestException as e:
        logger.error(f"Failed to fetch expiry dates for {index}: {e}")
        raise

def fetch_option_chain():
    """Fetch and process option chain data for multiple indices and expiries."""
    try:
        session = create_session()
        sleep(1)
        
        # Get unique indices from SHEET_CONFIG
        indices = list(set(config["index"] for config in SHEET_CONFIG))
        expiry_map = {}
        
        # Fetch expiry dates for each index
        for index in indices:
            if index == "NIFTY":
                expiry_map[index] = get_latest_expiries(session, index, num_expiries=4)
            else:
                expiry_map[index] = get_latest_expiries(session, index, num_expiries=1)  # Get at least one expiry for others
        logger.info(f"Expiry dates: {expiry_map}")

        # Dictionary to store DataFrames for each sheet
        sheet_dfs = {config["sheet_name"]: None for config in SHEET_CONFIG}

        # Fetch option chain data for each index
        for index in indices:
            url = OPTION_CHAIN_URL.format(index=index)
            response = session.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            data = response.json()

            option_data = data.get("records", {}).get("data", [])
            if not option_data:
                raise ValueError(f"No option chain data found for {index}.")

            # Organize data by expiry for this index
            expiry_dfs = {}
            for entry in option_data:
                expiry = entry.get("expiryDate")
                if expiry in expiry_map[index]:
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

            # Convert to DataFrames and assign to sheets
            for config in SHEET_CONFIG:
                if config["index"] == index:
                    sheet_name = config["sheet_name"]
                    expiry_index = config["expiry_index"]
                    if expiry_index is not None and index == "NIFTY":
                        # For NIFTY sheets, select specific expiry
                        expiry = expiry_map[index][expiry_index]
                        if expiry in expiry_dfs:
                            df = pd.DataFrame(expiry_dfs[expiry])
                            sheet_dfs[sheet_name] = df
                    elif expiry_index is None:
                        # For non-NIFTY sheets, use the first available expiry
                        expiry = expiry_map[index][0]
                        if expiry in expiry_dfs:
                            df = pd.DataFrame(expiry_dfs[expiry])
                            sheet_dfs[sheet_name] = df

        # Validate DataFrames
        for sheet_name, df in sheet_dfs.items():
            if df is None or df.empty:
                logger.warning(f"No data found for sheet {sheet_name}")
            else:
                logger.info(f"Fetched {len(df)} rows for sheet {sheet_name} (index: {next(config['index'] for config in SHEET_CONFIG if config['sheet_name'] == sheet_name)})")
        return sheet_dfs



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
        existing_sheets = {ws.title for ws in spreadsheet.worksheets()}
        for config in SHEET_CONFIG:
            if config["sheet_name"] not in existing_sheets:
                spreadsheet.add_worksheet(title=config["sheet_name"], rows="100", cols="20")

        # Update each worksheet
        for config in SHEET_CONFIG:
            sheet_name = config["sheet_name"]
            df = dfs.get(sheet_name)
            if df is None or df.empty:
                logger.warning(f"Skipping update for {sheet_name} due to missing or empty data")
                continue
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()
            data = [df.columns.values.tolist()] + df.values.tolist()
            worksheet.update(range_name="A1", values=data)
            logger.info(f"Updated {sheet_name} with {len(df)} rows at {datetime.now(pytz.timezone('Asia/Kolkata'))}")

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
    market_start = dtime(9, 10)
    market_end = dtime(15, 35)
   
    is_weekday = current_date.weekday() < 5  # Monday to Friday only
    is_open = is_weekday and market_start <= current_time <= market_end
    logger.debug(f"Market open check: {is_open} (Current time: {current_time}, Date: {current_date}, IST)")
    return is_open


# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("Starting option chain updater...")
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









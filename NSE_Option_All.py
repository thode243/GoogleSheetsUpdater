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
SHEET_NAMES = ["Sheet1", "Sheet2", "Sheet3", "Sheet4", "Sheet5", "Sheet6", "Sheet7"]  # Worksheets for NIFTY and new indices
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\online-fetching-71bca82ecbf5.json"
)

BASE_URL = "https://www.nseindia.com"
INDICES = ["NIFTY", "BANKNIFTY", "MIDCAPNIFTY", "FINNIFTY"]  # Symbols to fetch
NIFTY_EXPIRY_DATES = ["21-Aug-2025", "28-Aug-2025", "02-Sep-2025", "09-Sep-2025"]  # Fixed expiry dates for NIFTY

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
log_file = os.path.join(os.getenv("GITHUB_WORKSPACE", "."), "option_chain_updater.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(stream=sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
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

def get_latest_expiries(session, symbol):
    """Fetch the next future expiry date for a given symbol from the NSE API."""
    try:
        url = f"{BASE_URL}/api/option-chain-indices?symbol={symbol}"
        response = session.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        expiries = data.get("records", {}).get("expiryDates", [])
        if not expiries:
            raise ValueError(f"No expiry dates found for {symbol} from NSE.")

        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist).date()
        expiry_dates = []
        for expiry in expiries:
            try:
                expiry_date = datetime.strptime(expiry, "%d-%b-%Y").date()
                if expiry_date > today:
                    expiry_dates.append((expiry_date, expiry))
            except ValueError:
                logger.warning(f"Invalid expiry date format for {symbol}: {expiry}")
                continue

        if not expiry_dates:
            raise ValueError(f"No future expiry dates found for {symbol}. Available dates: {expiries}")
        
        # Sort by date and take the next expiry
        expiry_dates.sort(key=lambda x: x[0])
        return expiry_dates[0][1]  # Return the nearest future expiry

    except requests.RequestException as e:
        logger.error(f"Failed to fetch expiry dates for {symbol}: {e}")
        raise

def fetch_option_chain():
    """Fetch and process option chain data for multiple indices with specific expiries."""
    try:
        session = create_session()
        sleep(1)
        index_dfs = {}
        
        for idx, symbol in enumerate(INDICES):
            if symbol == "NIFTY":
                # Use fixed expiry dates for NIFTY
                expiries = NIFTY_EXPIRY_DATES
                for expiry in expiries:
                    logger.info(f"Using fixed expiry date for {symbol}: {expiry}")
                    url = f"{BASE_URL}/api/option-chain-indices?symbol={symbol}"
                    response = session.get(url, headers=HEADERS, timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    option_data = data.get("records", {}).get("data", [])
                    if not option_data:
                        raise ValueError(f"No option chain data found for {symbol}.")

                    expiry_rows = []
                    for entry in option_data:
                        if entry.get("expiryDate") == expiry:
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
                            expiry_rows.append(row)

                    df = pd.DataFrame(expiry_rows)
                    if df.empty:
                        raise ValueError(f"No data found for {symbol} expiry {expiry}")
                    sheet_name = SHEET_NAMES[idx * len(NIFTY_EXPIRY_DATES) + NIFTY_EXPIRY_DATES.index(expiry)]
                    logger.info(f"Fetched {len(df)} rows from NSE for {symbol} expiry {expiry}")
                    index_dfs[sheet_name] = df
            else:
                # Use nearest future expiry for other indices
                expiry = get_latest_expiries(session, symbol)
                logger.info(f"Using expiry date for {symbol}: {expiry}")
                url = f"{BASE_URL}/api/option-chain-indices?symbol={symbol}"
                response = session.get(url, headers=HEADERS, timeout=10)
                response.raise_for_status()
                data = response.json()

                option_data = data.get("records", {}).get("data", [])
                if not option_data:
                    raise ValueError(f"No option chain data found for {symbol}.")

                expiry_rows = []
                for entry in option_data:
                    if entry.get("expiryDate") == expiry:
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
                        expiry_rows.append(row)

                df = pd.DataFrame(expiry_rows)
                if df.empty:
                    raise ValueError(f"No data found for {symbol} expiry {expiry}")
                sheet_name = SHEET_NAMES[4 + INDICES.index(symbol) - 1]  # Sheet5, Sheet6, Sheet7
                logger.info(f"Fetched {len(df)} rows from NSE for {symbol} expiry {expiry}")
                index_dfs[sheet_name] = df

        return index_dfs

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
    """Update Google Sheet with DataFrames for each sheet."""
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

        for sheet_name, df in dfs.items():
            idx = SHEET_NAMES.index(sheet_name)
            worksheet = worksheets[idx]
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
    market_start = dtime(8, 40)  # 9:15 AM IST
    market_end = dtime(15, 35)   # 3:30 PM IST
    
    is_weekday = current_date.weekday() < 6  # Monday to Friday only
    is_open = is_weekday and market_start <= current_time <= market_end
    logger.debug(f"Market open check: {is_open} (Current time: {current_time}, Date: {current_date}, IST)")
    return is_open

# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("Starting multi-index option chain updater...")
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

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
    {"sheet_name": "Sheet6", "index": "MIDCPNIFTY", "expiry_index": None},
    # {"sheet_name": "Sheet7", "index": "FINNIFTY", "expiry_index": None},
    {"sheet_name": "Sheet8", "index": "NIFTY", "expiry_index": 3},  # Fifth expiry
    {"sheet_name": "Sheet9", "index": "NIFTY", "expiry_index": 0},  # First expiry (from NiftyTrader)
    
]
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
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
    
    try:
        response = session.get(f"{BASE_URL}/option-chain", headers=HEADERS, timeout=30)
        response.raise_for_status()
        cookies = session.cookies.get_dict()
        logger.debug(f"Cookies fetched: {cookies}")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch cookies from {BASE_URL}/option-chain: {e}")
        raise
    return session

def get_latest_expiries(session, index, num_expiries=4):
    """Fetch the next 'num_expiries' future expiry dates for a given index from the NSE API."""
    try:
        url = OPTION_CHAIN_URL.format(index=index)
        response = session.get(url, headers=HEADERS, timeout=30)
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

        indices = list(set(config["index"] for config in SHEET_CONFIG if config["index"]))
        expiry_map = {}

        # NSE expiries (for all except Sheet9)
        for index in indices:
            if index == "NIFTY":
                expiry_map[index] = get_latest_expiries(session, index, num_expiries=4)
            else:
                expiry_map[index] = get_latest_expiries(session, index, num_expiries=1)

        sheet_dfs = {config["sheet_name"]: None for config in SHEET_CONFIG}

        for config in SHEET_CONFIG:
            sheet_name = config["sheet_name"]
            index = config["index"]
            expiry_index = config["expiry_index"]

           # ✅ Special case for Sheet9 → NiftyTrader API
            if sheet_name == "Sheet9":
                expiry = expiry_map["NIFTY"][0]  # first expiry
                nt_url = (
                    "https://webapi.niftytrader.in/webapi/option/option-chain-data"
                    f"?symbol={index}&exchange=nse&expiryDate={expiry}&atmBelow=0&atmAbove=0"
                )

                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
                    "Accept": "application/json",
                    "Referer": "https://www.niftytrader.in/",
                    "Origin": "https://www.niftytrader.in",
                }

                response = requests.get(nt_url, headers=headers, timeout=30)

                logger.info(f"Requesting: {nt_url}")
                logger.info(f"Status: {response.status_code}")
                logger.debug(f"Response (truncated): {response.text[:200]}")

                if response.status_code != 200:
                    logger.error("Failed to fetch data for Sheet9")
                    continue

                data = response.json()
                option_data = data.get("data", {}).get("records", [])
                if not option_data:
                    logger.warning("No option data found in response for Sheet9")
                    continue

                # ✅ Process entries including VWAP
                rows = []
                for entry in option_data:
                    strike = entry.get("strikePrice")
                    ce = entry.get("CE", {})
                    pe = entry.get("PE", {})
                    rows.append({
                        "CE OI": ce.get("openInterest", 0),
                        "CE Chng OI": ce.get("changeinOpenInterest", 0),
                        "CE LTP": ce.get("lastPrice", 0),
                        "CE VWAP": ce.get("vwap", 0),   # added
                        "Strike Price": strike,
                        "Expiry Date": expiry,
                        "PE LTP": pe.get("lastPrice", 0),
                        "PE VWAP": pe.get("vwap", 0),   # added
                        "PE Chng OI": pe.get("changeinOpenInterest", 0),
                        "PE OI": pe.get("openInterest", 0),
                    })

                if rows:
                    sheet_dfs[sheet_name] = pd.DataFrame(rows)
                continue  # skip NSE logic for Sheet9


            # ✅ Normal NSE logic (Sheet1–8)
            url = OPTION_CHAIN_URL.format(index=index)
            response = session.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()
            option_data = data.get("records", {}).get("data", [])
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

            if expiry_index is not None and index == "NIFTY":
                expiry = expiry_map[index][expiry_index]
                if expiry in expiry_dfs:
                    sheet_dfs[sheet_name] = pd.DataFrame(expiry_dfs[expiry])
            elif expiry_index is None:
                expiry = expiry_map[index][0]
                if expiry in expiry_dfs:
                    sheet_dfs[sheet_name] = pd.DataFrame(expiry_dfs[expiry])

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
    market_end = dtime(18, 35)
    return now.weekday() in [0, 1, 2, 3, 4, 6] and market_start <= now.time() <= market_end
    # return now.weekday() < 5 and market_start <= now.time() <= market_end
    
    # is_weekday = current_date.weekday() < 5  # Monday to Friday only
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






























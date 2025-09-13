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
#
SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
SHEET_CONFIG = [
    {"sheet_name": "Sheet1", "index": "NIFTY", "expiry_index": 0},
    {"sheet_name": "Sheet2", "index": "NIFTY", "expiry_index": 1},
    {"sheet_name": "Sheet3", "index": "NIFTY", "expiry_index": 2},
    {"sheet_name": "Sheet4", "index": "NIFTY", "expiry_index": 3},
    {"sheet_name": "Sheet5", "index": "BANKNIFTY", "expiry_index": None},
    {"sheet_name": "Sheet8", "index": "NIFTY", "expiry_index": 3},
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
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/option-chain",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

# ===== LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(stream=sys.stdout),
        logging.FileHandler("option_chain_updater1.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ===== FUNCTIONS =====
def create_session():
    """Create a requests session with retry logic, warmup, and cookies."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)

    try:
        # Step 1: Visit homepage
        resp1 = session.get(BASE_URL, timeout=30)
        resp1.raise_for_status()
        logger.debug("Homepage loaded, cookies set.")

        # Step 2: Visit option-chain page (sets more cookies)
        resp2 = session.get(f"{BASE_URL}/option-chain", timeout=30)
        resp2.raise_for_status()
        logger.debug("Option-chain page loaded, cookies updated.")

        logger.info("Session created successfully with cookies.")
    except requests.RequestException as e:
        logger.error(f"Failed to initialize session: {e}")
        raise
    return session

# (👉 keep the rest of your code unchanged: get_latest_expiries, fetch_option_chain,
# update_google_sheet, is_market_open, and the main loop.)

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
            raise ValueError(f"No future expiry dates found for {index}. Available: {expiries}")

        expiry_dates.sort(key=lambda x: x[0])
        return [expiry[1] for expiry in expiry_dates[:num_expiries]]

    except requests.RequestException as e:
        logger.error(f"Failed to fetch expiry dates for {index}: {e}")
        raise


def fetch_option_chain(session):
    """Fetch and process option chain data for multiple indices and expiries."""
    try:
        # Get unique indices from SHEET_CONFIG
        indices = list(set(config["index"] for config in SHEET_CONFIG))
        expiry_map = {}

        # Fetch expiry dates for each index
        for index in indices:
            if index == "NIFTY":
                expiry_map[index] = get_latest_expiries(session, index, num_expiries=4)
            else:
                expiry_map[index] = get_latest_expiries(session, index, num_expiries=1)
        logger.info(f"Expiry dates: {expiry_map}")

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
                    expiry_dfs.setdefault(expiry, []).append(row)

            # Assign dataframes to sheets
            for config in SHEET_CONFIG:
                if config["index"] == index:
                    sheet_name = config["sheet_name"]
                    expiry_index = config["expiry_index"]
                    if expiry_index is not None and index == "NIFTY":
                        expiry = expiry_map[index][expiry_index]
                        if expiry in expiry_dfs:
                            sheet_dfs[sheet_name] = pd.DataFrame(expiry_dfs[expiry])
                    elif expiry_index is None:
                        expiry = expiry_map[index][0]
                        if expiry in expiry_dfs:
                            sheet_dfs[sheet_name] = pd.DataFrame(expiry_dfs[expiry])

        for sheet_name, df in sheet_dfs.items():
            if df is None or df.empty:
                logger.warning(f"No data found for sheet {sheet_name}")
            else:
                idx = next(c["index"] for c in SHEET_CONFIG if c["sheet_name"] == sheet_name)
                logger.info(f"Fetched {len(df)} rows for {sheet_name} (index: {idx})")

        return sheet_dfs

    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        raise


def is_market_open():
    """Check if the market is open (Mon–Fri, 9:10–15:35 IST)."""
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    current_time = now.time()
    current_date = now.date()

    market_start = dtime(9, 10)
    market_end = dtime(15, 35)

    is_weekday = current_date.weekday() < 6  # Mon=0 … Fri=4
    is_open = is_weekday and market_start <= current_time <= market_end
    return is_open


# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("Starting option chain updater...")

    session = create_session()   # 👈 one persistent session

    while True:
        try:
            if is_market_open():
                logger.info("Market is open, fetching option chain data...")
                dfs = fetch_option_chain(session)
                update_google_sheet(dfs)
            else:
                logger.info("Market is closed, skipping update...")

            logger.info(f"Sleeping for {POLLING_INTERVAL_SECONDS} seconds...")
            sleep(POLLING_INTERVAL_SECONDS)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            logger.info(f"Retrying after {POLLING_INTERVAL_SECONDS} seconds...")
            sleep(POLLING_INTERVAL_SECONDS)



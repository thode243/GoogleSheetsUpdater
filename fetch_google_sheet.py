import requests
import pandas as pd
import gspread
import os
import logging
from datetime import datetime, time as dtime
from time import sleep
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== CONFIG ==================
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Sheet1")
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL", 30))
CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    r"C:\Users\user\Desktop\GoogleSheetsUpdater\fetching-data-468910-02079de166c4.json"
)

BASE_URL = "https://www.nseindia.com"
OPTION_CHAIN_URL = f"{BASE_URL}/api/option-chain-indices?symbol=NIFTY"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{BASE_URL}/option-chain",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("option_chain_updater.log"),
    ],
)
logger = logging.getLogger(__name__)

# ================== FUNCTIONS ==================
def create_session():
    """Create a requests session, load NSE cookies from homepage and option chain page."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Step 1: Visit homepage to get initial cookies
    session.get(BASE_URL, headers=HEADERS, timeout=10)
    # Step 2: Visit option chain HTML page to get authenticated cookies
    session.get(f"{BASE_URL}/option-chain", headers=HEADERS, timeout=10)

    return session

def fetch_option_chain():
    """Fetch and process NIFTY option chain data."""
    try:
        session = create_session()
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
                "CE Volume": ce.get("totalTradedVolume", 0),
                "CE IV": ce.get("impliedVolatility", 0),
                "CE LTP": ce.get("lastPrice", 0),
                "CE Chng LTP": ce.get("change", 0),
                "CE Bid Qty": ce.get("bidQty", 0),
                "CE Bid Price": ce.get("bidprice", 0),
                "CE Ask Price": ce.get("askPrice", 0),
                "CE Ask Qty": ce.get("askQty", 0),
                "Strike Price": strike,
                "Expiry Date": expiry,
                "PE Bid Qty": pe.get("bidQty", 0),
                "PE Bid Price": pe.get("bidprice", 0),
                "PE Ask Price": pe.get("askPrice", 0),
                "PE Ask Qty": pe.get("askQty", 0),
                "PE Chng LTP": pe.get("change", 0),
                "PE LTP": pe.get("lastPrice", 0),
                "PE IV": pe.get("impliedVolatility", 0),
                "PE Volume": pe.get("totalTradedVolume", 0),
                "PE Chng OI": pe.get("changeinOpenInterest", 0),
                "PE OI": pe.get("openInterest", 0),
            })

        df = pd.DataFrame(rows)
        if df.empty:
            raise ValueError("Empty DataFrame created from option chain data.")
        return df

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

        # Clear and update sheet
        sheet.clear()
        data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update("A1", data)

        logger.info(f"Updated Google Sheet with {len(df)} rows at {datetime.now()}")

    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error updating Google Sheet: {e}")
        raise

def is_market_open():
    """Check if the market is open based on IST time."""
    now = datetime.now().time()
    market_start = dtime(9, 15)  # 9:15 AM IST
    market_end = dtime(18, 30)  # 3:30 PM IST
    is_open = market_start <= now <= market_end
    logger.debug(f"Market open check: {is_open} (Current time: {now})")
    return is_open

def main():
    """Main loop to periodically fetch and update option chain data."""
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

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

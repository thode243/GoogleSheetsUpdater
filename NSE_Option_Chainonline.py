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
#
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
    call_diff_sum = 0
    put_diff_sum = 0
    for item in data:
        strike = item.get("strike_price", 0)
        call_oi = item.get("calls_oi", 0)
        call_ltp = item.get("calls_ltp", 0)
        call_vwap = item.get("calls_average_price", 0)
        call_chg_oi = item.get("calls_chng_oi", 0)
        put_oi = item.get("puts_oi", 0)
        put_ltp = item.get("puts_ltp", 0)
        put_vwap = item.get("puts_average_price", 0)
        put_chg_oi = item.get("puts_chng_oi", 0)

        call_diff_sum += call_ltp - call_vwap
        put_diff_sum += put_ltp - put_vwap

        df_rows.append({
            "Strike": strike,
            "Call OI": call_oi,
            "Call LTP": call_ltp,
            "Call VWAP": call_vwap,
            "Call LTP - VWAP": call_ltp - call_vwap,
            "Put OI": put_oi,
            "Put LTP": put_ltp,
            "Put VWAP": put_vwap,
            "Put LTP - VWAP": put_ltp - put_vwap,
            "Change Call OI": call_chg_oi,
            "Change Put OI": put_chg_oi
        })

    df = pd.DataFrame(df_rows)
    logger.info(f"✅ Fetched {len(df)} rows for {index} expiry {expiry}")
    return df, call_diff_sum, put_diff_sum

def update_google_sheet(sheet_dfs, spot_value=0):
    """Update Google Sheets with the fetched DataFrames."""
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)

    for sheet_name, df_tuple in sheet_dfs.items():
        df, call_diff_sum, put_diff_sum = df_tuple
        if df.empty:
            logger.warning(f"No data for {sheet_name}, skipping...")
            continue
        try:
            # ✅ Check if sheet exists first
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="200", cols="30")

            # Update data
            worksheet.update([df.columns.tolist()] + df.values.tolist())

            # Optional: update spot value and LTP–VWAP sums
            if spot_value:
                worksheet.update("P2", spot_value)
            worksheet.update("F3", call_diff_sum)
            worksheet.update("K3", put_diff_sum)

            logger.info(f"Updated {sheet_name} with {len(df)} rows")

        except Exception as e:
            logger.error(f"Failed to update {sheet_name}: {e}")


# ===== MAIN =====
if __name__ == "__main__":
    logger.info("Starting Option Chain Updater with VWAP...")
    session = create_session()
    sheet_dfs = {}
    spot_value = 0  # You can fetch spot from another API or Sheet if needed

    for cfg in SHEET_CONFIG:
        df, call_diff_sum, put_diff_sum = fetch_option_chain(session, cfg["index"], cfg["expiry"])
        sheet_dfs[cfg["sheet_name"]] = (df, call_diff_sum, put_diff_sum)

    update_google_sheet(sheet_dfs, spot_value)
    logger.info("All sheets updated successfully with VWAP!")


    




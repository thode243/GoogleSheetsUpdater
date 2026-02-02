# daily run

import requests
import pandas as pd
import gspread
import os
from time import sleep
from datetime import datetime, date, time, timedelta
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys
import uuid
import re
import numpy as np
from io import StringIO
from zoneinfo import ZoneInfo

# ==== CONFIG =================

SHEET_ID = os.getenv("SHEET_ID", "15pghBDGQ34qSMI2xXukTYD4dzG2cOYIYmXfCtb-X5ow")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "service_account.json")

SHEET_CONFIG = [
   {"sheet_name": "sheet111", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2026-02-03"},
    {"sheet_name": "sheet222", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2026-02-10"},
    {"sheet_name": "sheet333", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2026-02-17"},
    {"sheet_name": "sheet444", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2026-02-24"},
    {"sheet_name": "sheet555", "url": "https://www.moneycontrol.com/indices/fno/view-option-chain/BANKNIFTY/2026-02-24"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
    return session


def clean_number(x):
    """Convert strings with commas/currency to float, leave numbers as-is."""
    if isinstance(x, str):
        x = re.sub(r"[^\d.-]", "", x)
        try:
            return float(x)
        except:
            return 0
    if x is None or pd.isna(x):
        return 0
    return x


def fetch_option_chain_html(session, url):
    """Fetch option chain table from Moneycontrol using pandas.read_html"""
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    # ✅ Wrap HTML string to avoid deprecation warning
    tables = pd.read_html(StringIO(resp.text))
    if not tables:
        logger.warning(f"No tables found at {url}")
        return pd.DataFrame()

    df = tables[0]

    # ✅ Future-proof numeric cleaning
    try:
        df = df.map(clean_number)
    except Exception:
        df = df.applymap(clean_number)

    # Replace NaN/inf/-inf with 0
    df = df.replace([np.nan, np.inf, -np.inf], 0)

    logger.info(f"✅ Fetched {len(df)} rows from {url}")
    return df


def update_google_sheet(sheet_dfs):
    """Update Google Sheets with the cleaned DataFrames."""
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
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=str(len(df) + 10),
                    cols=str(len(df.columns) + 5)
                )

            worksheet.update([df.columns.astype(str).tolist()] + df.values.tolist())
            logger.info(f"Updated {sheet_name} with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to update {sheet_name}: {e}")


def is_market_open():
    """Check if market is open (IST)"""
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    current_time = now.time()
    current_date = now.date()
    market_start = time(9, 10)
    market_end = time(15, 31)
    return (current_date.weekday() < 5 or current_date.weekday() == 6) and market_start <= current_time <= market_end
    # return current_date.weekday() < 5 and market_start <= current_time <= market_end


IST = ZoneInfo("Asia/Kolkata")

def seconds_until_next_open():
    now = datetime.now(IST)
    market_open_time = datetime.combine(now.date(), time(9, 15), tzinfo=IST)

    if now > market_open_time:
        # Move to next day
        market_open_time += timedelta(days=1)

    return (market_open_time - now).total_seconds()


# ===== MAIN LOOP =====
if __name__ == "__main__":
    logger.info("Starting Option Chain Updater (Moneycontrol HTML)...")
    session = create_session()

    while True:
        if is_market_open():
            try:
                sheet_dfs = {}
                for cfg in SHEET_CONFIG:
                    df = fetch_option_chain_html(session, cfg["url"])
                    sheet_dfs[cfg["sheet_name"]] = df

                update_google_sheet(sheet_dfs)
                logger.info("✅ All sheets updated successfully!")
            except Exception as e:
                logger.error(f"Error during fetch-update cycle: {e}")

            sleep(60)  # Wait 60 seconds before next fetch

        else:
            secs = seconds_until_next_open()
            logger.info(f"📉 Market closed, sleeping for {int(secs/60)} minutes until next open.")
            sleep(secs)











































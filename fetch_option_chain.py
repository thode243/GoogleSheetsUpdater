import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NSE_API_URL = "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry=21-Aug-2025"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/option-chain",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive"
}

def fetch_option_chain():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Step 1: Visit NSE homepage
    session.get("https://www.nseindia.com/", headers=HEADERS, timeout=10)

    # Step 2: Visit the option chain page to get full cookies
    session.get("https://www.nseindia.com/option-chain", headers=HEADERS, timeout=10)

    # Step 3: Call the API
    resp = session.get(NSE_API_URL, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    option_data = data.get("records", {}).get("data", [])
    if not option_data:
        raise Exception("No option chain data found for this expiry")

    rows = []
    for entry in option_data:
        strike = entry.get("strikePrice")
        expiry = entry.get("expiryDate")
        ce = entry.get("CE", {})
        pe = entry.get("PE", {})
        rows.append({
            "Strike Price": strike,
            "Expiry Date": expiry,
            "CE OI": ce.get("openInterest"),
            "CE LTP": ce.get("lastPrice"),
            "PE OI": pe.get("openInterest"),
            "PE LTP": pe.get("lastPrice"),
        })

    return pd.DataFrame(rows)

if __name__ == "__main__":
    df = fetch_option_chain()
    print(df.head())

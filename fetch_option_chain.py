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

    # Step 1: NSE homepage
    session.get("https://www.nseindia.com/", headers=HEADERS, timeout=10)
    session.get("https://www.nseindia.com/option-chain", headers=HEADERS, timeout=10)

    # Step 2: Call API
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
            "CE OI": ce.get("openInterest"),
            "CE Chng OI": ce.get("changeinOpenInterest"),
            "CE Volume": ce.get("totalTradedVolume"),
            "CE IV": ce.get("impliedVolatility"),
            "CE LTP": ce.get("lastPrice"),
            "CE Chng LTP": ce.get("change"),
            "CE Bid Qty": ce.get("bidQty"),
            "CE Bid Price": ce.get("bidprice"),
            "CE Ask Price": ce.get("askPrice"),
            "CE Ask Qty": ce.get("askQty"),
            "Strike Price": strike,
            "Expiry Date": expiry,
            "PE Bid Qty": pe.get("bidQty"),
            "PE Bid Price": pe.get("bidprice"),
            "PE Ask Price": pe.get("askPrice"),
            "PE Ask Qty": pe.get("askQty"),
            "PE Chng LTP": pe.get("change"),
            "PE LTP": pe.get("lastPrice"),
            "PE IV": pe.get("impliedVolatility"),
            "PE Volume": pe.get("totalTradedVolume"),
            "PE Chng OI": pe.get("changeinOpenInterest"),
            "PE OI": pe.get("openInterest")
        })

    df = pd.DataFrame(rows)
    return df


if __name__ == "__main__":
    df = fetch_option_chain()
    print(df.head(10))

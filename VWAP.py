import requests
import pandas as pd
from datetime import datetime, timedelta
from functools import reduce

# -------------------------------
# NIFTY 50 Moneycontrol symbols
# -------------------------------
nifty50_symbols = [
    "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","HINDUNILVR","SBIN","KOTAKBANK",
    "LT","ITC","AXISBANK","BAJFINANCE","HDFC","MARUTI","ONGC","BHARTIARTL","ASIANPAINT",
    "TECHM","SUNPHARMA","WIPRO","ULTRACEMCO","NESTLEIND","POWERGRID","TITAN","HCLTECH",
    "JSWSTEEL","INDUSINDBK","TATASTEEL","BPCL","NTPC","M%26M","COALINDIA","DIVISLAB",
    "GRASIM","BAJAJFINSV","HDFCLIFE","EICHERMOT","TATAMOTORS","SBILIFE","ADANIPORTS",
    "BRITANNIA","HINDALCO","UPL","TATACONSUM","CIPLA","DRREDDY","IOC","VEDL"
]

# -------------------------------
# Fetch per-minute data and calculate VWAP
# -------------------------------
def fetch_vwap(symbol):
    now = int(pd.Timestamp.now().timestamp())
    from_time = now - 86400  # last 24 hours

    url = f"https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol={symbol}&resolution=1&from={from_time}&to={now}&countback=500&currencyCode=INR"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.moneycontrol.com/"
    }
    
    resp = requests.get(url, headers=headers)
    data = resp.json()

    if not data or "c" not in data or "v" not in data:
        print(f"⚠️ No data for {symbol}")
        return None

    cumulative_pv = 0
    cumulative_vol = 0
    rows = []

    for i in range(len(data['c'])):
        price = data['c'][i]
        volume = data['v'][i]
        timestamp = pd.to_datetime(data['t'][i], unit='s')

        cumulative_pv += price * volume
        cumulative_vol += volume
        vwap = cumulative_pv / cumulative_vol if cumulative_vol > 0 else price

        rows.append([timestamp, price, vwap])
    
    df = pd.DataFrame(rows, columns=[f"{symbol}_Time", f"{symbol}_Close", f"{symbol}_VWAP"])
    
    # Add LTP-VWAP difference
    df[f"{symbol}_Diff"] = df[f"{symbol}_Close"] - df[f"{symbol}_VWAP"]
    
    return df

# -------------------------------
# Fetch all NIFTY 50 stocks
# -------------------------------
all_data = []

for symbol in nifty50_symbols:
    print(f"Fetching {symbol}...")
    df = fetch_vwap(symbol)
    if df is not None:
        all_data.append(df)

# -------------------------------
# Merge all stocks on time
# -------------------------------
merged_df = reduce(lambda left, right: pd.merge(left, right, left_on=left.columns[0], right_on=right.columns[0], how='outer'), all_data)

# Remove duplicate timestamp columns after merge
merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

# Sort latest time on top
merged_df = merged_df.sort_values(by=merged_df.columns[0], ascending=False).reset_index(drop=True)

# -------------------------------
# Save to CSV
# -------------------------------
merged_df.to_csv("Nifty50_VWAP_withDiff.csv", index=False)
print("✅ All NIFTY 50 VWAP data with LTP-VWAP difference saved to Nifty50_VWAP_withDiff.csv")

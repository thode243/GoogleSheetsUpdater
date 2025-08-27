import os
import json
import gspread
from google.oauth2.service_account import Credentials

# --- Load Google credentials ---
creds_json = os.environ.get("GOOGLE_CREDENTIALS")

creds_info = None
if creds_json and creds_json.strip():
    try:
        creds_info = json.loads(creds_json)
    except json.JSONDecodeError:
        print("⚠️ GOOGLE_CREDENTIALS env var is set but not valid JSON")
else:
    # Fallback: read from file
    if os.path.exists("service_account.json"):
        with open("service_account.json") as f:
            creds_info = json.load(f)
    else:
        raise RuntimeError("❌ No GOOGLE_CREDENTIALS env var and no service_account.json file found")

# --- Authenticate with Google Sheets ---
creds = Credentials.from_service_account_info(
    creds_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)

# --- Get sheet ID from env (or default for local testing) ---
sheet_id = os.environ.get("SHEET_ID", "1JJ5nIJpCfX1lNHCiL3kmRXqEYeIFT2sQuXYbzNaia8U")

# Open spreadsheet
sh = gc.open_by_key(sheet_id)

# Example: read first worksheet
worksheet = sh.get_worksheet(0)
data = worksheet.get_all_values()

print(f"✅ Loaded {len(data)} rows from sheet '{worksheet.title}'")

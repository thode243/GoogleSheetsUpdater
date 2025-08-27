
import os
import json
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

#=== Load credentials from GitHub Codespaces secret =====
creds_json = os.environ["GOOGLE_CREDENTIALS"]   # secret in Codespaces
creds_dict = json.loads(creds_json)

# ===== Authorize with Google Sheets API =====
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# ===== Open spreadsheet by URL (recommended, less error-prone) =====
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1JJ5nIJpCfX1lNHCiL3kmRXqEYeIFT2sQuXYbzNaia8U/edit"  # <-- paste your sheet link here
spreadsheet = client.open_by_url(SPREADSHEET_URL)

# ===== Load all sheets into DataFrames =====
dfs = {}
for ws in spreadsheet.worksheets():
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    dfs[ws.title] = df   # key = sheet name

print("✅ Sheets loaded:", list(dfs.keys()))

# Example usage:
# print(dfs["Call OI"].head())
# print(dfs["Put OI"].head())

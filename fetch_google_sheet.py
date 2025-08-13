import requests
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

# URL of the NIFTY option chain
URL = "https://www.moneycontrol.com/indices/fno/view-option-chain/NIFTY/2025-08-14"

# Google Sheets setup
SHEET_ID = os.getenv("SHEET_ID")  # Use environment variable for Google Sheet ID
SHEET_NAME = "Sheet1"  # Name of the worksheet (adjust if needed)

def fetch_option_chain():
    # Send a GET request to the URL
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(URL, headers=headers)
    
    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: Status code {response.status_code}")
    
    # Parse the HTML content
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find all tables on the page
    tables = soup.find_all("table")
    if len(tables) < 2:
        raise Exception("Table not found on the page")
    
    # Extract the second table (index 1, as per IMPORTHTML "table", 2)
    table = tables[1]
    
    # Use pandas to read the HTML table
    df = pd.read_html(str(table))[0]
    
    return df

def update_google_sheet(df):
    # Set up Google Sheets API credentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    client = gspread.authorize(creds)
    
    # Open the Google Sheet by ID and select the worksheet
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    
    # Clear existing content
    sheet.clear()
    
    # Convert dataframe to a list of lists for Google Sheets
    data = [df.columns.values.tolist()] + df.values.tolist()
    
    # Update the sheet with new data
    sheet.update("A1", data)
    print(f"Updated Google Sheet at {datetime.now()}")

def main():
    try:
        # Fetch the option chain data
        df = fetch_option_chain()
        
        # Optional: Clean the dataframe (e.g., remove NaN values or unwanted columns)
        # df = df.dropna()  # Uncomment if cleaning is needed
        
        # Update the Google Sheet
        update_google_sheet(df)
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()

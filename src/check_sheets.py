"""Check what data is in Google Sheets."""
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import gspread

load_dotenv()

service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE", "secrets/service-account.json")
spreadsheet_name = os.environ.get("SPREADSHEET_NAME", "LinkedIn Jobs")

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
client = gspread.authorize(creds)
spreadsheet = client.open(spreadsheet_name)
worksheet = spreadsheet.sheet1

# Get all values
all_values = worksheet.get_all_values()

print(f"Total rows in sheet: {len(all_values)}")

if all_values:
    print(f"\nHeader row (row 1): {all_values[0]}")
    print(f"\nLast 5 rows of data:")
    for i, row in enumerate(all_values[-5:], start=len(all_values)-4):
        print(f"\nRow {i}:")
        for j, cell in enumerate(row):
            if cell:  # Only print non-empty cells
                print(f"  Col {j}: {cell[:80] if len(cell) > 80 else cell}")
            else:
                print(f"  Col {j}: (empty)")

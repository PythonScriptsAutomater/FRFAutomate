import os
import time
import requests
import json
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from oauth2client.service_account import ServiceAccountCredentials
from jotform import JotformAPIClient
from http.client import IncompleteRead
from requests.exceptions import ConnectionError as RequestsConnectionError
from gspread.exceptions import WorksheetNotFound
# ---------------- CONFIG ----------------
API_KEY          = os.environ['API_KEY']
FORM_ID          = os.environ['FORM_ID']
SPREADSHEET_NAME = os.environ['SPREADSHEET_NAME']
WORKSHEET_NAME   = os.environ['WORKSHEET_NAME']

CREDS_FILE          = os.environ.get('CREDS_FILE', 'credentials.json')
BASE_URL            = 'https://pw.jotform.com/API'
PAGE_SIZE           = 300
SLEEP_BETWEEN_CALLS = 1
MAX_PAGES           = 500
WRITE_BATCH_SIZE    = 500   # rows per Google Sheets API write call

# ---------------- GOOGLE SHEETS ----------------
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
client = gspread.authorize(creds)
spreadsheet = client.open(SPREADSHEET_NAME)
try:
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
except WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=10)

headers = ['Unique ID', 'Created at', 'Updated at', 'Approval Status']

# ---------------- RESUME LOGIC ----------------
existing_values = sheet.get_all_values()

if not existing_values:
    # Fresh sheet — write headers and start from scratch
    sheet.update([headers], 'A1')
    last_unique_id = None
    resume_start_date = START_DATE
    print(f"📋 Empty sheet — starting fresh from {START_DATE}")
else:
    # Sheet has data — find last row's Unique ID and Created at
    last_row = existing_values[-1]
    last_unique_id    = last_row[0] if len(last_row) > 0 else None
    last_created_at   = last_row[1] if len(last_row) > 1 else None

    # Use last created_at as the new start filter (fetch anything after it)
    resume_start_date = last_created_at if last_created_at else START_DATE
    print(f"🔁 Resuming — last Unique ID: '{last_unique_id}', last created_at: '{last_created_at}'")
    print(f"📅 Fetching submissions created after: {resume_start_date}")

# ---------------- HELPERS ----------------
def fetch_submissions(offset=0, limit=100, start_date=None):
    url = f"{BASE_URL}/form/{FORM_ID}/submissions"
    params = {
        'apiKey': API_KEY,
        'limit': limit,
        'offset': offset,
        'orderby[created_at]': 'asc',
        'addWorkflowStatus': 1,
        'filter': json.dumps({
            'created_at:gt': start_date or START_DATE
        })
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    if data.get('responseCode') != 200:
        raise Exception(f"Jotform API error: {data}")
    return data.get('content', [])

def extract_unique_id(answers):
    for _, meta in answers.items():
        if meta.get('name') == 'RequestId' or meta.get('text') == 'Request ID':
            return meta.get('answer', '')
    return ''

def append_with_retry(sheet, batch, retries=3):
    """Write a batch of rows to Google Sheets with retry on connection errors."""
    for attempt in range(retries):
        try:
            sheet.append_rows(batch, value_input_option='RAW')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️  Write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise

# ---------------- FETCH & WRITE (streaming batches) ----------------
rows_buffer   = []
total_written = 0
offset        = 0
page          = 0
skipped_first = False  # used to skip the duplicate of last_unique_id if created_at filter is inclusive

print("🚀 Fetching submissions...")

while page < MAX_PAGES:
    submissions = fetch_submissions(offset=offset, limit=PAGE_SIZE, start_date=resume_start_date)
    if not submissions:
        break

    for sub in submissions:
        answers          = sub.get('answers', {})
        approval_status  = sub.get('workflowStatus', '')
        unique_id        = extract_unique_id(answers)
        last_update_date = sub.get('updated_at', '')
        created_at       = sub.get('created_at', '')

        # Skip the row we already have (boundary record with same Unique ID)
        if not skipped_first and last_unique_id and unique_id == last_unique_id:
            skipped_first = True
            continue

        rows_buffer.append([
            unique_id,
            created_at,
            last_update_date,
            approval_status,
        ])

    # Flush buffer to Sheets whenever it reaches WRITE_BATCH_SIZE
    if len(rows_buffer) >= WRITE_BATCH_SIZE:
        append_with_retry(sheet, rows_buffer)
        total_written += len(rows_buffer)
        print(f"📝 Written {total_written} rows so far...")
        rows_buffer = []
        time.sleep(2)   # brief pause after each write

    offset += PAGE_SIZE
    page   += 1
    print(f"✔ Pulled {total_written + len(rows_buffer)} rows so far...")
    time.sleep(SLEEP_BETWEEN_CALLS)

# ---------------- FLUSH REMAINING ROWS ----------------
if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

if total_written == 0:
    print("✅ DONE — Sheet is already up to date, no new rows added.")
else:
    print(f"✅ DONE — Wrote {total_written} new rows to '{SPREADSHEET_NAME}' -> '{WORKSHEET_NAME}'")
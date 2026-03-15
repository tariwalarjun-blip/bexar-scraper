"""
Bexar County Foreclosure Scraper - Final
- Goes directly to results URL (no form clicking needed)
- Sorted by recorded date descending
- Pages through ALL results until hitting a known recorded date
- Splits address into Street, City, State, ZIP columns
- New address → writes to sheet
- Duplicate address → writes to sheet + texts you
"""

import os
import re
import time
import logging
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

load_dotenv()

CREDENTIALS_FILE   = os.environ["GOOGLE_SHEETS_CREDENTIALS"]
SHEET_ID           = os.environ.get("SHEET_ID", "1Z9l13Z62LuTJu2hP3ttlYJyWfK253eMvvtWQ5D0iLKo")
SHEET_TAB          = "Sheet1"
GMAIL_USER         = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
SMS_TO             = "7262412180@vtext.com"

BASE_URL = "https://bexar.tx.publicsearch.us/results?department=FC&limit=50&searchType=advancedSearch&sort=desc&sortBy=recordedDate&offset={offset}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

def send_text(message):
    try:
        msg = MIMEText(message)
        msg["From"]    = GMAIL_USER
        msg["To"]      = SMS_TO
        msg["Subject"] = ""
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, SMS_TO, msg.as_string())
        log.info(f"  Text sent: {message}")
    except Exception as e:
        log.warning(f"  Text failed: {e}")

def get_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_TAB)

def get_existing_data(sheet):
    rows = sheet.get_all_values()
    existing_addresses = set()
    existing_dates     = set()
    for row in rows[1:]:
        if row[0].strip():
            existing_addresses.add(row[0].strip().upper())
        if len(row) > 4 and row[4].strip():
            existing_dates.add(row[4].strip())
    return existing_addresses, existing_dates

def parse_address(full_address):
    parts  = [p.strip() for p in full_address.split(",")]
    street = parts[0] if len(parts) > 0 else full_address
    city   = parts[1] if len(parts) > 1 else ""
    state  = parts[2] if len(parts) > 2 else "TX"
    zip_   = parts[3] if len(parts) > 3 else ""
    state_map = {"TEXAS": "TX", "Texas": "TX"}
    state = state_map.get(state.strip(), state.strip())
    return street.strip(), city.strip(), state.strip(), zip_.strip()

def append_row(sheet, address, recorded_date, sale_date):
    street, city, state, zip_ = parse_address(address)
    sheet.append_row(
        [street, city, state, zip_, recorded_date, sale_date, "Active", ""],
        value_input_option="USER_ENTERED"
    )
    log.info(f"  Written: {street} | {city} | {state} | {zip_} | {recorded_date}")

def scrape_foreclosures(existing_dates):
    results = []
    done    = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page    = context.new_page()

        offset   = 0
        page_num = 1

        while not done:
            url = BASE_URL.format(offset=offset)
            log.info(f"Loading page {page_num} (offset={offset})...")
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(3)

            rows = page.locator("table tbody tr").all()
            log.info(f"  Rows on page: {len(rows)}")

            data_rows = 0
            for row in rows:
                try:
                    cells     = row.locator("td").all()
                    cell_text = [c.inner_text().strip() for c in cells]

                    dates = [v for v in cell_text if re.match(r"\d{1,2}/\d{1,2}/\d{4}", v)]
                    if not dates:
                        continue

                    data_rows += 1

                    address = ""
                    for val in reversed(cell_text):
                        if val and re.search(r"\d+\s+\w+", val):
                            address = val
                            break

                    recorded_date = dates[0]
                    sale_date     = dates[1] if len(dates) >= 2 else ""

                    if recorded_date in existing_dates:
                        log.info(f"  Hit known date {recorded_date} — stopping.")
                        done = True
                        break

                    if address:
                        results.append({
                            "address": address,
                            "recorded_date": recorded_date,
                            "sale_date": sale_date,
                        })

                except Exception as e:
                    log.warning(f"  Row error: {e}")

            log.info(f"  Data rows: {data_rows}")

            if done or data_rows == 0:
                break

            if data_rows < 50:
                log.info("  Last page reached.")
                break

            offset   += 50
            page_num += 1

        browser.close()
    return results

def main():
    log.info("=" * 60)
    log.info("Bexar County Foreclosure Scraper")
    log.info("=" * 60)
    sheet                              = get_sheet()
    existing_addresses, existing_dates = get_existing_data(sheet)
    log.info(f"Existing records: {len(existing_addresses)} | Known dates: {len(existing_dates)}")

    foreclosures = scrape_foreclosures(existing_dates)
    log.info(f"New foreclosures found: {len(foreclosures)}")

    new_count  = 0
    dupe_count = 0
    for f in foreclosures:
        street, city, state, zip_ = parse_address(f["address"])
        key = street.strip().upper()
        if not key:
            continue

        is_dupe = key in existing_addresses
        append_row(sheet, f["address"], f["recorded_date"], f["sale_date"])
        existing_addresses.add(key)
        existing_dates.add(f["recorded_date"])
        new_count += 1

        if is_dupe:
            dupe_count += 1
            send_text(f"REFILE: {f['address']} recorded again {f['recorded_date']}. Sale: {f['sale_date']}")

        time.sleep(1.5)

    log.info(f"Done. {new_count} records written. {dupe_count} refile alerts sent.")

if __name__ == "__main__":
    main()

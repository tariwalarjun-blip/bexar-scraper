"""
Bexar County Foreclosure Scraper - v10
- Smart stop, duplicate handling, address normalization, retry logic, crash SMS
- 3 second sleep between writes to avoid Google Sheets rate limit
- v10: Extracts substitute trustee from each property detail page (direct URL nav, no click/back)
       Trustee written to Sheet1 column J (col 10)
"""

import os
import re
import time
import logging
import smtplib
import json
import urllib.request
from datetime import datetime, timedelta
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
MAX_DAYS_BACK  = 30
DETAIL_BASE    = "https://bexar.tx.publicsearch.us"

# Known substitute trustee companies operating in Bexar County
KNOWN_TRUSTEES = [
    "agency sales & postings",
    "aldridge pite llp",
    "america west lender services",
    "assured lender services inc",
    "avt title services",
    "barrett daffin fapper turner & engel llp",
    "birdlaw pllc",
    "bonial & associates pc",
    "brock & scott",
    "malcolm cisneros",
    "trustee corps",
    "c/o the mortgage law firm",
    "codilis & moody",
    "davis & santos pllc",
    "de cubas & lewis",
    "entra default solutions llc",
    "foreclosure services llc",
    "ghidotti berger llp",
    "home & associates",
    "hughes watters & askanase",
    "mackie wolf zientz & mann",
    "marinosic law group",
    "mccalla raymer",
    "mccarthy",
    "miller george & suggs",
    "nestor solutions",
    "padgett law group",
    "power default services",
    "prestige default services",
    "robertson anschutz schneid crane & partners",
    "settlepou",
    "shapiro schwartz llp",
    "taherzadeh pllc",
    "tejas corporate services",
    "thurman & phillips pc",
    "tromberg miller morris & partners pllc",
    "upton mickits & heymann llp",
    "vylla solutions",
    "west & west",
]

BASE_URL = "https://bexar.tx.publicsearch.us/results?department=FC&limit=50&searchType=advancedSearch&sort=desc&sortBy=recordedDate&offset={offset}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)




# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

def parse_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except Exception:
        return None

def send_text(message):
    try:
        msg = MIMEText(message)
        msg["From"]    = GMAIL_USER
        msg["To"]      = SMS_TO
        msg["Subject"] = ""
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, SMS_TO, msg.as_string())
        log.info(f"  Text sent: {message[:80]}")
    except Exception as e:
        log.warning(f"  Text failed: {e}")


def goto_with_retry(page, url, retries=3, delay=5):
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, wait_until="networkidle", timeout=60000)
            return True
        except Exception as e:
            log.warning(f"  Page load attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    return False


def match_known_trustee(text):
    """Try to match text against the known trustee list."""
    text_lower = text.lower()
    for trustee in KNOWN_TRUSTEES:
        if trustee in text_lower:
            return trustee.title()
    return ""

def get_substitute_trustee(page, doc_number, results_url):
    """
    Navigate to the document detail page using the doc number,
    extract the substitute trustee from the document text.
    Returns trustee name or "" if not found.
    """
    trustee = ""
    detail_url = f"{DETAIL_BASE}/doc/{doc_number}"
    try:
        log.info(f"  Loading detail: {detail_url}")
        if not goto_with_retry(page, detail_url):
            log.warning(f"  Could not load detail page: {detail_url}")
            return ""
        time.sleep(3)

        # Get full page text - document viewer renders text in the DOM
        body = page.inner_text("body")
        log.info(f"  Detail preview: {body[:300].replace(chr(10), ' | ')}")

        # Strategy 1: look for "SUBSTITUTE TRUSTEE" pattern in document text
        match = re.search(
            r'SUBSTITUTE TRUSTEE[S)(\s]*[:\-]?\s*\n?\s*([A-Za-z][A-Za-z0-9\s,\.\&]+?)(?:\n|Attorney|LLC|PC|PLLC|LLP|Corp|Inc)',
            body, re.IGNORECASE
        )
        if match:
            candidate = match.group(1).strip().rstrip(",")
            if len(candidate) > 3:
                trustee = candidate
                log.info(f"  Trustee (pattern): {trustee}")

        # Strategy 2: match against known trustee list
        if not trustee:
            trustee = match_known_trustee(body)
            if trustee:
                log.info(f"  Trustee (known list): {trustee}")

        if not trustee:
            log.info("  Trustee: not found")

    except Exception as e:
        log.warning(f"  Trustee extraction error: {e}")
    finally:
        try:
            goto_with_retry(page, results_url)
            time.sleep(2)
        except Exception as e:
            log.warning(f"  Failed to navigate back: {e}")

    return trustee


# ─────────────────────────────────────────────
# Google Sheets
# ─────────────────────────────────────────────

def get_sheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    wb = gspread.authorize(creds).open_by_key(SHEET_ID)
    return wb.worksheet(SHEET_TAB)

def get_existing_data(sheet):
    rows = sheet.get_all_values()
    existing_addresses = {}
    all_dates = []

    for i, row in enumerate(rows[1:], start=2):
        street = row[0].strip() if len(row) > 0 else ""
        if street:
            existing_addresses[street.upper()] = i
        if len(row) > 4 and row[4].strip():
            d = parse_date(row[4])
            if d:
                all_dates.append(d)

    most_recent_date = max(all_dates) if all_dates else None
    return existing_addresses, most_recent_date


def parse_address(full_address):
    parts  = [p.strip() for p in full_address.split(",")]
    street = parts[0] if parts else full_address
    city   = parts[1] if len(parts) > 1 else ""
    state  = parts[2] if len(parts) > 2 else "TX"
    zip_   = parts[3] if len(parts) > 3 else ""
    state_map = {"TEXAS": "TX", "Texas": "TX"}
    state = state_map.get(state.strip(), state.strip())
    return street.strip().upper(), city.strip(), state.strip(), zip_.strip()

def append_row(sheet, address, recorded_date, sale_date, trustee=""):
    street, city, state, zip_ = parse_address(address)
    # Col:  A       B     C      D     E              F          G   H         I   J
    sheet.append_row(
        [street, city, state, zip_, recorded_date, sale_date, "", "Active", "", trustee],
        value_input_option="USER_ENTERED",
    )
    log.info(f"  New row: {street} | {recorded_date} | trustee={trustee or 'N/A'}")
    time.sleep(2)

def update_row(sheet, row_index, address, recorded_date, sale_date, trustee=""):
    sheet.update_cell(row_index, 5, recorded_date)
    time.sleep(2)
    sheet.update_cell(row_index, 6, sale_date)
    time.sleep(2)
    if trustee:
        sheet.update_cell(row_index, 10, trustee)
        time.sleep(2)
    log.info(f"  Updated row {row_index}: {recorded_date} / {sale_date} | trustee={trustee or 'N/A'}")


# ─────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────

def scrape_page_with_retry(page, url, max_retries=3):
    for attempt in range(1, max_retries + 1):
        if not goto_with_retry(page, url):
            return []
        time.sleep(3)
        rows = page.locator("table tbody tr").all()
        data_rows = [r for r in rows if re.search(r"\d{1,2}/\d{1,2}/\d{4}", r.inner_text())]
        if data_rows:
            return rows
        log.warning(f"  0 data rows on attempt {attempt}/{max_retries} — retrying in 10s...")
        time.sleep(10)
    log.error("  Page returned 0 rows after all retries.")
    return []


def scrape_foreclosures(most_recent_date):
    results = []
    done    = False

    hard_cutoff = datetime.now() - timedelta(days=MAX_DAYS_BACK)
    stop_date   = max(most_recent_date, hard_cutoff) if most_recent_date else hard_cutoff
    log.info(f"Stop date: {stop_date.strftime('%m/%d/%Y')}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page    = context.new_page()

        offset   = 0
        page_num = 1

        while not done:
            results_url = BASE_URL.format(offset=offset)
            log.info(f"Loading page {page_num} (offset={offset})…")

            rows = scrape_page_with_retry(page, results_url)
            if not rows:
                log.error(f"  Giving up on page {page_num} after retries.")
                break

            # Collect all row data + detail URLs before navigating away
            page_records = []
            data_rows = 0
            for row in rows:
                try:
                    cells     = row.locator("td").all()
                    cell_text = [c.inner_text().strip() for c in cells]

                    dates = [v for v in cell_text if re.match(r"\d{1,2}/\d{1,2}/\d{4}", v)]
                    if not dates:
                        continue

                    data_rows    += 1
                    recorded_date = dates[0]
                    sale_date     = dates[1] if len(dates) >= 2 else ""

                    rec_dt = parse_date(recorded_date)
                    if rec_dt and rec_dt < stop_date:
                        log.info(f"  {recorded_date} < stop date — stopping.")
                        done = True
                        break

                    address = ""
                    for val in reversed(cell_text):
                        if val and re.search(r"\d+\s+\w+", val):
                            address = val
                            break

                    # Doc number is a long numeric string in the row cells
                    doc_number = ""
                    for val in cell_text:
                        if re.match(r"^\d{10,}$", val.strip()):
                            doc_number = val.strip()
                            break

                    if address:
                        page_records.append({
                            "address":       address,
                            "recorded_date": recorded_date,
                            "sale_date":     sale_date,
                            "doc_number":    doc_number,
                        })

                except Exception as e:
                    log.warning(f"  Row error: {e}")

            log.info(f"  Data rows this page: {data_rows}")

            # Now visit each detail page to get trustee
            for rec in page_records:
                trustee = ""
                if rec.get("doc_number"):
                    trustee = get_substitute_trustee(page, rec["doc_number"], results_url)
                rec["trustee"] = trustee
                results.append(rec)

            if done or data_rows == 0:
                break
            if data_rows < 50:
                log.info("  Last page reached.")
                break

            offset   += 50
            page_num += 1

        browser.close()

    return results


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Bexar County Foreclosure Scraper v10")
    log.info("=" * 60)

    try:
        sheet = get_sheets()
        existing_addresses, most_recent = get_existing_data(sheet)
        log.info(
            f"Existing records: {len(existing_addresses)} | "
            f"Most recent date: {most_recent.strftime('%m/%d/%Y') if most_recent else 'none'}"
        )

        foreclosures = scrape_foreclosures(most_recent)
        log.info(f"Records to process: {len(foreclosures)}")

        new_count    = 0
        update_count = 0

        for f in foreclosures:
            street, _, _, _ = parse_address(f["address"])
            key = street.strip().upper()
            if not key:
                continue

            row_index = existing_addresses.get(key)
            trustee   = f.get("trustee", "")

            if row_index is not None:
                update_row(sheet, row_index, f["address"], f["recorded_date"], f["sale_date"], trustee)
                update_count += 1
            else:
                append_row(sheet, f["address"], f["recorded_date"], f["sale_date"], trustee)
                new_count += 1

            time.sleep(3)

        if new_count == 0 and update_count == 0:
            send_text("⚠️ Bexar scraper ran but found 0 records. Check county site manually.")

        log.info(f"Done. {new_count} new | {update_count} updated.")

    except Exception as e:
        log.error(f"SCRAPER CRASHED: {e}")
        send_text(f"Bexar scraper crashed: {str(e)[:120]}")
        raise


if __name__ == "__main__":
    main()

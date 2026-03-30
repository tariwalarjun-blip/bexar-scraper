"""
Bexar County Foreclosure Scraper - v13
- Generates a unique BEXAR-ID for every new lead
- Writes BEXAR-ID to sheet column J at the same time as the row
- Sends BEXAR-ID in webhook payload so REsimpli stores it as a custom question
- DK status Zap looks up sheet rows by BEXAR-ID — 100% reliable, no address matching
- Refiling SMS alert for existing addresses with new dates
- Webhook retry logic (3 attempts) so no leads are silently dropped
"""

import os
import re
import time
import logging
import smtplib
import json
import urllib.request
import urllib.error
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
ZAPIER_WEBHOOK_URL = os.environ.get("ZAPIER_WEBHOOK_URL", "")
SMS_TO             = "7262412180@vtext.com"
MAX_DAYS_BACK      = 30

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


def parse_address(full_address):
    parts  = [p.strip() for p in full_address.split(",")]
    street = parts[0] if parts else full_address
    city   = parts[1] if len(parts) > 1 else ""
    state  = parts[2] if len(parts) > 2 else "TX"
    zip_   = parts[3] if len(parts) > 3 else ""
    state_map = {"TEXAS": "TX", "Texas": "TX"}
    state = state_map.get(state.strip(), state.strip())
    return street.strip().upper(), city.strip(), state.strip(), zip_.strip()


def generate_bexar_id(street, recorded_date):
    """
    Generate a unique ID for a lead using street + recorded date.
    Format: BEXAR-YYYYMMDD-STREETKEY
    Example: BEXAR-20260326-402PRESTWICK
    """
    date_part = recorded_date.replace("/", "")
    # Convert MM/DD/YYYY to YYYYMMDD
    try:
        d = datetime.strptime(recorded_date, "%m/%d/%Y")
        date_part = d.strftime("%Y%m%d")
    except Exception:
        date_part = recorded_date.replace("/", "")
    street_key = re.sub(r"[^A-Z0-9]", "", street.upper())[:12]
    return f"BEXAR-{date_part}-{street_key}"


def fire_zapier_webhook(address, recorded_date, sale_date, bexar_id, retries=3, delay=5):
    if not ZAPIER_WEBHOOK_URL:
        log.warning("  ZAPIER_WEBHOOK_URL not set — skipping webhook fire.")
        return False

    street, city, state, zip_ = parse_address(address)
    full_address = f"{street}, {city}, {state} {zip_}".strip(", ")

    payload = json.dumps({
        "address":       full_address,
        "street":        street,
        "city":          city,
        "state":         state,
        "zip":           zip_,
        "recorded_date": recorded_date,
        "sale_date":     sale_date,
        "lead_source":   "Foreclosure Auction",
        "bexar_id":      bexar_id,
    }).encode("utf-8")

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                ZAPIER_WEBHOOK_URL,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                status = resp.getcode()
                if status == 200:
                    log.info(f"  Webhook fired OK: {street}")
                    return True
                else:
                    log.warning(f"  Webhook attempt {attempt} returned status {status}")
        except urllib.error.URLError as e:
            log.warning(f"  Webhook attempt {attempt}/{retries} failed: {e}")
        if attempt < retries:
            time.sleep(delay)

    send_text(f"⚠️ Webhook failed after {retries} attempts for: {street} | {recorded_date}")
    return False



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


def append_row(sheet, address, recorded_date, sale_date, bexar_id):
    street, city, state, zip_ = parse_address(address)
    sheet.append_row(
        [street, city, state, zip_, recorded_date, sale_date, "", "Active", "", bexar_id],
        value_input_option="USER_ENTERED",
    )
    log.info(f"  New row: {street} | {recorded_date} | {bexar_id}")
    time.sleep(2)


def update_row(sheet, row_index, recorded_date, sale_date):
    sheet.update_cell(row_index, 5, recorded_date)
    time.sleep(2)
    sheet.update_cell(row_index, 6, sale_date)
    time.sleep(2)
    log.info(f"  Updated row {row_index}: {recorded_date} / {sale_date}")



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
            log.info(f"Loading page {page_num} (offset={offset})...")

            rows = scrape_page_with_retry(page, results_url)
            if not rows:
                log.error(f"  Giving up on page {page_num} after retries.")
                break

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

                    if address:
                        results.append({
                            "address":       address,
                            "recorded_date": recorded_date,
                            "sale_date":     sale_date,
                        })

                except Exception as e:
                    log.warning(f"  Row error: {e}")

            log.info(f"  Data rows this page: {data_rows}")

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
    log.info("Bexar County Foreclosure Scraper v12")
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

        new_count      = 0
        update_count   = 0
        webhook_failed = 0

        for f in foreclosures:
            street, _, _, _ = parse_address(f["address"])
            key = street.strip().upper()
            if not key:
                continue

            row_index = existing_addresses.get(key)

            if row_index is not None:
                # REFILING — update sheet dates, send SMS alert, do NOT fire webhook
                update_row(sheet, row_index, f["recorded_date"], f["sale_date"])
                update_count += 1
                send_text(
                    f"Refiled: {street} | "
                    f"Recorded: {f['recorded_date']} | "
                    f"Auction: {f['sale_date']}"
                )
            else:
                # NEW LEAD — generate ID, write to sheet, fire webhook
                bexar_id = generate_bexar_id(street, f["recorded_date"])
                append_row(sheet, f["address"], f["recorded_date"], f["sale_date"], bexar_id)
                new_count += 1
                success = fire_zapier_webhook(f["address"], f["recorded_date"], f["sale_date"], bexar_id)
                if not success:
                    webhook_failed += 1

            time.sleep(3)

        # Summary SMS
        if new_count == 0 and update_count == 0:
            send_text("Bexar scraper ran but found 0 records. Check county site manually.")
        else:
            summary = f"Scraper done. {new_count} new leads to REsimpli | {update_count} refilings"
            if webhook_failed > 0:
                summary += f" | {webhook_failed} webhook(s) failed - check leads manually"
            send_text(summary)

        log.info(f"Done. {new_count} new | {update_count} updated | {webhook_failed} webhook failures.")

    except Exception as e:
        log.error(f"SCRAPER CRASHED: {e}")
        send_text(f"Bexar scraper crashed: {str(e)[:120]}")
        raise


if __name__ == "__main__":
    main()

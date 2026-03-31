"""
Bexar County Foreclosure Scraper - v14
- All v13 features retained
- NEW: Bexar CAD lookup for every new address
  - Searches by house number + stripped street name
  - Reads owner name from results table
  - If LLC/CORP/INC/TRUST/LP/LTD → skips entirely, nothing created
  - Clicks into detail page to get mailing address, appraised value, last sale date
- Owner data written to sheet columns K-N and included in webhook payload
- Sheet columns: A=Street, B=City, C=State, D=Zip, E=Recorded Date, F=Sale Date,
  G=(empty), H=Status, I=DK Status, J=BEXAR ID,
  K=Owner Name, L=Mailing Address, M=Appraised Value, N=Last Sale Date
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

CAD_SEARCH_URL = "https://bexar.trueautomation.com/clientdb/?cid=110"
CAD_BASE_URL   = "https://bexar.trueautomation.com"

# Words to strip from street address before CAD search
STREET_SUFFIXES = {
    "ST", "STREET", "DR", "DRIVE", "RD", "ROAD", "LN", "LANE",
    "BLVD", "BOULEVARD", "AVE", "AVENUE", "CT", "COURT", "CIR",
    "CIRCLE", "WAY", "TRL", "TRAIL", "PKWY", "PARKWAY", "PL",
    "PLACE", "LOOP", "PASS", "PATH", "RUN", "CV", "COVE", "XING",
    "CROSSING", "HWY", "HIGHWAY", "LACE", "RIDGE", "GLEN", "PARK",
    "BEND", "CREEK", "HILLS", "VIEW", "WOOD", "WOODS", "MEADOW",
    "MEADOWS", "VALLEY", "HOLLOW", "HOLW", "GROVE", "TRACE",
}

# Owner name fragments that indicate LLC/corporate ownership
LLC_KEYWORDS = {
    "LLC", "INC", "CORP", "CORPORATION", "LP", "LTD", "LIMITED",
    "TRUST", "PROPERTIES", "HOLDINGS", "INVESTMENTS", "REALTY",
    "REAL ESTATE", "PARTNERS", "GROUP", "ENTERPRISES", "VENTURES",
    "FUND", "CAPITAL", "ASSETS", "MANAGEMENT",
}

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


def strip_street_suffix(street):
    """
    Strip directional prefixes and street suffixes for CAD search.
    '5715 TIANNA LACE'  → '5715 TIANNA'
    '402 PRESTWICK ST'  → '402 PRESTWICK'
    '203 E LAMBERT ST'  → '203 LAMBERT'  (E stripped from middle)
    """
    DIRECTIONALS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW",
                    "NORTH", "SOUTH", "EAST", "WEST"}
    tokens = street.upper().split()

    # Remove trailing suffix words
    while tokens and tokens[-1] in STREET_SUFFIXES:
        tokens = tokens[:-1]

    # Remove directional tokens anywhere except position 0 (house number)
    # Keep house number (tokens[0]) always
    if len(tokens) > 1:
        filtered = [tokens[0]] + [t for t in tokens[1:] if t not in DIRECTIONALS]
        tokens = filtered

    return " ".join(tokens)


def is_llc_owner(owner_name):
    """Return True if the owner name looks like an LLC or corporate entity."""
    if not owner_name:
        return False
    upper = owner_name.upper()
    for kw in LLC_KEYWORDS:
        if kw in upper:
            return True
    return False


def generate_bexar_id(street, recorded_date):
    try:
        d = datetime.strptime(recorded_date, "%m/%d/%Y")
        date_part = d.strftime("%Y%m%d")
    except Exception:
        date_part = recorded_date.replace("/", "")
    street_key = re.sub(r"[^A-Z0-9]", "", street.upper())[:12]
    return f"BEXAR-{date_part}-{street_key}"


def fire_zapier_webhook(address, recorded_date, sale_date, bexar_id,
                        owner_name="", mailing_address="", appraised_value="", last_sale_date="",
                        retries=3, delay=5):
    if not ZAPIER_WEBHOOK_URL:
        log.warning("  ZAPIER_WEBHOOK_URL not set — skipping webhook fire.")
        return False

    street, city, state, zip_ = parse_address(address)
    full_address = f"{street}, {city}, {state} {zip_}".strip(", ")

    payload = json.dumps({
        "address":         full_address,
        "street":          street,
        "city":            city,
        "state":           state,
        "zip":             zip_,
        "recorded_date":   recorded_date,
        "sale_date":       sale_date,
        "lead_source":     "Foreclosure Auction",
        "bexar_id":        bexar_id,
        "owner_name":      owner_name,
        "mailing_address": mailing_address,
        "appraised_value": appraised_value,
        "last_sale_date":  last_sale_date,
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
# Bexar CAD Lookup
# ─────────────────────────────────────────────

def cad_lookup(page, street):
    """
    Search Bexar CAD for a street address.
    Returns dict with owner_name, mailing_address, appraised_value, last_sale_date
    Returns None if LLC/corporate owner (should be skipped)
    Returns {} if no match found (proceed without CAD data)
    """
    search_term = strip_street_suffix(street)
    if not search_term:
        log.warning(f"  CAD: Could not build search term from '{street}'")
        return {}

    log.info(f"  CAD lookup: '{search_term}'")

    try:
        # Load CAD search page
        if not goto_with_retry(page, CAD_SEARCH_URL):
            log.warning("  CAD: Could not load search page")
            return {}

        # Type search term and submit
        # Try multiple possible input selectors
        for selector in ["input[name='PropertySearch']", "input[type='text']", "#PropertySearch", "input.search"]:
            try:
                search_box = page.locator(selector).first
                search_box.wait_for(timeout=10000)
                search_box.fill(search_term)
                page.keyboard.press("Enter")
                page.wait_for_load_state("networkidle", timeout=30000)
                time.sleep(2)
                break
            except Exception:
                continue
        else:
            log.warning(f"  CAD: Could not find search input for '{search_term}'")
            return {}

        # Check results count
        results_text = page.locator("body").inner_text()
        if "0 of 0" in results_text or "no results" in results_text.lower():
            log.info(f"  CAD: No results for '{search_term}'")
            return {}

        # Get all result rows from the table
        rows = page.locator("table tr").all()
        matched_row = None
        house_num = street.split()[0] if street.split() else ""

        for row in rows:
            cells = row.locator("td").all()
            if len(cells) < 3:
                continue
            try:
                # Table columns: checkbox(0), PropID(1), GeoID(2), Type(3), Address(4), Owner(5)
                if len(cells) < 5:
                    continue
                address_cell = cells[4].inner_text().strip().upper()
                owner_cell   = cells[5].inner_text().strip().upper() if len(cells) > 5 else ""

                # Match by house number at start of address
                if address_cell.startswith(house_num + " ") or address_cell.startswith(house_num + ","):
                    # Check LLC right here in results table — no need to click in
                    if is_llc_owner(owner_cell):
                        log.info(f"  CAD: LLC owner detected '{owner_cell}' — skipping lead")
                        return None  # None = skip this lead

                    matched_row = row
                    matched_owner = owner_cell
                    break
            except Exception:
                continue

        if not matched_row:
            log.info(f"  CAD: No address match for house number {house_num}")
            return {}

        # Click View Details to get full data
        detail_link = matched_row.locator("a:has-text('View Details')")
        if detail_link.count() == 0:
            log.warning("  CAD: No View Details link found")
            return {}

        detail_link.first.click()
        page.wait_for_load_state("networkidle", timeout=30000)
        time.sleep(2)

        detail_text = page.locator("body").inner_text()

        # Extract owner name — stop before "Owner ID:"
        owner_name = ""
        owner_match = re.search(r"Name:\s*(.+?)(?:\s{2,}Owner ID:|\n)", detail_text)
        if owner_match:
            owner_name = owner_match.group(1).strip()

        # Extract mailing address — first line stops before "% Ownership"
        # Second line is the city/state/zip
        mailing_address = ""
        mail_match = re.search(
            r"Mailing Address:\s*(.+?)(?:\s+%\s+Ownership:|\n).*?\n\s*([A-Z][^\n]+,\s*TX\s+\d{5})",
            detail_text, re.DOTALL
        )
        if mail_match:
            line1 = mail_match.group(1).strip()
            line2 = mail_match.group(2).strip()
            mailing_address = f"{line1}, {line2}"
        else:
            # Fallback: just grab first address-looking line after Mailing Address
            mail_simple = re.search(r"Mailing Address:\s*([^\n%]+)", detail_text)
            if mail_simple:
                mailing_address = mail_simple.group(1).strip()

        # Extract appraised value — look for dollar amount near "Appraised"
        appraised_value = ""
        appr_match = re.search(r"Appraised[^\n]*?\$?\s*([\d,]+)", detail_text)
        if not appr_match:
            # Try finding any dollar value in the Values section
            appr_match = re.search(r"Total[^\n]*?\$?\s*([\d,]+)", detail_text)
        if appr_match:
            appraised_value = appr_match.group(1).replace(",", "").strip()

        # Extract last sale date — look for Deed History table
        last_sale_date = ""
        try:
            # Try multiple selectors for the deed history table
            deed_selectors = [
                "table:has(th:has-text('Deed Date')) tbody tr",
                "table tr:has(td:nth-child(1)):has(td:nth-child(2))",
            ]
            for sel in deed_selectors:
                deed_rows = page.locator(sel).all()
                if deed_rows:
                    first_cells = deed_rows[0].locator("td").all()
                    if first_cells:
                        candidate = first_cells[0].inner_text().strip()
                        # Validate it looks like a date
                        if re.match(r"\d{1,2}/\d{1,2}/\d{4}", candidate):
                            last_sale_date = candidate
                            break
            # Fallback: regex on full text for dates in deed history section
            if not last_sale_date:
                deed_section = re.search(r"Deed History.{0,500}?(\d{1,2}/\d{1,2}/\d{4})", detail_text, re.DOTALL)
                if deed_section:
                    last_sale_date = deed_section.group(1)
        except Exception as de:
            log.warning(f"  CAD: deed date extraction failed: {de}")

        log.info(f"  CAD: {owner_name} | {mailing_address} | ${appraised_value} | Last sale: {last_sale_date}")

        return {
            "owner_name":      owner_name,
            "mailing_address": mailing_address,
            "appraised_value": appraised_value,
            "last_sale_date":  last_sale_date,
        }

    except Exception as e:
        log.warning(f"  CAD lookup failed for '{street}': {e}")
        return {}


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


def append_row(sheet, address, recorded_date, sale_date, bexar_id, cad_data):
    """
    Write new row with all data.
    Columns: A=Street, B=City, C=State, D=Zip, E=Recorded Date, F=Sale Date,
             G=(empty), H=Status, I=DK Status, J=BEXAR ID,
             K=Owner Name, L=Mailing Address, M=Appraised Value, N=Last Sale Date
    """
    street, city, state, zip_ = parse_address(address)
    row = [
        street,                              # A
        city,                                # B
        state,                               # C
        zip_,                                # D
        recorded_date,                       # E
        sale_date,                           # F
        "",                                  # G
        "Active",                            # H
        "",                                  # I (DK Status)
        bexar_id,                            # J
        cad_data.get("owner_name", ""),      # K
        cad_data.get("mailing_address", ""), # L
        cad_data.get("appraised_value", ""), # M
        cad_data.get("last_sale_date", ""),  # N
    ]
    sheet.append_row(row, value_input_option="USER_ENTERED")
    log.info(f"  New row: {street} | {recorded_date} | {bexar_id} | {cad_data.get('owner_name', 'no owner')}")
    time.sleep(2)


def update_row(sheet, row_index, recorded_date, sale_date):
    sheet.update_cell(row_index, 5, recorded_date)
    time.sleep(2)
    sheet.update_cell(row_index, 6, sale_date)
    time.sleep(2)
    log.info(f"  Updated row {row_index}: {recorded_date} / {sale_date}")


# ─────────────────────────────────────────────
# Foreclosure Scraper
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
    log.info("Bexar County Foreclosure Scraper v14")
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
        skipped_llc    = 0
        webhook_failed = 0

        # Single browser session for all CAD lookups
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={"width": 1280, "height": 900})
            cad_page = context.new_page()

            for f in foreclosures:
                street, _, _, _ = parse_address(f["address"])
                key = street.strip().upper()
                if not key:
                    continue

                row_index = existing_addresses.get(key)

                if row_index is not None:
                    # REFILING — update sheet dates, send SMS alert
                    update_row(sheet, row_index, f["recorded_date"], f["sale_date"])
                    update_count += 1
                    send_text(
                        f"Refiled: {street} | "
                        f"Recorded: {f['recorded_date']} | "
                        f"Auction: {f['sale_date']}"
                    )
                else:
                    # NEW LEAD — CAD lookup first
                    cad_data = cad_lookup(cad_page, street)

                    if cad_data is None:
                        # LLC owner — skip entirely
                        skipped_llc += 1
                        log.info(f"  Skipped LLC: {street}")
                        time.sleep(1)
                        continue

                    # Individual owner — proceed
                    bexar_id = generate_bexar_id(street, f["recorded_date"])
                    append_row(sheet, f["address"], f["recorded_date"], f["sale_date"], bexar_id, cad_data)
                    new_count += 1

                    success = fire_zapier_webhook(
                        f["address"], f["recorded_date"], f["sale_date"], bexar_id,
                        owner_name      = cad_data.get("owner_name", ""),
                        mailing_address = cad_data.get("mailing_address", ""),
                        appraised_value = cad_data.get("appraised_value", ""),
                        last_sale_date  = cad_data.get("last_sale_date", ""),
                    )
                    if not success:
                        webhook_failed += 1

                time.sleep(3)

            browser.close()

        # Summary SMS
        if new_count == 0 and update_count == 0 and skipped_llc == 0:
            send_text("Bexar scraper ran but found 0 records. Check county site manually.")
        else:
            summary = (
                f"Scraper done. {new_count} new | "
                f"{update_count} refilings | "
                f"{skipped_llc} LLC skipped"
            )
            if webhook_failed > 0:
                summary += f" | {webhook_failed} webhook(s) failed"
            send_text(summary)

        log.info(f"Done. {new_count} new | {update_count} updated | {skipped_llc} LLC skipped | {webhook_failed} webhook failures.")

    except Exception as e:
        log.error(f"SCRAPER CRASHED: {e}")
        send_text(f"Bexar scraper crashed: {str(e)[:120]}")
        raise


if __name__ == "__main__":
    main()

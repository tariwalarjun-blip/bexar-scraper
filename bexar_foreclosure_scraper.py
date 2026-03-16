"""
Bexar County Foreclosure Scraper - v4
- Smart stop: stops when recorded date is OLDER THAN most recent date in sheet
- Duplicate handling: updates existing row with new dates instead of adding new row
- Address normalization: converts street suffixes to USPS abbreviations to match BatchLeads
- Substitute trustee extraction via Claude API
- Retry logic on page loads
- Crash notification via SMS
"""

import os
import re
import time
import logging
import smtplib
import base64
from datetime import datetime
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
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
SMS_TO             = "7262412180@vtext.com"

BASE_URL = "https://bexar.tx.publicsearch.us/results?department=FC&limit=50&searchType=advancedSearch&sort=desc&sortBy=recordedDate&offset={offset}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SUFFIX_MAP = {
    "ALLEY": "ALY", "AVENUE": "AVE", "BEND": "BND", "BLUFF": "BLF",
    "BOULEVARD": "BLVD", "BRANCH": "BR", "BRIDGE": "BRG", "BROOK": "BRK",
    "CANYON": "CYN", "CIRCLE": "CIR", "CLIFF": "CLF", "CLUB": "CLB",
    "COMMON": "CMN", "CORNER": "COR", "COURT": "CT", "COVE": "CV",
    "CREEK": "CRK", "CROSSING": "XING", "CROSSROAD": "XRD", "CURVE": "CURV",
    "DALE": "DL", "DRIVE": "DR", "ESTATE": "EST", "EXPRESSWAY": "EXPY",
    "EXTENSION": "EXT", "FALLS": "FLS", "FERRY": "FRY", "FIELD": "FLD",
    "FIELDS": "FLDS", "FLAT": "FLT", "FORD": "FRD", "FOREST": "FRST",
    "FORGE": "FRG", "FORK": "FRK", "FREEWAY": "FWY", "GARDEN": "GDN",
    "GARDENS": "GDNS", "GATEWAY": "GTWY", "GLEN": "GLN", "GREEN": "GRN",
    "GROVE": "GRV", "HARBOR": "HBR", "HAVEN": "HVN", "HEIGHTS": "HTS",
    "HIGHWAY": "HWY", "HILL": "HL", "HILLS": "HLS", "HOLLOW": "HOLW",
    "INLET": "INLT", "ISLAND": "IS", "ISLE": "ISLE", "JUNCTION": "JCT",
    "KEY": "KY", "KNOLL": "KNL", "LAKE": "LK", "LAKES": "LKS",
    "LANE": "LN", "LIGHT": "LGT", "LOAF": "LF", "LOCK": "LCK",
    "LODGE": "LDG", "LOOP": "LOOP", "MALL": "MALL", "MANOR": "MNR",
    "MEADOW": "MDW", "MEADOWS": "MDWS", "MILL": "ML", "MILLS": "MLS",
    "MISSION": "MSN", "MOTORWAY": "MTWY", "MOUNT": "MT", "MOUNTAIN": "MTN",
    "NECK": "NCK", "ORCHARD": "ORCH", "OVAL": "OVAL", "OVERPASS": "OPAS",
    "PARK": "PARK", "PARKWAY": "PKWY", "PASS": "PASS", "PATH": "PATH",
    "PIKE": "PIKE", "PINE": "PNE", "PINES": "PNES", "PLACE": "PL",
    "PLAIN": "PLN", "PLAINS": "PLNS", "PLAZA": "PLZ", "POINT": "PT",
    "POINTS": "PTS", "PORT": "PRT", "PRAIRIE": "PR", "RADIAL": "RADL",
    "RAMP": "RAMP", "RANCH": "RNCH", "RAPID": "RPD", "RAPIDS": "RPDS",
    "REST": "RST", "RIDGE": "RDG", "RIDGES": "RDGS", "RIVER": "RIV",
    "ROAD": "RD", "ROADS": "RDS", "ROUTE": "RTE", "ROW": "ROW",
    "RUN": "RUN", "SHOAL": "SHL", "SHOALS": "SHLS", "SHORE": "SHR",
    "SHORES": "SHRS", "SKYWAY": "SKWY", "SPRING": "SPG", "SPRINGS": "SPGS",
    "SPUR": "SPUR", "SQUARE": "SQ", "STREAM": "STRM", "STREET": "ST",
    "SUMMIT": "SMT", "TERRACE": "TER", "THROUGHWAY": "TRWY", "TRACE": "TRCE",
    "TRACK": "TRAK", "TRAIL": "TRL", "TUNNEL": "TUNL", "TURNPIKE": "TPKE",
    "UNDERPASS": "UPAS", "UNION": "UN", "VALLEY": "VLY", "VIADUCT": "VIA",
    "VIEW": "VW", "VILLAGE": "VLG", "VILLE": "VL", "VISTA": "VIS",
    "WALK": "WALK", "WALL": "WALL", "WAY": "WAY", "WELL": "WL",
    "WELLS": "WLS",
}


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

def normalize_street(street):
    """Normalize street suffix to USPS abbreviation to match BatchLeads format."""
    parts = street.upper().split()
    if parts and parts[-1] in SUFFIX_MAP:
        parts[-1] = SUFFIX_MAP[parts[-1]]
    return " ".join(parts)


# ─────────────────────────────────────────────
# Google Sheets
# ─────────────────────────────────────────────

def get_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_TAB)

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
    return normalize_street(street.strip()), city.strip(), state.strip(), zip_.strip()

def append_row(sheet, address, recorded_date, sale_date, trustee=""):
    street, city, state, zip_ = parse_address(address)
    sheet.append_row(
        [street, city, state, zip_, recorded_date, sale_date, trustee, "Active", ""],
        value_input_option="USER_ENTERED",
    )
    log.info(f"  New row: {street} | {recorded_date} | trustee: {trustee or '—'}")

def update_row(sheet, row_index, recorded_date, sale_date):
    sheet.update_cell(row_index, 5, recorded_date)
    sheet.update_cell(row_index, 6, sale_date)
    log.info(f"  Updated row {row_index}: {recorded_date} / {sale_date}")


# ─────────────────────────────────────────────
# Trustee Extraction via Claude API
# ─────────────────────────────────────────────

def extract_trustee(page, doc_url):
    if not ANTHROPIC_API_KEY:
        return ""
    try:
        import anthropic
        if not goto_with_retry(page, doc_url, retries=2, delay=3):
            return ""
        time.sleep(2)
        screenshot = page.screenshot(full_page=True)
        img_b64 = base64.standard_b64encode(screenshot).decode("utf-8")

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=100,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": img_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": (
                            "This is a Texas foreclosure notice. "
                            "Find and return ONLY the name of the Substitute Trustee. "
                            "Return just the name with no extra text. "
                            "If you cannot find it, return 'Unknown'."
                        ),
                    },
                ],
            }],
        )
        trustee = response.content[0].text.strip()
        log.info(f"  Trustee: {trustee}")
        return trustee
    except Exception as e:
        log.warning(f"  Trustee extraction failed: {e}")
        return ""


# ─────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────

def scrape_foreclosures(most_recent_date):
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
            log.info(f"Loading page {page_num} (offset={offset})…")

            if not goto_with_retry(page, url):
                log.error(f"  Gave up after 3 retries on page {page_num}.")
                break

            time.sleep(3)

            rows      = page.locator("table tbody tr").all()
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

                    if most_recent_date:
                        rec_dt = parse_date(recorded_date)
                        if rec_dt and rec_dt < most_recent_date:
                            log.info(
                                f"  {recorded_date} < most recent "
                                f"{most_recent_date.strftime('%m/%d/%Y')} — stopping."
                            )
                            done = True
                            break

                    address = ""
                    for val in reversed(cell_text):
                        if val and re.search(r"\d+\s+\w+", val):
                            address = val
                            break

                    doc_url = None
                    try:
                        href = row.locator("a").first.get_attribute("href")
                        if href:
                            doc_url = (
                                f"https://bexar.tx.publicsearch.us{href}"
                                if href.startswith("/") else href
                            )
                    except Exception:
                        pass

                    if address:
                        results.append({
                            "address":       address,
                            "recorded_date": recorded_date,
                            "sale_date":     sale_date,
                            "doc_url":       doc_url,
                            "trustee":       "",
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

        if ANTHROPIC_API_KEY and results:
            log.info(f"Extracting trustee names for {len(results)} record(s)…")
            for f in results:
                if f.get("doc_url"):
                    f["trustee"] = extract_trustee(page, f["doc_url"])
                    time.sleep(1)

        browser.close()

    return results


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Bexar County Foreclosure Scraper v4")
    log.info("=" * 60)

    try:
        sheet = get_sheet()
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

            if row_index is not None:
                update_row(sheet, row_index, f["recorded_date"], f["sale_date"])
                update_count += 1
            else:
                append_row(
                    sheet,
                    f["address"],
                    f["recorded_date"],
                    f["sale_date"],
                    f.get("trustee", ""),
                )
                new_count += 1

            time.sleep(1.5)

        log.info(f"Done. {new_count} new | {update_count} updated.")

    except Exception as e:
        log.error(f"SCRAPER CRASHED: {e}")
        send_text(f"Bexar scraper crashed: {str(e)[:120]}")
        raise


if __name__ == "__main__":
    main()

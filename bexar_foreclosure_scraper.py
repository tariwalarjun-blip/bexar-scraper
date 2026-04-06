"""
Bexar County Foreclosure Scraper - v17
Changes from v16:
- BatchData skip trace now also extracts email address + DNC flag
- New BatchData Property Lookup call pulls equity %, AVM value, loan balance
- Auto-calculates Potential (Sub-To / Wholesale / Both) based on equity:
    Equity < 25%  → Sub-To
    Equity 25-50% → Both
    Equity > 50%  → Wholesale
    Unknown       → blank (manual review)
- New columns:
    T: Email
    U: Estimated Equity %
    V: Estimated Value (AVM)
    W: Loan Balance
    X: Potential (Sub-To / Wholesale / Both)
- All new fields included in Zapier webhook payload
- New GitHub secret required: BATCHDATA_API_TOKEN (same key, already added)
"""

import os
import re
import time
import json
import logging
import urllib.request
from datetime import datetime, timedelta
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

load_dotenv()

CREDENTIALS_FILE   = os.environ["GOOGLE_SHEETS_CREDENTIALS"]
SHEET_ID           = os.environ.get("SHEET_ID", "1Z9l13Z62LuTJy2hP3ttlYJyWfK253eMvvtWQ5D0iLKo")
ZAPIER_WEBHOOK_URL = os.environ.get("ZAPIER_WEBHOOK_URL", "")
BATCHDATA_TOKEN    = os.environ.get("BATCHDATA_API_TOKEN", "")
SHEET_TAB          = "Sheet1"
REFILE_TAB         = "Refile Log"
MAX_DAYS_BACK      = 30
MIN_APPRAISAL      = 80000

CAD_SEARCH_URL = "https://bexar.trueautomation.com/clientdb/?cid=110"

STREET_SUFFIXES = {
    "ST", "STREET", "DR", "DRIVE", "RD", "ROAD", "LN", "LANE",
    "BLVD", "BOULEVARD", "AVE", "AVENUE", "CT", "COURT", "CIR",
    "CIRCLE", "WAY", "TRL", "TRAIL", "PKWY", "PARKWAY", "PL",
    "PLACE", "LOOP", "PASS", "PATH", "RUN", "CV", "COVE", "XING",
    "CROSSING", "HWY", "HIGHWAY", "LACE", "RIDGE", "GLEN", "PARK",
    "BEND", "CREEK", "HILLS", "VIEW", "WOOD", "WOODS", "MEADOW",
    "MEADOWS", "VALLEY", "HOLLOW", "HOLW", "GROVE", "TRACE", "BAY",
    "VISTA", "KNOLL", "KNOLLS", "SPRING", "SPRINGS", "FALLS",
    "FIELD", "FIELDS", "POINT", "PT", "POINTE", "TERRACE", "TER",
    "BLUFF", "BLUFFS", "CANYON", "CLIFF", "CLIFFS", "CREST",
    "GATE", "GATES", "HAVEN", "HEIGHTS", "HTS", "HILL", "HILLS",
    "MANOR", "MILL", "MILLS", "MOUNT", "MT", "OVAL", "PIKE",
    "PINE", "PINES", "PRAIRIE", "RANCH", "RIDGE", "SQUARE", "SQ",
    "SUMMIT", "TRACE", "TRAIL", "TRAILS", "VILLAGE", "VLG",
    "WALK", "WELL", "WELLS", "ESTATES", "ESTATE", "LAKES", "LAKE",
    "SHORES", "SHORE", "CHASE", "COMMON", "COMMONS", "CROSSING",
}

LLC_KEYWORDS = {
    "LLC", "INC", "CORP", "CORPORATION", "LP", "LTD", "LIMITED",
    "TRUST", "PROPERTIES", "HOLDINGS", "INVESTMENTS", "REALTY",
    "REAL ESTATE", "PARTNERS", "GROUP", "ENTERPRISES", "VENTURES",
    "FUND", "CAPITAL", "ASSETS", "MANAGEMENT",
}

BASE_URL = (
    "https://bexar.tx.publicsearch.us/results"
    "?department=FC&limit=50&searchType=advancedSearch"
    "&sort=desc&sortBy=recordedDate&offset={offset}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

def parse_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except Exception:
        return None


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
    DIRECTIONALS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW",
                    "NORTH", "SOUTH", "EAST", "WEST"}
    tokens = street.upper().split()
    while tokens and tokens[-1] in STREET_SUFFIXES:
        tokens = tokens[:-1]
    if len(tokens) > 1:
        filtered = [tokens[0]] + [t for t in tokens[1:] if t not in DIRECTIONALS]
        tokens = filtered
    return " ".join(tokens)


def is_llc_owner(owner_name):
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


def days_until_auction(sale_date_str):
    dt = parse_date(sale_date_str)
    if not dt:
        return ""
    delta = (dt.date() - datetime.now().date()).days
    return max(0, delta)


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


def calc_potential(equity_pct_str):
    """
    Auto-classify lead as Sub-To, Wholesale, or Both based on equity %.
    Returns empty string if equity unknown.
    """
    if not equity_pct_str:
        return ""
    try:
        eq = float(str(equity_pct_str).replace("%", "").strip())
        if eq < 25:
            return "Sub-To"
        elif eq > 50:
            return "Wholesale"
        else:
            return "Both"
    except Exception:
        return ""


# ─────────────────────────────────────────────
# BatchData API helpers
# ─────────────────────────────────────────────

def _batchdata_post(endpoint, payload):
    """Make a POST request to BatchData API. Returns parsed JSON or None."""
    if not BATCHDATA_TOKEN:
        log.warning("  BatchData: BATCHDATA_API_TOKEN not set — skipping")
        return None
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.batchdata.com/api/v1/{endpoint}",
            data=data,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": BATCHDATA_TOKEN,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning(f"  BatchData API error ({endpoint}): {e}")
        return None


def skip_trace_address(street, city, state, zip_):
    """
    Call BatchData skip trace API.
    Returns dict: { phone, email, dnc, owner_name, mailing_address }
    owner_name and mailing_address are used as CAD fallback when CAD misses.
    """
    result = {"phone": "", "email": "", "dnc": "", "owner_name": "", "mailing_address": ""}

    resp = _batchdata_post("property/skip-trace", {
        "requests": [{
            "propertyAddress": {
                "street": street.title(),
                "city":   city.title() if city else "San Antonio",
                "state":  state or "TX",
                "zip":    zip_ or "",
            }
        }]
    })

    if not resp:
        return result

    results = resp.get("results", [])
    if not results:
        log.info(f"  BatchData skip trace: no results for {street}")
        return result

    persons = results[0].get("personDetails", [])

    mobile_phones  = []
    other_phones   = []
    emails         = []
    dnc_flag       = False

    for person in persons:
        # ── Phones ──────────────────────────────────────────────────
        for phone in person.get("phones", []):
            number     = phone.get("number", "")
            phone_type = phone.get("type", "").lower()
            is_dnc     = phone.get("dnc", False) or phone.get("dncRegistered", False)

            digits = re.sub(r"\D", "", number)
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if len(digits) != 10:
                continue

            if is_dnc:
                dnc_flag = True

            if "mobile" in phone_type or "cell" in phone_type:
                mobile_phones.append(digits)
            else:
                other_phones.append(digits)

        # ── Emails ──────────────────────────────────────────────────
        for email in person.get("emails", []):
            addr = email.get("address", "") or email.get("email", "") or (
                email if isinstance(email, str) else ""
            )
            if addr and "@" in addr:
                emails.append(addr)

    result["phone"] = mobile_phones[0] if mobile_phones else (
                      other_phones[0]  if other_phones  else "")
    result["email"] = emails[0] if emails else ""
    result["dnc"]   = "YES" if dnc_flag else "NO"

    # ── Owner name + mailing address from first person (CAD fallback) ──
    if persons:
        first      = persons[0]
        first_name = first.get("firstName", "") or first.get("first_name", "")
        last_name  = first.get("lastName",  "") or first.get("last_name",  "")
        result["owner_name"] = f"{first_name} {last_name}".strip()

        addresses = first.get("addresses", [])
        if addresses:
            a = addresses[0]
            m_street = a.get("street", "") or a.get("streetAddress", "")
            m_city   = a.get("city", "")
            m_state  = a.get("state", "")
            m_zip    = a.get("zip", "") or a.get("postalCode", "")
            if m_street:
                result["mailing_address"] = (
                    f"{m_street}, {m_city}, {m_state} {m_zip}".strip(", ")
                )

    log.info(
        f"  BatchData skip trace: {street} -> "
        f"phone={result['phone'] or 'none'} | "
        f"email={result['email'] or 'none'} | "
        f"owner={result['owner_name'] or 'none'} | "
        f"dnc={result['dnc']}"
    )
    return result


def property_lookup(street, city, state, zip_):
    """
    Call BatchData Property Lookup API to get equity, AVM, loan balance.
    Returns dict: { equity_pct, avm_value, loan_balance }
    """
    result = {"equity_pct": "", "avm_value": "", "loan_balance": ""}

    resp = _batchdata_post("property/search", {
        "data": {
            "propertyAddress": {
                "street": street.title(),
                "city":   city.title() if city else "San Antonio",
                "state":  state or "TX",
                "zip":    zip_ or "",
            }
        }
    })

    if not resp:
        return result

    results = resp.get("results", [])
    if not results:
        log.info(f"  BatchData property lookup: no results for {street}")
        return result

    prop = results[0]

    # Navigate nested financial data — field names vary slightly by API version
    # Try common paths BatchData uses
    financial = (
        prop.get("financial") or
        prop.get("financialData") or
        prop.get("mortgage") or
        {}
    )

    valuation = (
        prop.get("valuation") or
        prop.get("avm") or
        prop.get("value") or
        {}
    )

    # Equity %
    eq_pct = (
        financial.get("equityPercent") or
        financial.get("equity_percent") or
        financial.get("estimatedEquityPercent") or
        prop.get("equityPercent") or
        prop.get("equity_percent") or
        ""
    )

    # AVM value
    avm = (
        valuation.get("estimatedValue") or
        valuation.get("value") or
        valuation.get("avm") or
        prop.get("estimatedValue") or
        prop.get("avm") or
        ""
    )

    # Loan balance
    loan = (
        financial.get("openLoanAmount") or
        financial.get("loanBalance") or
        financial.get("estimatedLoanBalance") or
        financial.get("totalOpenLienAmount") or
        prop.get("openLoanAmount") or
        prop.get("loanBalance") or
        ""
    )

    # Format values
    if eq_pct:
        try:
            result["equity_pct"] = f"{round(float(eq_pct))}%"
        except Exception:
            result["equity_pct"] = str(eq_pct)

    if avm:
        try:
            result["avm_value"] = f"${int(float(avm)):,}"
        except Exception:
            result["avm_value"] = str(avm)

    if loan:
        try:
            result["loan_balance"] = f"${int(float(loan)):,}"
        except Exception:
            result["loan_balance"] = str(loan)

    # Last sale date (CAD fallback)
    sale_history = (
        prop.get("saleHistory") or
        prop.get("lastSale") or
        prop.get("transactionHistory") or
        []
    )
    if isinstance(sale_history, list) and sale_history:
        last = sale_history[0]
        sale_date_raw = (
            last.get("saleDate") or last.get("recordingDate") or
            last.get("date") or ""
        )
        if sale_date_raw:
            try:
                # Normalize to MM/DD/YYYY
                from datetime import datetime as _dt
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
                    try:
                        result["last_sale_date"] = _dt.strptime(
                            str(sale_date_raw)[:10], fmt
                        ).strftime("%m/%d/%Y")
                        break
                    except Exception:
                        pass
            except Exception:
                result["last_sale_date"] = str(sale_date_raw)

    # Appraised/assessed value (CAD fallback)
    assessed = (
        prop.get("assessedValue") or
        prop.get("taxAssessedValue") or
        financial.get("assessedValue") or
        ""
    )
    if assessed:
        try:
            result["appraised_value"] = f"${int(float(assessed)):,}"
        except Exception:
            result["appraised_value"] = str(assessed)

    log.info(
        f"  BatchData property lookup: {street} -> "
        f"equity={result['equity_pct'] or 'n/a'} | "
        f"avm={result['avm_value'] or 'n/a'} | "
        f"loan={result['loan_balance'] or 'n/a'} | "
        f"last_sale={result['last_sale_date'] or 'n/a'}"
    )
    return result


# ─────────────────────────────────────────────
# Zapier webhook ping
# ─────────────────────────────────────────────

def ping_zapier(row_data: dict):
    if not ZAPIER_WEBHOOK_URL:
        log.warning("  ZAPIER_WEBHOOK_URL not set — skipping webhook ping")
        return
    try:
        payload = json.dumps(row_data).encode("utf-8")
        req = urllib.request.Request(
            ZAPIER_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.getcode()
            log.info(f"  Zapier webhook ping: HTTP {status}")
    except Exception as e:
        log.warning(f"  Zapier webhook ping failed: {e}")


# ─────────────────────────────────────────────
# Bexar CAD Lookup
# ─────────────────────────────────────────────

def cad_lookup(page, street):
    """
    Returns dict with CAD data if found and passes all filters.
    Returns None → skip this lead (LLC / non-SF / below min appraisal)
    Returns {}   → no CAD match found, proceed with empty data
    """
    search_term = strip_street_suffix(street)
    if not search_term:
        log.warning(f"  CAD: Could not build search term from '{street}'")
        return {}

    log.info(f"  CAD lookup: '{search_term}'")

    try:
        if not goto_with_retry(page, CAD_SEARCH_URL):
            log.warning("  CAD: Could not load search page")
            return {}

        for selector in ["input[name='PropertySearch']", "input[type='text']",
                         "#PropertySearch", "input.search"]:
            try:
                sb = page.locator(selector).first
                sb.wait_for(timeout=10000)
                sb.fill(search_term)
                page.keyboard.press("Enter")
                page.wait_for_load_state("networkidle", timeout=30000)
                time.sleep(2)
                break
            except Exception:
                continue
        else:
            log.warning(f"  CAD: Could not find search input for '{search_term}'")
            return {}

        results_text = page.locator("body").inner_text()
        if "0 of 0" in results_text or "no results" in results_text.lower():
            log.info(f"  CAD: No results for '{search_term}'")
            return {}

        house_num         = street.split()[0] if street.split() else ""
        matched_row       = None
        result_owner_name = ""
        result_appraised  = ""

        def try_match_rows(rows):
            nonlocal matched_row, result_owner_name, result_appraised
            for row in rows:
                cells = row.locator("td").all()
                if len(cells) < 7:
                    continue
                try:
                    address_cell = cells[4].inner_text().strip().upper()
                    if not (address_cell.startswith(house_num + " ") or
                            address_cell.startswith(house_num + ",")):
                        continue

                    owner_cell = cells[6].inner_text().strip() if len(cells) > 6 else ""
                    appr_cell  = cells[8].inner_text().strip() if len(cells) > 8 else ""

                    if not owner_cell or owner_cell.lower() in {"view details", "view map", ""}:
                        owner_cell = cells[5].inner_text().strip() if len(cells) > 5 else ""

                    if is_llc_owner(owner_cell.upper()):
                        log.info(f"  CAD: LLC owner '{owner_cell}' — skipping")
                        return "LLC"

                    matched_row       = row
                    result_owner_name = owner_cell
                    result_appraised  = re.sub(r"[$,]", "", appr_cell).strip()
                    return "FOUND"
                except Exception:
                    continue
            return "NONE"

        rows   = page.locator("table tr").all()
        status = try_match_rows(rows)

        if status == "LLC":
            return None

        if status == "NONE":
            street_words = street.split()
            if len(street_words) >= 2:
                fallback_term = f"{street_words[0]} {street_words[1]}"
                if fallback_term != search_term:
                    log.info(f"  CAD: No match, trying fallback '{fallback_term}'")
                    try:
                        goto_with_retry(page, CAD_SEARCH_URL)
                        for selector in ["input[name='PropertySearch']",
                                         "input[type='text']", "#PropertySearch"]:
                            try:
                                sb = page.locator(selector).first
                                sb.wait_for(timeout=8000)
                                sb.fill(fallback_term)
                                page.keyboard.press("Enter")
                                page.wait_for_load_state("networkidle", timeout=30000)
                                time.sleep(2)
                                break
                            except Exception:
                                continue
                        fb_status = try_match_rows(page.locator("table tr").all())
                        if fb_status == "LLC":
                            return None
                    except Exception as fe:
                        log.warning(f"  CAD: fallback search failed: {fe}")

        if not matched_row:
            log.info(f"  CAD: No address match for house number {house_num}")
            return {}

        if result_appraised:
            try:
                appr_num = float(re.sub(r"[^\d.]", "", result_appraised))
                if appr_num < MIN_APPRAISAL:
                    log.info(
                        f"  CAD: Appraised ${appr_num:,.0f} below "
                        f"${MIN_APPRAISAL:,} minimum — skipping: {street}"
                    )
                    return None
            except Exception:
                pass

        detail_link = matched_row.locator("a:has-text('View Details')")
        if detail_link.count() == 0:
            log.warning("  CAD: No View Details link found")
            return {}

        detail_link.first.click()
        page.wait_for_load_state("networkidle", timeout=30000)
        time.sleep(2)

        detail_text = page.locator("body").inner_text()
        owner_name  = result_owner_name

        mailing_address = ""
        mail_block = re.search(
            r"Mailing Address:(.*?)(?:Exemptions:|\Z)", detail_text, re.DOTALL
        )
        if mail_block:
            block = mail_block.group(1)
            street_match = re.search(
                r"(?:^|\n)\s*(\d+\s+[^\n%]+?)(?:\s{3,}%|\n|$)", block
            )
            city_match = re.search(
                r"(?:^|\n)\s*([A-Z]+(?:\s+[A-Z]+){0,2},\s*TX\s+\d{5}[-\d]*)",
                block, re.MULTILINE,
            )
            if street_match and city_match:
                mailing_address = (
                    f"{street_match.group(1).strip()}, {city_match.group(1).strip()}"
                )
            elif street_match:
                mailing_address = street_match.group(1).strip()
            elif city_match:
                mailing_address = f"{street.title()}, {city_match.group(1).strip()}"

        appraised_value = result_appraised

        last_sale_date = ""
        try:
            for selector in ["text=Deed History", "text=Deed History - (Last 3"]:
                try:
                    hdr = page.locator(selector).first
                    if hdr.count() > 0:
                        hdr.click()
                        time.sleep(1.5)
                        break
                except Exception:
                    pass

            expanded_text = page.locator("body").inner_text()
            deed_match = re.search(r"Deed Date.+?\n(.{0,300})", expanded_text, re.DOTALL)
            if deed_match:
                for m in re.finditer(r"(\d{1,2}/\d{1,2}/\d{4})", deed_match.group(1)):
                    candidate = m.group(1)
                    try:
                        dt = datetime.strptime(candidate, "%m/%d/%Y")
                        if dt.year < datetime.now().year:
                            last_sale_date = candidate
                            break
                    except Exception:
                        pass
        except Exception as de:
            log.warning(f"  CAD: deed date extraction failed: {de}")

        property_type = ""
        prop_match = re.search(r"Property Use Description:\s*([^\n]+)", detail_text)
        if prop_match:
            property_type = prop_match.group(1).strip()

        if property_type and "single family" not in property_type.lower():
            log.info(f"  CAD: Non-SF '{property_type}' — skipping: {street}")
            return None

        log.info(
            f"  CAD: {owner_name} | {mailing_address} | "
            f"${appraised_value} | Last sale: {last_sale_date} | {property_type}"
        )

        return {
            "owner_name":      owner_name,
            "mailing_address": mailing_address,
            "appraised_value": appraised_value,
            "last_sale_date":  last_sale_date,
            "property_type":   property_type,
        }

    except Exception as e:
        log.warning(f"  CAD lookup failed for '{street}': {e}")
        return {}


# ─────────────────────────────────────────────
# Google Sheets helpers
# ─────────────────────────────────────────────

def get_workbook():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SHEET_ID)


def get_main_sheet(wb):
    return wb.worksheet(SHEET_TAB)


def get_refile_log(wb):
    try:
        return wb.worksheet(REFILE_TAB)
    except Exception:
        log.info("  Creating 'Refile Log' tab...")
        relog = wb.add_worksheet(title=REFILE_TAB, rows=1000, cols=10)
        relog.append_row([
            "Date Detected", "Address",
            "Old Recorded Date", "New Recorded Date",
            "Old Auction Date",  "New Auction Date",
            "DK Status", "Action Taken",
        ])
        return relog


def get_existing_data(sheet):
    rows      = sheet.get_all_values()
    existing  = {}
    all_dates = []

    for i, row in enumerate(rows[1:], start=2):
        street = row[0].strip().upper() if len(row) > 0 else ""
        if not street:
            continue
        recorded_date = row[4].strip() if len(row) > 4 else ""
        auction_date  = row[5].strip() if len(row) > 5 else ""
        dk_status     = row[8].strip().upper() if len(row) > 8 else ""

        existing[street] = {
            "row_index":     i,
            "dk_status":     dk_status,
            "recorded_date": recorded_date,
            "auction_date":  auction_date,
        }

        d = parse_date(recorded_date)
        if d:
            all_dates.append(d)

    most_recent = max(all_dates) if all_dates else None
    return existing, most_recent


def log_refile(refile_sheet, address, old_rec, new_rec,
               old_auction, new_auction, dk_status, action):
    refile_sheet.append_row([
        datetime.now().strftime("%m/%d/%Y"),
        address, old_rec, new_rec,
        old_auction, new_auction, dk_status, action,
    ])
    time.sleep(1)


def build_row(address, recorded_date, sale_date, bexar_id, cad_data,
              phone="", email="", equity_pct="", avm_value="", loan_balance=""):
    """Build the 24-column row list (A through X)."""
    street, city, state, zip_ = parse_address(address)
    cad_match = "YES" if cad_data.get("owner_name") else "NO"
    potential = calc_potential(equity_pct)

    return [
        street,                               # A
        city,                                 # B
        state,                                # C
        zip_,                                 # D
        recorded_date,                        # E
        sale_date,                            # F
        "",                                   # G
        "Active",                             # H
        "",                                   # I  DK Status
        bexar_id,                             # J
        cad_data.get("owner_name", ""),       # K
        cad_data.get("mailing_address", ""),  # L
        cad_data.get("appraised_value", ""),  # M
        cad_data.get("last_sale_date", ""),   # N
        "",                                   # O  Owner Type
        cad_data.get("property_type", ""),    # P
        str(days_until_auction(sale_date)),   # Q  Days Until Auction
        cad_match,                            # R  CAD Match
        phone,                                # S  Seller Phone
        email,                                # T  Seller Email
        equity_pct,                           # U  Estimated Equity %
        avm_value,                            # V  Estimated Value (AVM)
        loan_balance,                         # W  Loan Balance
        potential,                            # X  Potential (Sub-To/Wholesale/Both)
    ]


def row_to_webhook_payload(row):
    return {
        "street":            row[0],
        "city":              row[1],
        "state":             row[2],
        "zip":               row[3],
        "recorded_date":     row[4],
        "sale_date":         row[5],
        "status":            row[7],
        "dk_status":         row[8],
        "bexar_id":          row[9],
        "owner_name":        row[10],
        "mailing_address":   row[11],
        "appraised_value":   row[12],
        "last_sale_date":    row[13],
        "property_type":     row[15],
        "days_until_auction": row[16],
        "cad_match":         row[17],
        "seller_phone":      row[18],
        "seller_email":      row[19],
        "equity_pct":        row[20],
        "avm_value":         row[21],
        "loan_balance":      row[22],
        "potential":         row[23],
    }


def append_row(sheet, address, recorded_date, sale_date, bexar_id, cad_data,
               phone="", email="", equity_pct="", avm_value="", loan_balance=""):
    row = build_row(address, recorded_date, sale_date, bexar_id, cad_data,
                    phone, email, equity_pct, avm_value, loan_balance)
    sheet.append_row(row, value_input_option="USER_ENTERED")
    log.info(
        f"  New row: {row[0]} | {recorded_date} | {row[9]} | "
        f"owner={row[10] or 'none'} | phone={phone or 'none'} | "
        f"equity={equity_pct or 'n/a'} | potential={row[23] or 'unknown'}"
    )
    time.sleep(2)
    ping_zapier(row_to_webhook_payload(row))


def reset_dead_row(sheet, row_index, address, recorded_date, sale_date,
                   bexar_id, cad_data, phone="", email="",
                   equity_pct="", avm_value="", loan_balance=""):
    row = build_row(address, recorded_date, sale_date, bexar_id, cad_data,
                    phone, email, equity_pct, avm_value, loan_balance)
    cell_range = f"A{row_index}:X{row_index}"
    sheet.update(cell_range, [row], value_input_option="USER_ENTERED")
    log.info(f"  Reset DEAD row {row_index}: {row[0]} | {recorded_date} | {row[9]}")
    time.sleep(2)
    ping_zapier(row_to_webhook_payload(row))


def update_dates_only(sheet, row_index, recorded_date, sale_date):
    sheet.update_cell(row_index, 5, recorded_date)
    time.sleep(1)
    sheet.update_cell(row_index, 6, sale_date)
    time.sleep(1)
    dua = str(days_until_auction(sale_date))
    sheet.update_cell(row_index, 17, dua)
    time.sleep(1)
    log.info(f"  Updated dates row {row_index}: {recorded_date} / {sale_date}")


def update_dates_and_reset_dk(sheet, row_index, recorded_date, sale_date):
    sheet.update_cell(row_index, 5, recorded_date)
    time.sleep(1)
    sheet.update_cell(row_index, 6, sale_date)
    time.sleep(1)
    sheet.update_cell(row_index, 9, "DK1")
    time.sleep(1)
    dua = str(days_until_auction(sale_date))
    sheet.update_cell(row_index, 17, dua)
    time.sleep(1)
    log.info(f"  Updated dates + reset DK1 row {row_index}: {recorded_date} / {sale_date}")


# ─────────────────────────────────────────────
# Auction date expiry check
# ─────────────────────────────────────────────

def expire_old_leads(sheet, existing):
    today         = datetime.now().date()
    expired_count = 0

    for street, data in existing.items():
        auction_date = data.get("auction_date", "")
        dk_status    = data.get("dk_status", "")
        row_index    = data["row_index"]

        if not auction_date or dk_status == "DEAD":
            continue

        auction_dt = parse_date(auction_date)
        if auction_dt and auction_dt.date() < today:
            sheet.update_cell(row_index, 9, "DEAD")
            time.sleep(1)
            expired_count += 1
            log.info(f"  Expired (auction passed {auction_date}): {street}")
            data["dk_status"] = "DEAD"

    if expired_count:
        log.info(f"  Marked {expired_count} lead(s) DEAD — auction date passed")

    return expired_count


# ─────────────────────────────────────────────
# Foreclosure scraper
# ─────────────────────────────────────────────

def scrape_page_with_retry(page, url, max_retries=3):
    for attempt in range(1, max_retries + 1):
        if not goto_with_retry(page, url):
            return []
        time.sleep(3)
        rows      = page.locator("table tbody tr").all()
        data_rows = [r for r in rows
                     if re.search(r"\d{1,2}/\d{1,2}/\d{4}", r.inner_text())]
        if data_rows:
            return rows
        log.warning(f"  0 data rows on attempt {attempt}/{max_retries} — retrying in 10s...")
        time.sleep(10)
    log.error("  Page returned 0 rows after all retries.")
    return []


def scrape_foreclosures(most_recent_date):
    results     = []
    done        = False
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
            url  = BASE_URL.format(offset=offset)
            log.info(f"Loading page {page_num} (offset={offset})...")
            rows = scrape_page_with_retry(page, url)
            if not rows:
                log.error(f"  Giving up on page {page_num} after retries.")
                break

            data_rows = 0
            for row in rows:
                try:
                    cells     = row.locator("td").all()
                    cell_text = [c.inner_text().strip() for c in cells]
                    dates     = [v for v in cell_text
                                 if re.match(r"\d{1,2}/\d{1,2}/\d{4}", v)]
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
    log.info("Bexar County Foreclosure Scraper v17")
    log.info("=" * 60)

    try:
        wb           = get_workbook()
        sheet        = get_main_sheet(wb)
        refile_sheet = get_refile_log(wb)

        existing, most_recent = get_existing_data(sheet)
        log.info(
            f"Existing records: {len(existing)} | "
            f"Most recent date: {most_recent.strftime('%m/%d/%Y') if most_recent else 'none'}"
        )

        expired = expire_old_leads(sheet, existing)

        foreclosures = scrape_foreclosures(most_recent)
        log.info(f"Records to process: {len(foreclosures)}")

        new_count    = 0
        update_count = 0
        skipped      = 0

        with sync_playwright() as p:
            browser  = p.chromium.launch(headless=True)
            context  = browser.new_context(viewport={"width": 1280, "height": 900})
            cad_page = context.new_page()

            for f in foreclosures:
                street, city, state, zip_ = parse_address(f["address"])
                key = street.strip().upper()
                if not key:
                    continue

                row_data = existing.get(key)

                # ── DUPLICATE ──────────────────────────────────────────
                if row_data is not None:
                    existing_rec     = row_data["recorded_date"]
                    existing_auction = row_data["auction_date"]
                    new_rec          = f["recorded_date"]
                    new_auction      = f["sale_date"]
                    dk_status        = row_data["dk_status"]
                    row_index        = row_data["row_index"]

                    if existing_rec == new_rec and existing_auction == new_auction:
                        log.info(f"  Skip (same dates): {street}")
                        continue

                    log.info(
                        f"  Refile detected: {street} | "
                        f"auction {existing_auction} → {new_auction} | "
                        f"DK: {dk_status}"
                    )

                    if dk_status == "DEAD":
                        cad_data = cad_lookup(cad_page, street)

                        if cad_data is None:
                            log.info(f"  Refile: still LLC/non-SF/low-value — keeping DEAD: {street}")
                            log_refile(refile_sheet, street, existing_rec, new_rec,
                                       existing_auction, new_auction, dk_status,
                                       "Kept DEAD — LLC/non-SF/below min value")
                            skipped += 1
                        else:
                            st_data  = skip_trace_address(street, city, state, zip_)
                            pl_data  = property_lookup(street, city, state, zip_)
                            # Fill CAD miss from BatchData for refiled DEAD leads too
                            if cad_data == {}:
                                cad_data = {
                                    "owner_name":      st_data.get("owner_name", ""),
                                    "mailing_address": st_data.get("mailing_address", ""),
                                    "appraised_value": pl_data.get("appraised_value", ""),
                                    "last_sale_date":  pl_data.get("last_sale_date", ""),
                                    "property_type":   "",
                                }
                            bexar_id = generate_bexar_id(street, new_rec)
                            reset_dead_row(
                                sheet, row_index, f["address"],
                                new_rec, new_auction, bexar_id, cad_data,
                                phone=st_data["phone"],
                                email=st_data["email"],
                                equity_pct=pl_data["equity_pct"],
                                avm_value=pl_data["avm_value"],
                                loan_balance=pl_data["loan_balance"],
                            )
                            log_refile(refile_sheet, street, existing_rec, new_rec,
                                       existing_auction, new_auction, dk_status,
                                       "Reset row — was DEAD, refiled as new lead")
                            update_count += 1

                    elif dk_status == "STATUS CHECK":
                        update_dates_and_reset_dk(sheet, row_index, new_rec, new_auction)
                        log_refile(refile_sheet, street, existing_rec, new_rec,
                                   existing_auction, new_auction, dk_status,
                                   "Updated dates + reset DK status to DK1")
                        update_count += 1

                    else:
                        update_dates_only(sheet, row_index, new_rec, new_auction)
                        log_refile(refile_sheet, street, existing_rec, new_rec,
                                   existing_auction, new_auction, dk_status,
                                   f"Updated dates only — {dk_status}")
                        update_count += 1

                # ── NEW LEAD ───────────────────────────────────────────
                else:
                    cad_data = cad_lookup(cad_page, street)

                    if cad_data is None:
                        skipped += 1
                        log.info(f"  Skipped (LLC/non-SF/low-value): {street}")
                        time.sleep(1)
                        continue

                    # ── BatchData enrichment (always runs) ────────────
                    st_data = skip_trace_address(street, city, state, zip_)
                    pl_data = property_lookup(street, city, state, zip_)

                    # ── If CAD missed, fill blanks from BatchData ──────
                    # cad_data == {} means CAD found nothing (not skipped)
                    if cad_data == {}:
                        log.info(f"  CAD miss — filling from BatchData: {street}")
                        cad_data = {
                            "owner_name":      st_data.get("owner_name", ""),
                            "mailing_address": st_data.get("mailing_address", ""),
                            "appraised_value": pl_data.get("appraised_value", ""),
                            "last_sale_date":  pl_data.get("last_sale_date", ""),
                            "property_type":   "",
                        }

                    bexar_id = generate_bexar_id(street, f["recorded_date"])
                    append_row(
                        sheet, f["address"], f["recorded_date"],
                        f["sale_date"], bexar_id, cad_data,
                        phone=st_data["phone"],
                        email=st_data["email"],
                        equity_pct=pl_data["equity_pct"],
                        avm_value=pl_data["avm_value"],
                        loan_balance=pl_data["loan_balance"],
                    )
                    new_count += 1

                time.sleep(2)

            browser.close()

        log.info("=" * 60)
        log.info(
            f"Done. {new_count} new | {update_count} refiled | "
            f"{skipped} skipped | {expired} expired"
        )
        log.info("=" * 60)

    except Exception as e:
        log.error(f"SCRAPER CRASHED: {e}")
        raise


if __name__ == "__main__":
    main()

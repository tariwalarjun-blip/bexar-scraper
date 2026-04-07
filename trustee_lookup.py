"""
Trustee Lookup - v1
Reads Doc Number (column Y) from Sheet1.
For each row where Substitute Trustee (column G) is blank or "Unknown",
opens bexar.tx.publicsearch.us/doc/{DOC_NUMBER}, screenshots all canvas
pages, sends to Claude Vision API, extracts trustee name, writes back to
column G.

Run on schedule (GitHub Actions every 30 min) AFTER the main scraper runs.

Required env vars (same as scraper):
  GOOGLE_SHEETS_CREDENTIALS
  SHEET_ID
  ANTHROPIC_API_KEY
"""

import os
import re
import time
import base64
import logging
from dotenv import load_dotenv

import anthropic
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

load_dotenv()

CREDENTIALS_FILE = os.environ["GOOGLE_SHEETS_CREDENTIALS"]
SHEET_ID         = os.environ.get("SHEET_ID", "1Z9l13Z62LuTJy2hP3ttlYJyWfK253eMvvtWQ5D0iLKo")
ANTHROPIC_KEY    = os.environ["ANTHROPIC_API_KEY"]
SHEET_TAB        = "Sheet1"

# Column indices (1-based for gspread update_cell)
COL_TRUSTEE   = 7   # G
COL_DOC_NUM   = 25  # Y

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


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


def get_pending_rows(sheet):
    """
    Returns list of dicts for rows where:
    - Column Y (Doc Number) is filled
    - Column G (Substitute Trustee) is blank or 'Unknown'
    """
    rows    = sheet.get_all_values()
    pending = []

    for i, row in enumerate(rows[1:], start=2):  # skip header
        trustee  = row[COL_TRUSTEE - 1].strip() if len(row) >= COL_TRUSTEE else ""
        doc_num  = row[COL_DOC_NUM - 1].strip()  if len(row) >= COL_DOC_NUM else ""
        address  = row[0].strip()                if len(row) > 0            else ""

        if not doc_num:
            continue  # no doc number yet, scraper hasn't stored it

        if trustee and trustee.lower() != "unknown":
            continue  # already have a trustee

        pending.append({
            "row_index": i,
            "address":   address,
            "doc_number": doc_num,
        })

    return pending


# ─────────────────────────────────────────────
# Claude Vision — extract trustee from screenshots
# ─────────────────────────────────────────────

def extract_trustee(images: list, client: anthropic.Anthropic) -> str:
    """
    Send 1 or more page screenshots to Claude Vision.
    Returns trustee name string, or 'Unknown'.
    """
    content = []
    for img_bytes in images:
        b64 = base64.standard_b64encode(img_bytes).decode("utf-8")
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/png", "data": b64},
        })

    content.append({
        "type": "text",
        "text": (
            f"These are {len(images)} page(s) of a Texas Notice of Foreclosure Sale document. "
            "Find the Substitute Trustee or Trustee name. "
            "It usually appears near the bottom of the document near a signature block "
            "with the word 'Trustee' printed below a name or company. "
            "Return ONLY the trustee name or company name, nothing else. "
            "If you cannot find it, return exactly: Unknown"
        ),
    })

    resp = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=200,
        messages=[{"role": "user", "content": content}],
    )
    result = resp.content[0].text.strip()
    log.info(f"    Claude returned: {result}")
    return result


# ─────────────────────────────────────────────
# Playwright — screenshot canvas pages
# ─────────────────────────────────────────────

def get_best_canvas_screenshot(page):
    """Return screenshot bytes of the largest canvas on the page, or None."""
    canvases = page.locator("canvas").all()
    best      = None
    best_size = 0
    for c in canvases:
        try:
            box = c.bounding_box()
            if box:
                size = box["width"] * box["height"]
                if size > best_size:
                    best_size = size
                    best      = c
        except Exception:
            pass
    if best and best_size > 5000:
        return best.screenshot()
    return None


def screenshot_doc_pages(page, doc_number: str) -> list:
    """
    Navigate to the doc viewer, screenshot every page, return list of image bytes.
    """
    url = f"https://bexar.tx.publicsearch.us/doc/{doc_number}"
    log.info(f"  Opening: {url}")

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception as e:
        log.warning(f"  Page load failed: {e}")
        return []

    time.sleep(6)  # wait for canvas to render

    # Detect total pages
    total_pages = 1
    try:
        pages_text = page.locator("text=/of \\d+/").first.inner_text(timeout=3000)
        m = re.search(r"of (\d+)", pages_text)
        if m:
            total_pages = int(m.group(1))
    except Exception:
        pass
    log.info(f"  Total pages: {total_pages}")

    images = []
    for pg in range(1, total_pages + 1):
        img = get_best_canvas_screenshot(page)
        if img:
            images.append(img)
            log.info(f"  Captured page {pg}/{total_pages}")
        else:
            log.warning(f"  No canvas found on page {pg}")

        if pg < total_pages:
            # Click next page button
            clicked = False
            for selector in [
                "button[aria-label='Next Page']",
                "[title='Next Page']",
                "button:has-text('›')",
                "button:has-text('>')",
            ]:
                try:
                    btn = page.locator(selector).first
                    if btn.count() > 0:
                        btn.click()
                        time.sleep(4)
                        clicked = True
                        break
                except Exception:
                    pass
            if not clicked:
                log.warning(f"  Could not click to next page after page {pg}")
                break

    return images


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Trustee Lookup v1")
    log.info("=" * 60)

    sheet   = get_sheet()
    pending = get_pending_rows(sheet)
    log.info(f"Rows needing trustee lookup: {len(pending)}")

    if not pending:
        log.info("Nothing to do — all rows have trustee or no doc number.")
        return

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    filled  = 0
    unknown = 0
    errors  = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page    = context.new_page()

        for record in pending:
            row_index  = record["row_index"]
            address    = record["address"]
            doc_number = record["doc_number"]

            log.info(f"Row {row_index}: {address} — doc {doc_number}")

            try:
                images = screenshot_doc_pages(page, doc_number)

                if not images:
                    log.warning(f"  No images captured for {doc_number} — writing Unknown")
                    sheet.update_cell(row_index, COL_TRUSTEE, "Unknown")
                    unknown += 1
                else:
                    trustee = extract_trustee(images, client)
                    sheet.update_cell(row_index, COL_TRUSTEE, trustee)
                    log.info(f"  Written to row {row_index}: {trustee}")
                    if trustee.lower() == "unknown":
                        unknown += 1
                    else:
                        filled += 1

            except Exception as e:
                log.error(f"  Error on row {row_index} ({address}): {e}")
                errors += 1

            time.sleep(3)  # be polite between doc requests

        browser.close()

    log.info("=" * 60)
    log.info(f"Done. {filled} trustees found | {unknown} unknown | {errors} errors")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

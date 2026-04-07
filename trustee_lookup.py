"""
Trustee Lookup - v2
Changes from v1:
- Replaced canvas screenshot approach with network image interception
- The doc viewer fetches page images as real HTTP image responses
- We intercept those responses directly instead of screenshotting a canvas
- Fallback to full page screenshot if no images intercepted

Reads Doc Number (column Y) from Sheet1.
For each row where Substitute Trustee (column G) is blank or 'Unknown',
extracts trustee using Claude Vision and writes back to column G.
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

COL_TRUSTEE = 7   # G
COL_DOC_NUM = 25  # Y

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
    rows    = sheet.get_all_values()
    pending = []
    for i, row in enumerate(rows[1:], start=2):
        trustee = row[COL_TRUSTEE - 1].strip() if len(row) >= COL_TRUSTEE else ""
        doc_num = row[COL_DOC_NUM - 1].strip()  if len(row) >= COL_DOC_NUM else ""
        address = row[0].strip()                if len(row) > 0            else ""
        if not doc_num:
            continue
        if trustee and trustee.lower() != "unknown":
            continue
        pending.append({"row_index": i, "address": address, "doc_number": doc_num})
    return pending


# ─────────────────────────────────────────────
# Claude Vision
# ─────────────────────────────────────────────

def extract_trustee(images: list, client: anthropic.Anthropic) -> str:
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
            "It usually appears near the bottom near a signature block "
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
# Playwright — intercept image responses
# ─────────────────────────────────────────────

def fetch_doc_images(context, doc_number: str) -> list:
    """
    Open the doc viewer, intercept image HTTP responses, download each one.
    Falls back to full page screenshot if nothing intercepted.
    """
    url = f"https://bexar.tx.publicsearch.us/doc/{doc_number}"
    log.info(f"  Opening: {url}")

    captured_urls = []
    captured_bytes = {}

    page = context.new_page()

    def on_response(response):
        resp_url = response.url
        ct = response.headers.get("content-type", "")
        if "image" in ct and resp_url not in captured_urls:
            # Filter out tiny UI icons (favicons etc)
            captured_urls.append(resp_url)

    page.on("response", on_response)

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception as e:
        log.warning(f"  Page load failed: {e}")
        page.close()
        return []

    # Wait longer for the viewer to fetch page images
    time.sleep(10)

    images = []

    # Filter to only substantive images (skip tiny icons < 5KB)
    doc_image_urls = [
        u for u in captured_urls
        if any(x in u.lower() for x in ["page", "render", "image", "doc", "tiff", "jpg", "png"])
        and "favicon" not in u.lower()
        and "logo" not in u.lower()
    ]

    log.info(f"  Intercepted {len(captured_urls)} images, {len(doc_image_urls)} look like doc pages")

    if doc_image_urls:
        for img_url in doc_image_urls:
            try:
                img_page = context.new_page()
                img_page.goto(img_url, wait_until="domcontentloaded", timeout=15000)
                time.sleep(1)
                img_bytes = img_page.screenshot(full_page=True)
                img_page.close()
                images.append(img_bytes)
                log.info(f"  Captured image from: {img_url}")
            except Exception as e:
                log.warning(f"  Could not fetch {img_url}: {e}")
    elif captured_urls:
        # Use whatever images were intercepted
        for img_url in captured_urls[:5]:  # cap at 5
            try:
                img_page = context.new_page()
                img_page.goto(img_url, wait_until="domcontentloaded", timeout=15000)
                time.sleep(1)
                img_bytes = img_page.screenshot(full_page=True)
                img_page.close()
                images.append(img_bytes)
            except Exception as e:
                log.warning(f"  Could not fetch {img_url}: {e}")
    else:
        # Last resort: screenshot the full page
        log.warning("  No images intercepted — using full page screenshot")
        try:
            img_bytes = page.screenshot(full_page=True)
            images.append(img_bytes)
        except Exception as e:
            log.warning(f"  Full page screenshot failed: {e}")

    page.close()
    return images


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Trustee Lookup v2")
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

        for record in pending:
            row_index  = record["row_index"]
            address    = record["address"]
            doc_number = record["doc_number"]

            log.info(f"Row {row_index}: {address} — doc {doc_number}")

            try:
                images = fetch_doc_images(context, doc_number)

                if not images:
                    log.warning(f"  No images captured — writing Unknown")
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

            time.sleep(3)

        browser.close()

    log.info("=" * 60)
    log.info(f"Done. {filled} trustees found | {unknown} unknown | {errors} errors")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

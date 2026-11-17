print("[STARTUP] Python is executing ln-msging_merged.py", flush=True)
import json
import logging
import hashlib
import re
import time
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

print("[STARTUP] Loading dependencies...", flush=True)
import gspread
import pyperclip
print("[STARTUP] Dependencies loaded.", flush=True)

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    StaleElementReferenceException,
    WebDriverException,
    TimeoutException,
    NoSuchElementException,
)
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"config.json not found: {CONFIG_PATH}")

print(f"[STARTUP] Reading config: {CONFIG_PATH}", flush=True)
CONFIG = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
print("[STARTUP] Config loaded.", flush=True)
TZ = ZoneInfo(CONFIG.get("timezone", "Asia/Kolkata"))

MESSAGE_DAILY_LIMIT = int(CONFIG.get("daily_limit", 22))
CONNECT_DAILY_LIMIT = int(CONFIG.get("connect_daily_limit", 20))
CONNECT_WEEKLY_LIMIT = int(CONFIG.get("connect_weekly_limit", 100))
CONNECT_WEEKDAYS_ONLY = bool(CONFIG.get("connect_weekdays_only", True))

HEADERS = [
    "Job ID", "Tracker Row", "Name", "LinkedIn URL", "Action",
    "Message Text", "Message Type", "Approved", "Not Before (IST)",
    "Status", "Processed At (IST)", "Result Code", "Result Detail",
    "Attempts", "Connection Degree"
]

STOP_CODES = {"CAPTCHA", "LOGGED_OUT", "WEEKLY_LIMIT", "MESSAGE_ACTION_FAILED", "POPUP_CLEANUP_FAILED", "CONNECT_VERIFICATION_PENDING"}

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(LOG_DIR / "outbox.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# 2. GOOGLE SHEETS
# ============================================================

service_account_file = Path(CONFIG["service_account_file"])
if not service_account_file.is_absolute():
    service_account_file = BASE_DIR / service_account_file

print(f"[STARTUP] Connecting to Google Sheets with: {service_account_file}", flush=True)
gc = gspread.service_account(filename=str(service_account_file))
print("[STARTUP] Google service-account authentication initialized.", flush=True)
spreadsheet = gc.open_by_key(CONFIG["spreadsheet_id"])
sheet = spreadsheet.worksheet(CONFIG.get("worksheet_name", "Outbox"))
print("[STARTUP] Opened worksheet:", CONFIG.get("worksheet_name", "Outbox"), flush=True)


def initialize_ledger():
    try:
        ledger_ws = spreadsheet.worksheet("SentLedger")
    except gspread.WorksheetNotFound:
        ledger_ws = spreadsheet.add_worksheet(
            title="SentLedger", rows=2000, cols=3
        )
        ledger_ws.append_row(["Job ID", "Sent At (IST)", "Recipient"])
    return ledger_ws


print("[STARTUP] Initializing SentLedger...", flush=True)
ledger = initialize_ledger()


def initialize_send_guard():
    """Persistent fail-closed guard against resending uncertain jobs."""
    try:
        ws = spreadsheet.worksheet("SendGuard")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title="SendGuard", rows=3000, cols=6)
        ws.append_row(["Job ID", "Recipient", "Message SHA256", "State", "Updated At (IST)", "Detail"])
    headers = ws.row_values(1)
    expected = ["Job ID", "Recipient", "Message SHA256", "State", "Updated At (IST)", "Detail"]
    if headers[:len(expected)] != expected:
        raise RuntimeError("SendGuard worksheet has unexpected headers. Do not edit it automatically.")
    return ws


print("[STARTUP] Initializing SendGuard...", flush=True)
send_guard = initialize_send_guard()
print("[STARTUP] SentLedger and SendGuard ready. Entering main workflow...", flush=True)


def load_rows():
    values = sheet.get_all_values()
    if not values:
        raise RuntimeError("Outbox is empty.")

    headers = values[0]
    missing = [field for field in HEADERS if field not in headers]
    if missing:
        raise RuntimeError(f"Missing Outbox columns: {missing}")

    rows = []
    for row_number, raw in enumerate(values[1:], start=2):
        raw = raw + [""] * max(0, len(headers) - len(raw))
        record = {headers[i]: raw[i] for i in range(len(headers))}
        record["_sheet_row"] = row_number
        rows.append(record)
    return rows


def update_status(row_number, status, code="", detail="",
                  attempts=None, degree=None):
    headers = sheet.row_values(1)
    values = {
        "Status": status,
        "Processed At (IST)": datetime.now(TZ).strftime("%Y-%m-%d %H:%M"),
        "Result Code": code,
        "Result Detail": str(detail)[:500],
    }
    if attempts is not None:
        values["Attempts"] = attempts
    if degree is not None:
        values["Connection Degree"] = degree

    updates = []
    for field, value in values.items():
        col = headers.index(field) + 1
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, col),
            "values": [[value]],
        })
    sheet.batch_update(updates)


# ============================================================
# 3. SENT LEDGER / DUPLICATE PROTECTION
# ============================================================

def get_sent_job_ids():
    values = ledger.col_values(1)
    return {str(v).strip() for v in values[1:] if str(v).strip()}


def message_fingerprint(message):
    normalized = normalize_message(message) if "normalize_message" in globals() else " ".join(str(message).split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def get_send_guard_records():
    values = send_guard.get_all_values()
    records = {}
    for raw in values[1:]:
        raw = raw + [""] * max(0, 6 - len(raw))
        job_id = raw[0].strip()
        if job_id:
            records[job_id] = {
                "recipient": raw[1].strip(),
                "fingerprint": raw[2].strip(),
                "state": raw[3].strip().upper(),
                "updated_at": raw[4].strip(),
                "detail": raw[5].strip(),
            }
    return records


def set_send_guard_state(row, state, detail=""):
    job_id = str(row.get("Job ID", "")).strip()
    if not job_id:
        raise ValueError("Cannot set SendGuard without a Job ID.")
    recipient = str(row.get("Name", "")).strip()
    fingerprint = message_fingerprint(row.get("Message Text", ""))
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    values = [job_id, recipient, fingerprint, state.upper(), now, str(detail)[:500]]
    all_values = send_guard.get_all_values()
    existing_row = None
    for idx, raw in enumerate(all_values[1:], start=2):
        if raw and str(raw[0]).strip() == job_id:
            existing_row = idx
            break
    if existing_row:
        send_guard.update(f"A{existing_row}:F{existing_row}", [values])
    else:
        send_guard.append_row(values)


def get_guard_state(job_id):
    return get_send_guard_records().get(str(job_id).strip(), {}).get("state", "")


def record_sent_job(row):
    job_id = row["Job ID"].strip()
    if not job_id:
        raise ValueError("Job ID is blank.")
    if job_id in get_sent_job_ids():
        return False

    ledger.append_row([
        job_id,
        datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        row["Name"].strip(),
    ])
    return True


def validate_job_ids(rows):
    ids = [r["Job ID"].strip() for r in rows if r["Job ID"].strip()]
    if len(ids) != len(set(ids)):
        raise RuntimeError("Duplicate Job IDs found in Outbox.")


def get_attempts(row):
    try:
        return int(row.get("Attempts", "") or 0)
    except (ValueError, TypeError):
        return 0


# ============================================================
# 4. SCHEDULE / LIMITS
# ============================================================

def is_approved(value):
    return str(value).strip().lower() == "yes"


def parse_ist(value):
    value = str(value or "").strip()
    if not value:
        return None

    formats = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=TZ)
        except ValueError:
            pass
    raise ValueError(f"Invalid Not Before (IST): {value}")


def is_due(value):
    scheduled = parse_ist(value)
    return scheduled is None or datetime.now(TZ) >= scheduled


def is_weekday():
    return datetime.now(TZ).weekday() < 5


def valid_linkedin_url(url):
    return bool(re.match(
        r"^https://(?:www\.)?linkedin\.com/in/[^/\s]+/?$",
        str(url).strip(),
        re.IGNORECASE,
    ))


def get_action_counts(action):
    now = datetime.now(TZ)
    daily = weekly = 0

    for row in load_rows():
        if row["Action"].strip().lower() != action:
            continue
        if row["Status"].strip().lower() != "sent":
            continue

        try:
            processed = datetime.strptime(
                row["Processed At (IST)"].strip(),
                "%Y-%m-%d %H:%M",
            ).replace(tzinfo=TZ)
        except ValueError:
            continue

        age = now - processed
        if processed.date() == now.date():
            daily += 1
        if timedelta(0) <= age < timedelta(days=7):
            weekly += 1

    return daily, weekly


def get_pending_rows():
    rows = load_rows()
    validate_job_ids(rows)
    ledger_ids = get_sent_job_ids()
    guard_records = get_send_guard_records()
    pending = []

    for row in rows:
        if row["Status"].strip().lower() != "queued":
            continue
        if not is_approved(row["Approved"]):
            continue

        job_id = row["Job ID"].strip()
        guard = guard_records.get(job_id, {})
        guard_state = guard.get("state", "")

        if job_id in ledger_ids or guard_state == "SENT":
            update_status(
                row["_sheet_row"], "Sent", "ALREADY_SENT",
                "Job ID is already recorded as sent. Duplicate processing blocked.", get_attempts(row)
            )
            continue

        if guard_state in {"PREPARED", "VERIFY_PENDING", "DRAFT_ONLY"}:
            update_status(
                row["_sheet_row"], "Verification Pending", "DUPLICATE_GUARD_BLOCK",
                "A previous run prepared this message or had an uncertain send. "
                "It will not be prepared again automatically. Check LinkedIn and reconcile SendGuard.",
                get_attempts(row)
            )
            continue

        pending.append(row)
    return pending


# ============================================================
# 5. CHROME / SELENIUM
# ============================================================

def start_browser():
    profile = BASE_DIR / CONFIG["chrome_user_data_dir"]
    profile.mkdir(parents=True, exist_ok=True)

    options = Options()
    options.add_argument(f"--user-data-dir={profile.resolve()}")
    options.add_argument("--start-maximized")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--remote-debugging-port=0")

    driver_path = ChromeDriverManager().install()
    service = Service(executable_path=driver_path, log_output=str(LOG_DIR / "chromedriver.log"))
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait_for_login(driver):
    driver.get("https://www.linkedin.com/feed/")
    print("\nLog in to LinkedIn manually if required.")
    input("Press ENTER after your LinkedIn feed is open: ")

    url = driver.current_url.lower()
    return "/login" not in url and "/checkpoint" not in url


def inspect_page(driver):
    url = driver.current_url.lower()
    if "/login" in url or "/checkpoint" in url:
        return "LOGGED_OUT"

    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
    except WebDriverException:
        return "ERROR"

    if "captcha" in body_text or "security verification" in body_text:
        return "CAPTCHA"

    if (
        "you've reached the weekly invitation limit" in body_text
        or "weekly invitation limit" in body_text
    ):
        return "WEEKLY_LIMIT"

    return None


def _clean_profile_name(value):
    value = re.sub(r"\s+", " ", value or "").strip()
    # Remove common accessibility suffixes that may be appended to headings.
    value = re.sub(r"\s+(?:•|·)\s+.*$", "", value).strip()
    return value


def detect_profile_name(driver, expected_name=None):
    """
    Read the profile heading using several LinkedIn DOM patterns.
    LinkedIn sometimes renders the visible name in a non-h1 heading or
    populates innerText after Selenium's .text initially returns empty.
    """
    selectors = [
        "main h1",
        "main .text-heading-xlarge",
        "main .pv-text-details__left-panel",
        "main .ph5",
        ".pv-top-card h1",
        ".pv-top-card .text-heading-xlarge",
        ".pv-text-details__left-panel h1",
        ".ph5 h1",
        "h1",
    ]

    candidates = []
    for selector in selectors:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                try:
                    if not element.is_displayed():
                        continue
                    for raw in (
                        element.text,
                        element.get_attribute("innerText"),
                        element.get_attribute("textContent"),
                        element.get_attribute("aria-label"),
                    ):
                        cleaned = _clean_profile_name(raw)
                        if cleaned and cleaned not in candidates:
                            candidates.append(cleaned)
                except StaleElementReferenceException:
                    continue
        except WebDriverException:
            continue

    # JS-backed extraction catches cases where Selenium .text is blank while
    # the browser has already painted the heading.
    try:
        js_headings = driver.execute_script("""
            return Array.from(document.querySelectorAll(
                'main h1, main .text-heading-xlarge, .pv-top-card h1, h1'
            )).filter(el => {
                const r = el.getBoundingClientRect();
                const s = window.getComputedStyle(el);
                return r.width > 0 && r.height > 0 &&
                       s.visibility !== 'hidden' && s.display !== 'none';
            }).map(el => el.innerText || el.textContent || '')
              .filter(Boolean);
        """)
        for raw in js_headings or []:
            cleaned = _clean_profile_name(raw)
            if cleaned and cleaned not in candidates:
                candidates.append(cleaned)
    except WebDriverException:
        pass

    if expected_name:
        expected = _clean_profile_name(expected_name)
        expected_folded = expected.casefold()

        # Prefer an exact visible heading match.
        for candidate in candidates:
            if candidate.casefold() == expected_folded:
                return candidate

        # LinkedIn's profile header can be a div/span rather than h1.
        # Check only the beginning of the main profile content, where the
        # profile identity card appears, rather than the entire page.
        try:
            top_text = driver.execute_script("""
                const root = document.querySelector('main') ||
                             document.querySelector('[role="main"]') ||
                             document.body;
                return (root && (root.innerText || root.textContent) || '')
                    .slice(0, 2200);
            """) or ""
            normalized_top = re.sub(r"\s+", " ", top_text).casefold()
            if expected_folded and expected_folded in normalized_top:
                return expected
        except WebDriverException:
            pass

    # Without an expected name, return the first plausible heading candidate.
    if candidates:
        return candidates[0]
    return ""


def detect_connection_degree(driver):
    """
    Read LinkedIn's visible connection-degree/accessibility labels.
    Return '1st', '2nd', '3rd', or '' when the page does not expose it.
    """
    candidates = []

    selectors = [
        "span.a11y-text",
        ".artdeco-entity-lockup__degree",
        ".distance-badge",
        "[aria-label*='degree connection']",
        "[aria-label*='connection']",
    ]
    for selector in selectors:
        for element in driver.find_elements(By.CSS_SELECTOR, selector):
            try:
                if element.is_displayed():
                    candidates.extend([
                        element.text or "",
                        element.get_attribute("aria-label") or "",
                        element.get_attribute("title") or "",
                    ])
            except StaleElementReferenceException:
                continue

    # Use accessible page text as a final fallback, but only look for explicit
    # LinkedIn degree wording; never infer degree from the existence of a button.
    try:
        candidates.append(driver.find_element(By.TAG_NAME, "body").text)
    except WebDriverException:
        pass

    for raw in candidates:
        text_value = (raw or "").lower()
        match = re.search(
            r"\b(1st|2nd|3rd)\s*(?:degree\s*)?connection\b|"
            r"\b(1st|2nd|3rd)\b",
            text_value,
        )
        if match:
            degree = match.group(1) or match.group(2)
            if degree in ("1st", "2nd", "3rd"):
                return degree

    return ""


def open_profile(driver, row):
    url = row["LinkedIn URL"].strip()
    expected_name = _clean_profile_name(row["Name"])
    if not valid_linkedin_url(url):
        return "INVALID_LINKEDIN_URL", ""

    print(f"\n[PROFILE] Opening: {url}")
    driver.get(url)

    try:
        WebDriverWait(driver, 30).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        logging.warning("document.readyState did not reach complete for %s", url)

    stop_code = inspect_page(driver)
    if stop_code:
        return stop_code, ""

    if "/in/" not in driver.current_url.lower():
        return "PROFILE_NOT_FOUND", ""

    # LinkedIn is a single-page application: document.readyState=complete
    # does not guarantee that the profile header has rendered. Wait for it.
    name_wait = int(CONFIG.get("profile_name_wait_seconds", 25))
    print(f"[PROFILE] Waiting up to {name_wait}s for profile name...")
    try:
        actual_name = WebDriverWait(
            driver, name_wait, poll_frequency=0.5
        ).until(lambda d: detect_profile_name(d, expected_name) or False)
    except TimeoutException:
        actual_name = ""

    if not actual_name:
        try:
            heading_debug = driver.execute_script("""
                return Array.from(document.querySelectorAll('h1, .text-heading-xlarge'))
                    .map(el => ({
                        text: el.innerText || el.textContent || '',
                        visible: !!(el.getBoundingClientRect().width &&
                                    el.getBoundingClientRect().height)
                    })).slice(0, 10);
            """)
            main_debug = driver.execute_script("""
                const root = document.querySelector('main') ||
                             document.querySelector('[role="main"]') ||
                             document.body;
                return (root && (root.innerText || root.textContent) || '').slice(0, 900);
            """)
        except WebDriverException:
            heading_debug, main_debug = [], ""
        logging.warning(
            "Profile name not detected after %ss. URL=%s page_title=%r "
            "headings=%r main_text=%r",
            name_wait, driver.current_url, driver.title,
            heading_debug, main_debug
        )
        print("[PROFILE DEBUG] Headings:", heading_debug)
        print("[PROFILE DEBUG] Main text preview:", repr(main_debug))
        return "PROFILE_NAME_NOT_DETECTED", ""

    if actual_name.casefold() != expected_name.casefold():
        logging.warning(
            "Profile name mismatch. Expected=%r, detected=%r, URL=%s",
            expected_name, actual_name, driver.current_url
        )
        return "NAME_MISMATCH", ""

    # The connection badge can render later than the heading, so wait for it
    # separately rather than failing after one immediate DOM read.
    degree_wait = int(CONFIG.get("connection_degree_wait_seconds", 15))
    print(
        f"[PROFILE] Name verified: {actual_name}. "
        f"Waiting up to {degree_wait}s for connection degree..."
    )
    try:
        degree = WebDriverWait(
            driver, degree_wait, poll_frequency=0.5
        ).until(lambda d: detect_connection_degree(d) or False)
    except TimeoutException:
        degree = ""

    if not degree:
        logging.warning(
            "Connection degree not detected after %ss for %s (%s)",
            degree_wait, actual_name, driver.current_url
        )
        return "DEGREE_NOT_DETECTED", ""

    print(
        f"[PROFILE VERIFIED] {actual_name} | "
        f"Connection degree: {degree} | {driver.current_url}"
    )
    return None, degree


# ============================================================
# 6. MESSAGE DOM INSPECTION / VERIFICATION
# Based on the LinkedIn HTML supplied in this conversation.
# ============================================================

POPUP_SELECTOR = ".msg-overlay-conversation-bubble"
RECIPIENT_SELECTOR = "h2.msg-overlay-bubble-header__title"
# LinkedIn changes these classes periodically. Keep the known selector first,
# but also recognize accessible contenteditable/textbox variants.
COMPOSER_SELECTORS = [
    ".msg-form__contenteditable[contenteditable='true']",
    "[contenteditable='true'][role='textbox']",
    "[contenteditable='true'][aria-label*='message' i]",
    "[contenteditable='true'][data-placeholder*='message' i]",
    "textarea[aria-label*='message' i]",
]
COMPOSER_SELECTOR = ", ".join(COMPOSER_SELECTORS)
MESSAGE_EVENT_SELECTOR = ".msg-s-event-listitem"
MESSAGE_BODY_SELECTOR = ".msg-s-event-listitem__body"
SENDER_SELECTOR = ".msg-s-message-group__name"
SENT_INDICATOR_SELECTOR = (
    ".msg-s-event-with-indicator__sending-indicator--sent"
)


class MessageActionError(RuntimeError):
    """Raised when the profile Message action cannot be opened and verified."""


def normalize_message(value):
    return re.sub(r"\s+", " ", value or "").strip()


def _visible(el):
    try:
        return el.is_displayed() and el.size.get("width", 0) > 0 and el.size.get("height", 0) > 0
    except (StaleElementReferenceException, WebDriverException):
        return False


def _conversation_root_for_composer(driver, expected_name=None):
    """Find a visible conversation container around a visible composer.

    LinkedIn's current markup may not use the older msg-overlay class. We
    therefore locate the editable composer first and walk its ancestors,
    requiring the expected recipient name to appear in the same container.
    """
    wanted = normalize_message(expected_name).casefold() if expected_name else ""
    for selector in COMPOSER_SELECTORS:
        try:
            composers = driver.find_elements(By.CSS_SELECTOR, selector)
        except WebDriverException:
            continue
        for composer in composers:
            if not _visible(composer):
                continue
            try:
                root = driver.execute_script("""
                    let n = arguments[0];
                    const wanted = (arguments[1] || '').toLowerCase();
                    for (let i = 0; n && i < 12; i++, n = n.parentElement) {
                      const text = (n.innerText || '').replace(/\\s+/g, ' ').trim();
                      const hasComposer = !!n.querySelector('[contenteditable=true], textarea, [role=textbox]');
                      if (hasComposer && (!wanted || text.toLowerCase().includes(wanted))) return n;
                    }
                    return null;
                """, composer, wanted)
                if root:
                    return root
            except WebDriverException:
                continue
    return None


def get_message_popup(driver, timeout=25, expected_name=None):
    def locate(d):
        wanted = normalize_message(expected_name).casefold() if expected_name else ""
        # First use the known LinkedIn conversation bubble when it identifies
        # the requested recipient (or when no recipient was supplied).
        for el in d.find_elements(By.CSS_SELECTOR, POPUP_SELECTOR):
            if not _visible(el):
                continue
            try:
                text = normalize_message(el.text).casefold()
                if not wanted or wanted in text:
                    return el
            except StaleElementReferenceException:
                continue
        # Fallback for changed LinkedIn markup: find a visible composer and
        # walk up to the nearest container that also contains the recipient.
        return _conversation_root_for_composer(d, expected_name)

    return WebDriverWait(driver, timeout).until(locate)


def inspect_message_popup(driver, expected_name):
    popup = get_message_popup(driver, expected_name=expected_name)
    actual_name = ""
    profile_url = ""

    # Prefer the historical title selector, then accessible heading variants.
    title_nodes = []
    for selector in [
        RECIPIENT_SELECTOR,
        "h1, h2, h3",
        "[role='heading']",
        "a[href*='/in/']",
    ]:
        try:
            title_nodes.extend(popup.find_elements(By.CSS_SELECTOR, selector))
        except WebDriverException:
            pass

    wanted = normalize_message(expected_name).casefold()
    for node in title_nodes:
        try:
            label = normalize_message(node.text)
            href = node.get_attribute("href") or ""
            if href and "/in/" in href:
                profile_url = href
            if label and wanted in label.casefold():
                actual_name = expected_name
                break
        except StaleElementReferenceException:
            continue

    # Last-resort verification: the expected name must be present in the same
    # visible container as the composer. Never accept a generic composer alone.
    if not actual_name:
        container_text = normalize_message(popup.text)
        if wanted and wanted in container_text.casefold():
            actual_name = expected_name

    if not actual_name:
        raise RuntimeError(
            f"Conversation recipient could not be verified as '{expected_name}'. "
            "The composer was not touched."
        )

    return popup, actual_name, profile_url


def close_all_message_popups(driver, timeout=12, max_popups=30):
    """Close every visible LinkedIn message overlay before opening a new one.

    Uses the supplied LinkedIn header-control markup. It repeatedly reacquires
    buttons because React may replace DOM nodes after each close. If any visible
    conversation close control remains, fail closed rather than risk composing
    in the wrong conversation.
    """
    close_selector = "button.msg-overlay-bubble-header__control"
    closed = 0
    deadline = time.time() + timeout

    while time.time() < deadline and closed < max_popups:
        candidates = []
        try:
            candidates = driver.find_elements(By.CSS_SELECTOR, close_selector)
        except WebDriverException:
            candidates = []

        target = None
        for button in candidates:
            try:
                if not _visible(button):
                    continue
                label = normalize_message(
                    (button.get_attribute("aria-label") or "") + " " +
                    (button.get_attribute("title") or "") + " " +
                    (button.text or "")
                ).casefold()
                # The supplied markup exposes this exact accessible text.
                if "close your conversation" in label:
                    target = button
                    break
            except (StaleElementReferenceException, WebDriverException):
                continue

        if target is None:
            break

        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center', inline:'nearest'});",
                target,
            )
            try:
                target.click()
            except WebDriverException:
                driver.execute_script("arguments[0].click();", target)
            closed += 1
            print(f"[POPUP CLEANUP] Closed message popup #{closed}.", flush=True)
            # Allow LinkedIn's UI to remove/re-render the overlay before
            # reacquiring the next close button.
            time.sleep(0.35)
        except (StaleElementReferenceException, WebDriverException) as exc:
            logging.warning("Could not close a message popup: %s", exc)
            time.sleep(0.25)

    remaining = []
    try:
        for button in driver.find_elements(By.CSS_SELECTOR, close_selector):
            try:
                if _visible(button):
                    label = normalize_message(
                        (button.get_attribute("aria-label") or "") + " " +
                        (button.get_attribute("title") or "") + " " +
                        (button.text or "")
                    )
                    if "close your conversation" in label.casefold():
                        remaining.append(label)
            except (StaleElementReferenceException, WebDriverException):
                continue
    except WebDriverException:
        pass

    if remaining:
        raise MessageActionError(
            "POPUP_CLEANUP_FAILED: Could not close all existing LinkedIn message "
            f"popups. Remaining controls: {remaining!r}. Queue stopped."
        )

    print(f"[POPUP CLEANUP] Complete. Closed {closed} existing popup(s).", flush=True)
    return closed


def open_message_popup_from_profile(driver, expected_name):
    """
    Find and click the actual profile Message action, then verify the expected
    conversation popup. If it cannot be located or opened, raise a stop error
    so the queue does not silently move on to the next profile.
    """
    wanted = normalize_message(expected_name).casefold()

    # Always clear old/minimized conversation overlays first. Never reuse an
    # existing popup: each queued recipient starts with a clean messaging UI.
    close_all_message_popups(driver)

    # Prefer LinkedIn's known profile-action selectors.
    selectors = [
        "button.pv-s-profile-actions--message",
        "a.pv-s-profile-actions--message",
        "button[aria-label='Message']",
        "button[aria-label^='Message ']",
        "a[aria-label='Message']",
        "a[aria-label^='Message ']",
    ]
    candidates = []
    seen = set()

    for selector in selectors:
        for element in driver.find_elements(By.CSS_SELECTOR, selector):
            try:
                if element.id not in seen and element.is_displayed() and element.is_enabled():
                    seen.add(element.id)
                    candidates.append(element)
            except StaleElementReferenceException:
                continue

    # Fallback: exact visible Message label on an actionable element.
    # Do not match navigation, "Messaging", or "Send a message" controls.
    scope = driver.find_elements(By.CSS_SELECTOR, "main")
    scope = scope[0] if scope else driver.find_element(By.TAG_NAME, "body")
    generic = scope.find_elements(
        By.XPATH,
        ".//*[self::button or self::a or @role='button']"
        "[normalize-space(string(.))='Message' "
        "or @aria-label='Message']"
    )
    for element in generic:
        try:
            if element.id not in seen and element.is_displayed() and element.is_enabled():
                seen.add(element.id)
                candidates.append(element)
        except StaleElementReferenceException:
            continue

    if not candidates:
        # Diagnostic: print actionable controls containing "message" so the
        # selector can be adapted from the actual page DOM if LinkedIn changed.
        diagnostic = []
        for element in scope.find_elements(
            By.XPATH, ".//*[self::button or self::a or @role='button']"
        ):
            try:
                if not element.is_displayed():
                    continue
                label = normalize_message(
                    (element.text or "") + " " +
                    (element.get_attribute("aria-label") or "")
                )
                if "message" in label.casefold():
                    diagnostic.append({
                        "tag": element.tag_name,
                        "text": label[:100],
                        "class": (element.get_attribute("class") or "")[:160],
                        "aria": element.get_attribute("aria-label") or "",
                    })
            except StaleElementReferenceException:
                continue

        logging.error("No profile Message action found. Candidate diagnostics: %r", diagnostic)
        raise MessageActionError(
            "MESSAGE_ACTION_FAILED: Could not locate a clickable profile Message action. "
            "Queue stopped to avoid moving through profiles without preparing messages. "
            f"Visible controls containing 'message': {diagnostic!r}"
        )

    # Reject Message links belonging to recommendation/search-result cards.
    # LinkedIn may render these inside <main>, so scoping to main alone is not
    # sufficient. A labelled link naming a different person is never valid.
    filtered_candidates = []
    rejected_candidates = []
    for element in candidates:
        try:
            aria = normalize_message(element.get_attribute("aria-label") or "")
            label = normalize_message(element.text)
            href = element.get_attribute("href") or ""
            # Accessible names such as "Message Pankuj Rathi" identify the
            # recipient; reject them unless they identify the requested person.
            if aria.casefold().startswith("message "):
                named_recipient = normalize_message(aria[8:]).strip(" ·")
                if named_recipient and wanted not in named_recipient.casefold():
                    rejected_candidates.append({"reason": "aria names another person",
                        "aria": aria, "href": href})
                    continue
            context = driver.execute_script("""
                const el = arguments[0];
                let n = el;
                const chain = [];
                for (let i = 0; n && i < 9; i++, n = n.parentElement) {
                  const txt = (n.innerText || '').trim().replace(/\\s+/g, ' ').slice(0, 500);
                  const cls = String(n.className || '');
                  chain.push({cls, text: txt});
                }
                const all = chain.map(x => x.text).join(' | ');
                const hasExpected = all.toLowerCase().includes(arguments[1]);
                const cardClass = chain.some(x => /entity-lockup|entity-result|reusable-search|feed-shared-update|discover-entity|artdeco-entity-lockup/i.test(x.cls));
                const degreeCard = chain.some(x => /·\\s*(1st|2nd|3rd)/i.test(x.text) && /message/i.test(x.text) && !x.text.toLowerCase().includes(arguments[1]));
                const card = cardClass || degreeCard;
                return {card, hasExpected, chain};
            """, element, wanted)
            if context.get("card"):
                rejected_candidates.append({"reason": "recommendation/result card",
                    "aria": aria, "href": href, "context": context["chain"][-1]["text"] if context.get("chain") else ""})
                continue
            filtered_candidates.append((element, context))
        except (StaleElementReferenceException, WebDriverException):
            continue

    if not filtered_candidates:
        raise MessageActionError(
            "MESSAGE_ACTION_FAILED: No safe profile-level Message button was identified for "
            f"{expected_name!r}. Rejected recommendation links: {rejected_candidates!r}. "
            "Queue stopped without opening a possibly wrong recipient."
        )
    candidates = [item[0] for item in filtered_candidates]

    # Choose the most specific LinkedIn profile action first.
    def score(element):
        try:
            cls = (element.get_attribute("class") or "").casefold()
            aria = (element.get_attribute("aria-label") or "").strip().casefold()
            text_value = normalize_message(element.text).casefold()
            value = 0
            if "pv-s-profile-actions--message" in cls:
                value -= 10
            if aria == "message" or text_value == "message":
                value -= 5
            # Never prefer a named link over the profile's unlabelled CTA.
            if aria.startswith("message "):
                value += 20
            return value
        except StaleElementReferenceException:
            return 100

    candidates.sort(key=score)
    target = candidates[0]
    logging.info(
        "Clicking profile Message action: tag=%s class=%r text=%r aria=%r",
        target.tag_name,
        target.get_attribute("class"),
        target.text,
        target.get_attribute("aria-label"),
    )
    print("[MESSAGE ACTION] Clicking profile Message button...")
    try:
        target_debug = driver.execute_script("""
            const el = arguments[0];
            const chain = [];
            let node = el;
            for (let i = 0; node && i < 5; i++, node = node.parentElement) {
              chain.push({tag: node.tagName, cls: String(node.className || '').slice(0,180),
                text: (node.innerText || '').trim().slice(0,260),
                href: node.getAttribute && node.getAttribute('href')});
            }
            const r = el.getBoundingClientRect();
            return {tag: el.tagName, text: (el.innerText || '').trim(),
              aria: el.getAttribute('aria-label') || '', href: el.getAttribute('href') || '',
              cls: String(el.className || ''), rect: {x:r.x,y:r.y,width:r.width,height:r.height},
              outerHTML: el.outerHTML.slice(0,1200), ancestor_chain: chain};
        """, target)
        print("[MESSAGE ACTION TARGET DEBUG]")
        print(json.dumps(target_debug, ensure_ascii=False, indent=2))
    except WebDriverException as debug_exc:
        print("[MESSAGE ACTION TARGET DEBUG ERROR]", repr(debug_exc))

    # Preserve the exact href from the verified profile-level action. If the
    # SPA click does not open the composer, this same recipient-specific URL is
    # the only permitted fallback; never search for another generic Message link.
    target_href = target.get_attribute("href") or ""
    if "/messaging/compose/" not in target_href or "recipient=" not in target_href:
        raise MessageActionError(
            "MESSAGE_ACTION_FAILED: The verified profile Message action did not expose "
            "a recipient-specific compose URL. Queue stopped."
        )

    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center', inline:'nearest'});",
        target
    )
    try:
        target.click()
    except (WebDriverException, StaleElementReferenceException) as exc:
        logging.warning("Profile Message click raised %r; trying its verified compose URL.", exc)

    # First allow the normal LinkedIn SPA click to open its overlay.
    def matching_popup(d):
        try:
            return get_message_popup(d, timeout=1, expected_name=expected_name)
        except TimeoutException:
            return False

    try:
        WebDriverWait(driver, 12, poll_frequency=0.5).until(matching_popup)
        return inspect_message_popup(driver, expected_name)
    except TimeoutException:
        # Some LinkedIn builds leave the profile page unchanged after the click.
        # Navigate to the exact href captured from this profile's own Message CTA.
        print("[MESSAGE ACTION] Click did not open composer; opening verified recipient-specific compose URL...")
        driver.get(target_href)

    try:
        WebDriverWait(driver, 35, poll_frequency=0.5).until(matching_popup)
        return inspect_message_popup(driver, expected_name)
    except TimeoutException as exc:
        # Capture the actual state after both the SPA click and exact-href fallback.
        diagnostic = {"url": driver.current_url, "title": driver.title,
                      "compose_href_used": target_href,
                      "fallback_navigation_attempted": True}
        try:
            diagnostic["visible_popups"] = driver.execute_script("""
                const selectors = [
                  '.msg-overlay-conversation-bubble',
                  '.msg-overlay-list-bubble',
                  '[role=dialog]',
                  '[aria-label*=conversation i]',
                  '[class*=msg-overlay]'
                ];
                const out = [];
                for (const sel of selectors) {
                  for (const el of document.querySelectorAll(sel)) {
                    const r = el.getBoundingClientRect();
                    const st = getComputedStyle(el);
                    if (r.width && r.height && st.visibility !== 'hidden' && st.display !== 'none') {
                      out.push({selector: sel, tag: el.tagName,
                        cls: String(el.className || '').slice(0,180),
                        aria: el.getAttribute('aria-label') || '',
                        text: (el.innerText || '').slice(0,500)});
                    }
                  }
                }
                return out.slice(-20);
            """)
            diagnostic["visible_composers"] = driver.execute_script("""
                return Array.from(document.querySelectorAll(
                  '[contenteditable=true], textarea, [role=textbox]'
                )).map(el => {
                  const r = el.getBoundingClientRect();
                  const st = getComputedStyle(el);
                  let p = el, chain = [];
                  for (let i=0; p && i<5; i++, p=p.parentElement) {
                    chain.push({tag:p.tagName, cls:String(p.className||'').slice(0,120),
                      text:(p.innerText||'').trim().replace(/\\s+/g,' ').slice(0,220)});
                  }
                  return {tag: el.tagName, cls: String(el.className || '').slice(0,160),
                    aria: el.getAttribute('aria-label') || '',
                    role: el.getAttribute('role') || '',
                    visible: !!(r.width && r.height) && st.visibility !== 'hidden' && st.display !== 'none',
                    ancestor_chain: chain};
                }).filter(x => x.visible).slice(-20);
            """)
            diagnostic["visible_headings"] = driver.execute_script("""
                return Array.from(document.querySelectorAll('h1,h2,h3,[role=heading]'))
                  .filter(el => {const r=el.getBoundingClientRect(),s=getComputedStyle(el);return r.width&&r.height&&s.display!=='none'&&s.visibility!=='hidden';})
                  .map(el => ({tag:el.tagName,text:(el.innerText||'').trim().slice(0,180),
                    cls:String(el.className||'').slice(0,140)})).slice(-30);
            """)
            diagnostic["visible_message_actions"] = driver.execute_script("""
                return Array.from(document.querySelectorAll('button,a,[role=button]'))
                  .filter(el => {
                    const r = el.getBoundingClientRect(), st = getComputedStyle(el);
                    const t = ((el.innerText || '') + ' ' + (el.getAttribute('aria-label') || '')).toLowerCase();
                    return r.width && r.height && st.visibility !== 'hidden' && st.display !== 'none' && t.includes('message');
                  }).slice(0,25).map(el => ({tag:el.tagName, text:(el.innerText||'').trim().slice(0,100),
                    aria:el.getAttribute('aria-label')||'', cls:String(el.className||'').slice(0,160)}));
            """)
        except WebDriverException as diag_exc:
            diagnostic["diagnostic_error"] = repr(diag_exc)
        logging.error("POST-CLICK MESSAGE DIAGNOSTIC: %s", diagnostic)
        print("\n[POST-CLICK MESSAGE DIAGNOSTIC]")
        print(json.dumps(diagnostic, ensure_ascii=False, indent=2))
        raise MessageActionError(
            "MESSAGE_ACTION_FAILED: Message was clicked, but the expected conversation "
            f"popup for {expected_name!r} was not detected. Diagnostic state printed above. "
            "Queue stopped."
        ) from exc


def paste_message_into_composer(driver, expected_name, message):
    """
    Insert the approved text into LinkedIn's open contenteditable composer.
    This function never presses Enter or clicks Send.
    Uses direct Selenium text entry first, then clipboard paste as fallback.
    """
    # Revalidate the conversation recipient immediately before touching composer.
    popup, actual_name, profile_url = inspect_message_popup(driver, expected_name)

    def visible_composer(d):
        # Re-query the DOM on each poll in case LinkedIn re-renders the popup.
        try:
            current_popup, verified_name, _ = inspect_message_popup(d, expected_name)
        except (TimeoutException, RuntimeError, StaleElementReferenceException):
            return False
        for selector in COMPOSER_SELECTORS:
            try:
                composers = current_popup.find_elements(By.CSS_SELECTOR, selector)
            except WebDriverException:
                continue
            for item in composers:
                if _visible(item) and item.is_enabled():
                    return current_popup, item
        return False

    result = WebDriverWait(driver, 25).until(visible_composer)
    popup, composer = result

    # Never overwrite a draft or partially composed message.
    existing_draft = normalize_message(
        composer.get_attribute("innerText")
        or composer.get_attribute("textContent")
        or composer.text
    )
    if existing_draft:
        if existing_draft == normalize_message(message):
            print("[MESSAGE] The exact message is already in the composer.")
            return popup, actual_name, profile_url
        raise RuntimeError(
            "The LinkedIn composer already contains text. "
            "Nothing was changed to avoid overwriting an existing draft. "
            f"Existing text begins: {existing_draft[:120]!r}"
        )

    expected = normalize_message(message)
    composer.click()

    # Method 1: Selenium sends text directly to the contenteditable element.
    # This avoids clipboard permission/focus issues in Chrome.
    try:
        #composer.send_keys(message)
        composer.send_keys(message)
        WebDriverWait(driver, 8).until(
            lambda d: any(
                normalize_message(
                    el.get_attribute("innerText")
                    or el.get_attribute("textContent")
                    or el.text
                ) == expected
                for el in d.find_elements(By.CSS_SELECTOR, COMPOSER_SELECTOR)
                if el.is_displayed()
            )
        )
        print("[MESSAGE] Text inserted into LinkedIn composer using Selenium.")
        print("[MESSAGE] Review it and click Send manually.")
        return popup, actual_name, profile_url
    except (TimeoutException, WebDriverException):
        logging.warning(
            "Direct text entry did not verify; attempting clipboard paste.",
            exc_info=True
        )

    # Method 2: clipboard paste fallback, then verify the actual composer text.
    # No Enter key or Send button is invoked.
    composer = visible_composer(driver)[1]
    pyperclip.copy(message)
    composer.click()
    composer.send_keys(Keys.CONTROL, "v")

    try:
        WebDriverWait(driver, 10).until(
            lambda d: any(
                normalize_message(
                    el.get_attribute("innerText")
                    or el.get_attribute("textContent")
                    or el.text
                ) == expected
                for el in d.find_elements(By.CSS_SELECTOR, COMPOSER_SELECTOR)
                if el.is_displayed()
            )
        )
    except TimeoutException as exc:
        # Leave the draft untouched if partial text was inserted; surface a
        # useful diagnostic rather than reporting a false success.
        current_values = []
        for el in driver.find_elements(By.CSS_SELECTOR, COMPOSER_SELECTOR):
            try:
                if el.is_displayed():
                    current_values.append(
                        el.get_attribute("innerText")
                        or el.get_attribute("textContent")
                        or el.text
                    )
            except StaleElementReferenceException:
                pass
        raise RuntimeError(
            "Message text could not be verified in LinkedIn's composer. "
            f"Composer currently contains: {current_values!r}. "
            "Check whether the composer is editable and focused."
        ) from exc

    print("[MESSAGE] Text pasted into LinkedIn composer using clipboard.")
    print("[MESSAGE] Review it and click Send manually.")
    return popup, actual_name, profile_url


def find_sent_message(driver, expected_message, expected_name=None):
    """Verify exact message in the requested recipient's conversation only."""
    expected = normalize_message(expected_message)
    popup = get_message_popup(driver, timeout=8, expected_name=expected_name)
    events = popup.find_elements(By.CSS_SELECTOR, MESSAGE_EVENT_SELECTOR)

    for event in events:
        try:
            # LinkedIn's current message DOM places sender and body inside each
            # event, while the sent marker is nested inside the event's
            # msg-s-event-with-indicator wrapper.
            sender_nodes = event.find_elements(By.CSS_SELECTOR, SENDER_SELECTOR)
            body_nodes = event.find_elements(By.CSS_SELECTOR, MESSAGE_BODY_SELECTOR)
            sent_markers = event.find_elements(By.CSS_SELECTOR, SENT_INDICATOR_SELECTOR)

            sender = normalize_message(sender_nodes[0].text) if sender_nodes else ""
            body = normalize_message(body_nodes[0].text) if body_nodes else ""
            # The event itself is rendered in the conversation history. Requiring
            # LinkedIn's CSS sent-indicator is brittle because its markup changes.
            # Exact sender + exact body is the primary evidence; indicator is metadata.
            if sender.casefold() == "deep patel".casefold() and body == expected:
                sent_at = sent_markers[0].get_attribute("title") or "" if sent_markers else ""
                return {
                    "verified": True,
                    "sender": sender,
                    "message": body,
                    "sent_at": sent_at,
                    "sent_indicator_present": bool(sent_markers),
                }
        except (NoSuchElementException, StaleElementReferenceException, WebDriverException):
            continue

    return {"verified": False}


def close_message_popup(driver):
    """Compatibility wrapper: close all open LinkedIn conversation overlays."""
    try:
        close_all_message_popups(driver)
        return True
    except (MessageActionError, WebDriverException) as exc:
        logging.warning("Popup cleanup did not complete: %s", exc)
        return False


def wait_for_manual_send_and_verify(driver, expected_message, expected_name=None, timeout=90):
    """
    The user sends the prepared message manually in LinkedIn.
    Selenium then checks the rendered conversation DOM for the exact text,
    the logged-in sender name, and LinkedIn's sent indicator.
    """
    print("\nSend the prepared message manually in LinkedIn.")
    print("Waiting for the exact sender + message text in conversation history...")
    print("Duplicate guard is active: an uncertain result will NOT be auto-retried.")

    deadline = time.time() + timeout
    while time.time() < deadline:
        stop_code = inspect_page(driver)
        if stop_code:
            return {"verified": False, "reason": stop_code}

        try:
            result = find_sent_message(driver, expected_message, expected_name=expected_name)
            if result.get("verified"):
                return result
        except (TimeoutException, WebDriverException):
            pass

        time.sleep(2)

    diagnostic = {}
    try:
        diagnostic = driver.execute_script("""
          const visible = el => !!(el && el.getBoundingClientRect().width &&
            el.getBoundingClientRect().height && getComputedStyle(el).visibility !== 'hidden');
          const selectors = ['.msg-overlay-conversation-bubble', '.msg-s-event-listitem',
            '.msg-s-message-group__name', '.msg-s-event-listitem__body',
            '.msg-s-event-with-indicator__sending-indicator--sent',
            '[contenteditable=true][role=textbox]'];
          const out = {url: location.href, title: document.title};
          for (const sel of selectors) {
            out[sel] = [...document.querySelectorAll(sel)].filter(visible).slice(-8).map(el => ({
              text: (el.innerText || el.textContent || '').trim().slice(0, 700),
              title: el.getAttribute('title') || '',
              aria: el.getAttribute('aria-label') || '',
              cls: typeof el.className === 'string' ? el.className : ''
            }));
          }
          return out;
        """)
    except WebDriverException as exc:
        diagnostic = {"diagnostic_error": str(exc)}
    print("\n[SENT VERIFICATION DEBUG]", json.dumps(diagnostic, ensure_ascii=False, indent=2), flush=True)
    logging.warning("Sent verification timeout diagnostic: %s", json.dumps(diagnostic, ensure_ascii=False))
    return {
        "verified": False,
        "reason": f"Message not verified within {timeout} seconds. See SENT VERIFICATION DEBUG.",
    }


# ============================================================
# 7. MESSAGE PROCESSING
# ============================================================

def process_message(driver, row):
    row_number = row["_sheet_row"]
    attempts = get_attempts(row) + 1
    message = row["Message Text"].strip()
    degree = ""
    guard_state = get_guard_state(row.get("Job ID", ""))
    if guard_state in {"SENT", "PREPARED", "VERIFY_PENDING", "DRAFT_ONLY"}:
        update_status(
            row_number, "Verification Pending", "DUPLICATE_GUARD_BLOCK",
            f"SendGuard state is {guard_state}; automatic re-preparation is blocked.",
            attempts,
        )
        return None

    if not message:
        update_status(
            row_number, "Skipped", "EMPTY_MESSAGE",
            "Message Text is empty.", attempts
        )
        return None

    update_status(
        row_number, "Processing", "PROCESSING",
        "Opening profile and preparing message for manual send.",
        attempts,
    )

    try:
        stop_code, degree = open_profile(driver, row)
        if stop_code:
            status = "Stopped" if stop_code == "USER_STOP" else "Failed"
            update_status(
                row_number, status, stop_code,
                "Automatic profile name/connection-degree verification failed: "+ stop_code, attempts, degree
            )
            return "USER_STOP" if stop_code == "USER_STOP" else stop_code

        if degree.lower() != "1st":
            update_status(
                row_number, "Skipped", "NOT_FIRST_DEGREE",
                "Message action is restricted to confirmed 1st-degree connections.",
                attempts, degree
            )
            return None

        print("\n" + "=" * 55)
        print("OPENING MESSAGE AND PREPARING DRAFT")
        print("=" * 55)
        print("Recipient:", row["Name"])
        print("Message type:", row["Message Type"])

        # Check the existing conversation first to avoid preparing a duplicate.
        popup, actual_name, conversation_url = open_message_popup_from_profile(
            driver, row["Name"]
        )

        existing = find_sent_message(driver, message, expected_name=row["Name"])
        if existing.get("verified"):
            update_status(
                row_number, "Skipped", "ALREADY_SENT",
                "Exact message already exists with LinkedIn sent indicator. "
                + existing.get("sent_at", ""),
                attempts, degree
            )
            close_message_popup(driver)
            return None

        # Durable fail-closed reservation BEFORE exposing the draft for manual send.
        # If Python/Chrome is interrupted after this point, the job cannot be resent
        # automatically on a later run, even if LinkedIn's sent DOM is not detected.
        set_send_guard_state(
            row, "PREPARED",
            "Draft is being prepared; manual send may occur. Do not retry automatically."
        )

        # Paste into the composer, but leave the actual Send action to the user.
        paste_message_into_composer(driver, row["Name"], message)

        #input(
        #    "\nAfter reviewing the recipient and draft, click Send manually "
        #    "in LinkedIn. Press ENTER here once you have sent it: "
        #)
        time.sleep(3)

        result = wait_for_manual_send_and_verify(driver, message, expected_name=row["Name"])

        if result.get("verified"):
            set_send_guard_state(
                row, "SENT",
                "Exact sender and message text found in recipient conversation history."
            )
            if not record_sent_job(row):
                update_status(
                    row_number, "Skipped", "ALREADY_SENT",
                    "Duplicate SentLedger entry detected.",
                    attempts, degree
                )
                close_message_popup(driver)
                return None

            update_status(
                row_number, "Sent", "SENT",
                "DOM verified exact message and LinkedIn sent indicator (not recipient delivery). "
                + result.get("sent_at", ""),
                attempts, degree
            )
            logging.info(
                "Message verified: job=%s recipient=%s sent_at=%s",
                row["Job ID"], row["Name"], result.get("sent_at", "")
            )
            close_message_popup(driver)
            return "SENT"

        reason = result.get("reason", "Message could not be verified.")
        set_send_guard_state(
            row, "VERIFY_PENDING",
            "Manual send was requested, but sent state was not verified. " + str(reason)
        )
        code = reason if reason in STOP_CODES else "VERIFICATION_PENDING"
        update_status(
            row_number, "Verification Pending", code,
            reason, attempts, degree
        )
        if code in STOP_CODES:
            return code
        return None

    except Exception as exc:
        logging.exception("Message job failed")
        # Once PREPARED has been written, never release the guard automatically:
        # the user may have sent the message immediately before an exception.
        if get_guard_state(row.get("Job ID", "")) == "PREPARED":
            set_send_guard_state(
                row, "VERIFY_PENDING",
                "Interrupted or failed after draft preparation. Check LinkedIn before any retry. " + str(exc)
            )
        if isinstance(exc, MessageActionError):
            update_status(
                row_number, "Failed", "MESSAGE_ACTION_FAILED",
                str(exc), attempts, degree
            )
            return "MESSAGE_ACTION_FAILED"
        update_status(
            row_number, "Failed", "ERROR", str(exc), attempts, degree
        )
        return None


# ============================================================
# 8. CONNECTION REQUEST PREPARATION (MANUAL)
# ============================================================

def _find_connect_action(driver, expected_name):
    """Find only the profile-level Connect action for the verified recipient."""
    wanted = normalize_message(expected_name).casefold()
    selectors = [
        "a[componentkey*='ConnectButtonstate:invitation']",
        "button[componentkey*='ConnectButtonstate:invitation']",
        "a[aria-label*='to connect' i]",
        "button[aria-label*='to connect' i]",
    ]
    found = []
    seen = set()
    for selector in selectors:
        for el in driver.find_elements(By.CSS_SELECTOR, selector):
            try:
                if el.id in seen or not _visible(el) or not el.is_enabled():
                    continue
                seen.add(el.id)
                aria = normalize_message(el.get_attribute("aria-label") or "")
                # Prefer the exact accessible label from LinkedIn's profile CTA.
                if aria.casefold() == f"invite {wanted} to connect":
                    found.append((0, el, aria))
                elif aria and "to connect" in aria.casefold() and wanted in aria.casefold():
                    found.append((1, el, aria))
            except (StaleElementReferenceException, WebDriverException):
                continue

    if not found:
        # Do not fall back to generic Connect labels: recommendation cards can
        # contain other people's buttons. Fail closed unless recipient is named.
        raise RuntimeError(
            f"CONNECT_ACTION_NOT_FOUND: Could not find a recipient-specific "
            f"'Invite {expected_name} to connect' button. No generic Connect button was clicked."
        )
    found.sort(key=lambda item: item[0])
    return found[0][1], found[0][2]


def _find_invitation_dialog(driver, expected_name, timeout=18):
    """Wait for a visible invitation dialog belonging to the expected profile."""
    wanted = normalize_message(expected_name).casefold()

    def locate(d):
        dialogs = d.find_elements(By.CSS_SELECTOR, "dialog[open], [role='dialog'], .artdeco-modal")
        for dialog in dialogs:
            try:
                if not _visible(dialog):
                    continue
                txt = normalize_message(dialog.text).casefold()
                has_invitation_ui = any(
                    phrase in txt for phrase in (
                        "add a note", "send without a note", "personalize", "invitation"
                    )
                )
                # Some LinkedIn dialog variants omit the recipient name from
                # the dialog text; in that case require the underlying profile
                # heading to still match the verified target.
                profile_matches = normalize_message(
                    detect_profile_name(d, expected_name)
                ).casefold() == wanted
                if has_invitation_ui and (wanted in txt or profile_matches):
                    return dialog
            except StaleElementReferenceException:
                continue
        return False

    return WebDriverWait(driver, timeout, poll_frequency=0.4).until(locate)


def _prepare_connection_note(driver, dialog, note):
    """Reveal the note field if needed, fill it, and verify exact contents."""
    expected = normalize_message(note)
    fields = [
        "textarea",
        "[contenteditable='true'][role='textbox']",
        "[contenteditable='true']",
    ]

    def find_field():
        for selector in fields:
            for el in dialog.find_elements(By.CSS_SELECTOR, selector):
                if _visible(el) and el.is_enabled():
                    return el
        return None

    field = find_field()
    if field is None:
        # LinkedIn often hides the note editor behind an Add a note button.
        for button in dialog.find_elements(By.CSS_SELECTOR, "button, a, [role='button']"):
            try:
                label = normalize_message((button.text or "") + " " + (button.get_attribute("aria-label") or ""))
                if _visible(button) and "add a note" in label.casefold():
                    button.click()
                    break
            except (StaleElementReferenceException, WebDriverException):
                continue
        WebDriverWait(driver, 8, poll_frequency=0.3).until(lambda d: find_field() or False)
        field = find_field()

    if field is None:
        raise RuntimeError("INVITATION_NOTE_FIELD_NOT_FOUND: Invitation dialog opened but note field was not found.")

    existing = normalize_message(
        field.get_attribute("value") or field.get_attribute("innerText") or
        field.get_attribute("textContent") or field.text
    )
    if existing:
        if existing == expected:
            print("[CONNECT] Exact note is already in the invitation dialog.", flush=True)
            return
        raise RuntimeError(
            "INVITATION_DIALOG_HAS_OTHER_TEXT: Existing note was not overwritten. "
            f"Current text begins: {existing[:100]!r}"
        )

    field.click()
    field.send_keys(note)

    def exact_note(_driver):
        try:
            current = normalize_message(
                field.get_attribute("value") or field.get_attribute("innerText") or
                field.get_attribute("textContent") or field.text
            )
            return current == expected
        except StaleElementReferenceException:
            return False

    WebDriverWait(driver, 8, poll_frequency=0.3).until(exact_note)
    print("[CONNECT] Invitation note inserted and verified.", flush=True)


def _find_invitation_send_button(driver, dialog):
    """Locate and validate the exact Send control in the active invite dialog.

    This deliberately does not click the button. The user reviews and submits
    the connection invitation manually.
    """
    candidates = dialog.find_elements(
        By.XPATH,
        ".//button[.//span[normalize-space()='Send'] or normalize-space()='Send']"
    )
    for button in candidates:
        try:
            if not _visible(button):
                continue
            label = normalize_message(button.text)
            if label != "Send":
                continue
            if not button.is_enabled() or (button.get_attribute("aria-disabled") or "").lower() == "true":
                continue
            print("[CONNECT] Exact invitation Send button found and enabled.", flush=True)
            return button
        except (StaleElementReferenceException, WebDriverException):
            continue

    raise RuntimeError(
        "INVITATION_SEND_BUTTON_NOT_FOUND: Could not identify an enabled button "
        "whose exact visible label is 'Send' inside the verified invitation dialog."
    )


def verify_connection_pending_on_profile(driver, row, timeout=25):
    """Reopen the exact queued profile and verify its recipient-specific Pending CTA.

    LinkedIn's current profile DOM exposes pending invitations as an anchor whose
    componentkey contains ConnectButtonstate:invitation:<member>_pending and whose
    aria-label says "Pending, click to withdraw invitation sent to <name>".
    """
    profile_url = (row.get("LinkedIn URL") or "").strip()
    expected_name = _clean_profile_name(row.get("Name", ""))
    wanted = normalize_message(expected_name).casefold()

    if not profile_url or "/in/" not in profile_url.lower() or not wanted:
        logging.warning("Cannot verify invitation: missing/invalid profile URL or recipient name.")
        return False

    print(f"[CONNECT VERIFY] Reopening profile for final verification: {profile_url}", flush=True)
    driver.get(profile_url)

    try:
        WebDriverWait(driver, 30, poll_frequency=0.5).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        logging.warning("Profile document did not reach readyState=complete during invite verification.")

    def matching_pending(d):
        try:
            # Verify the profile identity first. This prevents a Pending CTA on
            # a stale page or another profile from being accepted.
            actual = detect_profile_name(d, expected_name)
            if not actual or normalize_message(actual).casefold() != wanted:
                return False

            candidates = d.find_elements(
                By.CSS_SELECTOR,
                'a[componentkey*="ConnectButtonstate:invitation"][componentkey*="_pending"]'
            )
            for el in candidates:
                try:
                    if not _visible(el):
                        continue
                    componentkey = (el.get_attribute("componentkey") or "").casefold()
                    aria = normalize_message(el.get_attribute("aria-label") or "")
                    visible_text = normalize_message(el.text)
                    aria_folded = aria.casefold()

                    # Require all three signals from the supplied LinkedIn DOM:
                    # pending invitation component, Pending label, and recipient.
                    if "connectbuttonstate:invitation" not in componentkey or not componentkey.endswith("_pending"):
                        continue
                    if visible_text.casefold() != "pending":
                        continue
                    if not aria_folded.startswith("pending, click to withdraw invitation sent to "):
                        continue
                    if wanted not in aria_folded:
                        continue

                    logging.info(
                        "Verified Pending invitation on reopened profile: name=%r componentkey=%r aria=%r",
                        expected_name, componentkey, aria
                    )
                    print("[CONNECT VERIFY] Recipient-specific Pending button found:", aria, flush=True)
                    return el
                except (StaleElementReferenceException, WebDriverException):
                    continue
            return False
        except (StaleElementReferenceException, WebDriverException):
            return False

    try:
        WebDriverWait(driver, timeout, poll_frequency=0.5).until(matching_pending)
        return True
    except TimeoutException:
        diagnostic = []
        try:
            diagnostic = driver.execute_script("""
                const visible = el => { const r=el.getBoundingClientRect(), s=getComputedStyle(el);
                  return r.width>0 && r.height>0 && s.display!=='none' && s.visibility!=='hidden'; };
                return [...document.querySelectorAll('a[componentkey*="ConnectButtonstate:invitation"]')]
                  .filter(visible).map(el => ({
                    text: (el.innerText || '').trim(),
                    aria: el.getAttribute('aria-label') || '',
                    componentkey: el.getAttribute('componentkey') || ''
                  }));
            """) or []
        except WebDriverException:
            pass
        logging.warning(
            "Pending invitation not found after reopening profile. Expected=%r URL=%s candidates=%r",
            expected_name, driver.current_url, diagnostic
        )
        print("[CONNECT VERIFY] Pending button not found. Visible invitation actions:",
              json.dumps(diagnostic, ensure_ascii=False), flush=True)
        return False


def process_connect(driver, row):
    row_number = row["_sheet_row"]
    attempts = get_attempts(row) + 1
    note = row["Message Text"].strip()
    degree = ""
    job_id = row.get("Job ID", "").strip()
    guard_state = get_guard_state(job_id)

    if job_id in get_sent_job_ids() or guard_state in {"SENT", "PREPARED", "VERIFY_PENDING", "DRAFT_ONLY"}:
        update_status(
            row_number, "Verification Pending", "DUPLICATE_GUARD_BLOCK",
            f"Connection job already recorded or guarded ({guard_state or 'SentLedger'}). Check LinkedIn before any retry.",
            attempts,
        )
        return None

    update_status(
        row_number, "Processing", "PROCESSING",
        "Opening verified profile-level Connect action and preparing invitation note for manual submission.",
        attempts,
    )

    try:
        if not note:
            update_status(row_number, "Skipped", "EMPTY_NOTE", "Connection note is empty.", attempts)
            return None
        if len(note) > 300:
            update_status(row_number, "Skipped", "NOTE_TOO_LONG", f"Note contains {len(note)} characters.", attempts)
            return None
        if row["Message Type"].strip().lower() != "connect":
            update_status(row_number, "Skipped", "INVALID_MESSAGE_TYPE", "Expected Message Type Connect.", attempts)
            return None

        stop_code, degree = open_profile(driver, row)
        if stop_code:
            status = "Stopped" if stop_code == "USER_STOP" else "Failed"
            update_status(row_number, status, stop_code,
                          "Automatic profile name/connection-degree verification failed: " + stop_code,
                          attempts, degree)
            return "USER_STOP" if stop_code == "USER_STOP" else stop_code

        if degree.lower() == "1st":
            update_status(row_number, "Skipped", "ALREADY_CONNECTED", "Already a first-degree connection.", attempts, degree)
            return None

        # Clear messaging overlays before acting on the profile's Connect button.
        close_all_message_popups(driver)

        target, aria = _find_connect_action(driver, row["Name"])
        print("[CONNECT ACTION] Verified profile-level Connect target:", aria, flush=True)

        # Reserve the job BEFORE clicking Connect. If LinkedIn's UI behaves
        # unexpectedly (for example, submits immediately instead of opening a
        # dialog), the job is still blocked from automatic retry.
        set_send_guard_state(
            row, "PREPARED",
            "Verified Connect action is about to be clicked. A request may be initiated; do not retry automatically."
        )
        target.click()

        dialog = _find_invitation_dialog(driver, row["Name"], timeout=18)
        _prepare_connection_note(driver, dialog, note)
        send_button = _find_invitation_send_button(driver, dialog)
        send_button = dialog.find_element(
            By.XPATH,
            ".//button[.//span[normalize-space()='Send']]"
        )

        send_button.click                                  

        print("\nCONNECTION INVITATION READY")
        print("Recipient:", row["Name"])
        print("Characters:", len(note), "/ 300")
        print("Exact Send button detected:", normalize_message(send_button.text))
        print("Review the recipient and note in LinkedIn, then click Send manually.")
        # input("After clicking Send, press ENTER here so LinkedIn's Pending state can be checked: ")

        # Do not rely on the invitation modal's disappearance or stale page state.
        # Reopen the exact profile and verify LinkedIn's recipient-specific
        # ConnectButtonstate:invitation:<member>_pending control.
        pending = verify_connection_pending_on_profile(driver, row, timeout=30)
        if pending:
            set_send_guard_state(row, "SENT", "LinkedIn visibly showed the invitation as Pending after manual submission.")
            if not record_sent_job(row):
                update_status(row_number, "Skipped", "ALREADY_PENDING", "Duplicate ledger entry detected.", attempts, degree)
                return None
            update_status(row_number, "Sent", "INVITATION_PENDING",
                          "LinkedIn visibly showed Pending after manual submission.", attempts, degree)
            print("[CONNECT] Invitation verified as Pending.", flush=True)
            return "SENT"

        set_send_guard_state(row, "VERIFY_PENDING", "Manual submission was attempted but recipient-specific Pending button was not found after reopening the profile.")
        update_status(row_number, "Verification Pending", "INVITATION_NOT_VERIFIED",
                      "Reopened the exact profile but could not find its recipient-specific Pending button. Check LinkedIn before retrying; duplicate guard remains active.",
                      attempts, degree)
        return None

    except TimeoutException as exc:
        if get_guard_state(job_id) == "PREPARED":
            set_send_guard_state(row, "VERIFY_PENDING", "Invitation dialog or Pending state timed out. Check LinkedIn before retrying.")
            update_status(row_number, "Verification Pending", "INVITATION_VERIFY_TIMEOUT", str(exc), attempts, degree)
            return "CONNECT_VERIFICATION_PENDING"
        else:
            update_status(row_number, "Failed", "CONNECT_ACTION_FAILED", str(exc), attempts, degree)
        return None
    except Exception as exc:
        logging.exception("Connection job failed")
        if get_guard_state(job_id) == "PREPARED":
            set_send_guard_state(row, "VERIFY_PENDING", "Interrupted after invitation preparation. Check LinkedIn before retrying. " + str(exc))
            update_status(row_number, "Verification Pending", "INVITATION_VERIFY_PENDING", str(exc), attempts, degree)
            return "CONNECT_VERIFICATION_PENDING"
        else:
            update_status(row_number, "Failed", "ERROR", str(exc), attempts, degree)
        return None


# ============================================================
# 9. QUEUE STOPPING
# ============================================================

def stop_remaining_rows(reason, action_filter=None):
    print("\nSTOPPING QUEUED ROWS:", reason)
    for row in get_pending_rows():
        if action_filter and row["Action"].strip().lower() != action_filter:
            continue
        update_status(
            row["_sheet_row"], "Stopped", reason,
            "Stopped due to account restriction or manual stop.",
            get_attempts(row),
        )


# ============================================================
# 10. MAIN
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("LINKEDIN OUTBOX MANAGER — DUPLICATE-SAFE")
    print("=" * 60)
    print("Timezone:", CONFIG.get("timezone", "Asia/Kolkata"))
    print("Message daily cap:", MESSAGE_DAILY_LIMIT)
    print("Connection daily cap:", CONNECT_DAILY_LIMIT)
    print("Connection weekly cap:", CONNECT_WEEKLY_LIMIT)

    message_daily, _ = get_action_counts("message")
    connect_daily, connect_weekly = get_action_counts("connect")
    print("\nMessages sent today:", message_daily)
    print("Connections sent today:", connect_daily)
    print("Connections sent in rolling 7 days:", connect_weekly)

    pending = get_pending_rows()
    if not pending:
        print("No approved queued jobs.")
        return

    due = []
    for row in pending:
        action = row["Action"].strip().lower()
        if action not in ("message", "connect"):
            continue
        if action == "connect" and CONNECT_WEEKDAYS_ONLY and not is_weekday():
            continue
        try:
            if is_due(row["Not Before (IST)"]):
                due.append(row)
        except ValueError as exc:
            update_status(
                row["_sheet_row"], "Failed", "INVALID_SCHEDULE",
                str(exc), get_attempts(row) + 1
            )

    print("\nApproved queued:", len(pending))
    print("Eligible and due:", len(due))
    if not due:
        print("No eligible jobs currently due.")
        return

    driver = None
    try:
        driver = start_browser()
        if not wait_for_login(driver):
            stop_remaining_rows("LOGGED_OUT")
            return

        for index, original in enumerate(due, start=1):
            current = next(
                (r for r in load_rows()
                 if r["_sheet_row"] == original["_sheet_row"]),
                None,
            )
            if not current:
                continue
            if current["Status"].strip().lower() != "queued":
                continue
            if not is_approved(current["Approved"]):
                continue
            if not is_due(current["Not Before (IST)"]):
                continue

            action = current["Action"].strip().lower()

            if action == "message":
                sent_today, _ = get_action_counts("message")
                if sent_today >= MESSAGE_DAILY_LIMIT:
                    print("Message daily cap reached; remaining messages left queued.")
                    continue

            if action == "connect":
                if CONNECT_WEEKDAYS_ONLY and not is_weekday():
                    continue
                sent_today, sent_week = get_action_counts("connect")
                if sent_week >= CONNECT_WEEKLY_LIMIT:
                    stop_remaining_rows("WEEKLY_LIMIT", action_filter="connect")
                    break
                if sent_today >= CONNECT_DAILY_LIMIT:
                    print("Connection daily cap reached; remaining invites left queued.")
                    continue

            print("\n" + "=" * 55)
            print(f"JOB {index} OF {len(due)}")
            print("Job ID:", current["Job ID"])
            print("Action:", action)
            print("Recipient:", current["Name"])
            print("=" * 55)

            result = (
                process_message(driver, current)
                if action == "message"
                else process_connect(driver, current)
            )

            if result in STOP_CODES:
                stop_remaining_rows(result)
                break
            if result == "USER_STOP":
                stop_remaining_rows("USER_STOP")
                break

            # Keep the existing configured pacing between reviewed jobs.
            # This is not used to disguise or evade platform limits.
            pause = int(CONFIG.get("inter_job_pause_seconds", 0))
            if pause > 0 and index < len(due):
                time.sleep(pause)

        print("\nOutbox processing completed.")

    except KeyboardInterrupt:
        print("\nManual interruption.")
        stop_remaining_rows("USER_STOP")
    except Exception as exc:
        import traceback
        logging.exception("Main execution error")
        print(f"\n[ERROR] {exc!r}", flush=True)
        traceback.print_exc()
        print("\nLog file:", LOG_DIR / "outbox.log", flush=True)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    print("[STARTUP] Calling main()...", flush=True)
    main()
    print("[EXIT] Script finished.", flush=True)

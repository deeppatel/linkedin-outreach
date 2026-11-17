
import json
import logging
import random
import re
import time

from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import pyperclip

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException

from webdriver_manager.chrome import ChromeDriverManager


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

CONFIG = json.loads(
    (BASE_DIR / "config.json").read_text(encoding="utf-8")
)

TZ = ZoneInfo(CONFIG.get("timezone", "Asia/Kolkata"))

MESSAGE_DAILY_LIMIT = int(CONFIG.get("daily_limit", 22))
CONNECT_DAILY_LIMIT = int(CONFIG.get("connect_daily_limit", 20))
CONNECT_WEEKLY_LIMIT = int(CONFIG.get("connect_weekly_limit", 100))

MIN_DELAY = int(CONFIG.get("minimum_delay_seconds", 45))
MAX_DELAY = int(CONFIG.get("maximum_delay_seconds", 120))

HEADERS = [
    "Job ID",
    "Tracker Row",
    "Name",
    "LinkedIn URL",
    "Action",
    "Message Text",
    "Message Type",
    "Approved",
    "Not Before (IST)",
    "Status",
    "Processed At (IST)",
    "Result Code",
    "Result Detail",
    "Attempts",
    "Connection Degree"
]

STOP_CODES = {
    "WEEKLY_LIMIT",
    "LIMIT_REACHED",
    "CAPTCHA",
    "LOGGED_OUT"
}

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(LOG_DIR / "outbox.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ============================================================
# 2. GOOGLE SHEETS
# ============================================================

gc = gspread.service_account(
    filename=str(BASE_DIR / CONFIG["service_account_file"])
)

spreadsheet = gc.open_by_key(
    CONFIG["spreadsheet_id"]
)

sheet = spreadsheet.worksheet(
    CONFIG.get("worksheet_name", "Outbox")
)


def initialize_ledger():

    try:
        ledger = spreadsheet.worksheet("SentLedger")

    except gspread.WorksheetNotFound:

        ledger = spreadsheet.add_worksheet(
            title="SentLedger",
            rows=2000,
            cols=3
        )

        ledger.append_row([
            "Job ID",
            "Sent At (IST)",
            "Recipient"
        ])

    return ledger


ledger = initialize_ledger()


def load_rows():

    values = sheet.get_all_values()

    if not values:
        raise RuntimeError("Outbox is empty.")

    headers = values[0]

    missing = [
        field for field in HEADERS
        if field not in headers
    ]

    if missing:
        raise RuntimeError(
            f"Missing Outbox columns: {missing}"
        )

    rows = []

    for row_number, values_row in enumerate(
        values[1:], start=2
    ):

        values_row += [""] * (
            len(headers) - len(values_row)
        )

        record = {
            headers[i]: values_row[i]
            for i in range(len(headers))
        }

        record["_sheet_row"] = row_number

        rows.append(record)

    return rows


def update_status(
    row_number,
    status,
    code="",
    detail="",
    attempts=None,
    degree=None
):

    headers = sheet.row_values(1)

    values = {
        "Status": status,
        "Processed At (IST)": datetime.now(TZ).strftime(
            "%Y-%m-%d %H:%M"
        ),
        "Result Code": code,
        "Result Detail": str(detail)[:500]
    }

    if attempts is not None:
        values["Attempts"] = attempts

    if degree is not None:
        values["Connection Degree"] = degree

    updates = []

    for field, value in values.items():

        column = headers.index(field) + 1

        updates.append({
            "range": gspread.utils.rowcol_to_a1(
                row_number, column
            ),
            "values": [[value]]
        })

    sheet.batch_update(updates)


# ============================================================
# 3. SENT LEDGER
# ============================================================

def get_sent_job_ids():

    values = ledger.col_values(1)

    return {
        str(value).strip()
        for value in values[1:]
        if str(value).strip()
    }


def record_sent_job(row):

    job_id = row["Job ID"].strip()

    if job_id in get_sent_job_ids():
        return False

    ledger.append_row([
        job_id,
        datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        row["Name"]
    ])

    return True


def validate_job_ids(rows):

    ids = [
        row["Job ID"].strip()
        for row in rows
        if row["Job ID"].strip()
    ]

    if len(ids) != len(set(ids)):
        raise RuntimeError(
            "Duplicate Job IDs found in Outbox."
        )


def get_attempts(row):

    try:
        return int(row.get("Attempts", "") or 0)

    except (ValueError, TypeError):
        return 0


# ============================================================
# 4. DATE AND SCHEDULE VALIDATION
# ============================================================

def is_approved(value):

    return str(value).strip().lower() == "yes"


def parse_ist(value):

    if not str(value).strip():
        return None

    formats = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d"
    ]

    for fmt in formats:

        try:
            return datetime.strptime(
                value.strip(), fmt
            ).replace(tzinfo=TZ)

        except ValueError:
            continue

    raise ValueError(
        f"Invalid Not Before (IST): {value}"
    )


def is_due(value):

    scheduled = parse_ist(value)

    if scheduled is None:
        return True

    return datetime.now(TZ) >= scheduled


def is_weekday():

    return datetime.now(TZ).weekday() < 5


def valid_linkedin_url(url):

    return bool(
        re.match(
            r"^https://(?:www\.)?linkedin\.com/in/[^/\s]+/?$",
            url.strip(),
            re.IGNORECASE
        )
    )


# ============================================================
# 5. DAILY AND WEEKLY COUNTERS
# ============================================================

def get_action_counts(action):

    now = datetime.now(TZ)
    today = now.date()

    daily = 0
    weekly = 0

    for row in load_rows():

        if row["Action"].strip().lower() != action:
            continue

        if row["Status"].strip().lower() != "sent":
            continue

        try:

            processed = datetime.strptime(
                row["Processed At (IST)"].strip(),
                "%Y-%m-%d %H:%M"
            ).replace(tzinfo=TZ)

        except ValueError:
            continue

        age = now - processed

        if processed.date() == today:
            daily += 1

        if timedelta(0) <= age < timedelta(days=7):
            weekly += 1

    return daily, weekly


def get_pending_rows():

    rows = load_rows()

    validate_job_ids(rows)

    ledger_ids = get_sent_job_ids()

    pending = []

    for row in rows:

        if row["Status"].strip().lower() != "queued":
            continue

        if not is_approved(row["Approved"]):
            continue

        if row["Job ID"].strip() in ledger_ids:

            update_status(
                row["_sheet_row"],
                "Skipped",
                "ALREADY_SENT",
                "Job ID already exists in SentLedger.",
                get_attempts(row)
            )

            continue

        pending.append(row)

    return pending


# ============================================================
# 6. CHROME / SELENIUM
# ============================================================

def start_browser():

    profile = BASE_DIR / CONFIG["chrome_user_data_dir"]

    profile.mkdir(parents=True, exist_ok=True)

    options = Options()

    options.add_argument(
        f"--user-data-dir={profile.resolve()}"
    )

    options.add_argument("--start-maximized")

    # Selenium ChromeDriver fallback
    driver_path = ChromeDriverManager().install()

    service = Service(driver_path)

    driver = webdriver.Chrome(
        service=service,
        options=options
    )

    return driver


def wait_for_login(driver):

    driver.get("https://www.linkedin.com/feed/")

    print("\nLog in to LinkedIn manually if required.")

    input(
        "Press ENTER after your LinkedIn feed is open: "
    )

    url = driver.current_url.lower()

    if "/login" in url or "/checkpoint" in url:
        return False

    return True


def inspect_page(driver):

    url = driver.current_url.lower()

    if "/login" in url or "/checkpoint" in url:
        return "LOGGED_OUT"

    try:

        text = driver.find_element(
            By.TAG_NAME, "body"
        ).text.lower()

    except WebDriverException:

        return "ERROR"

    if (
        "captcha" in text
        or "security verification" in text
    ):
        return "CAPTCHA"

    if (
        "you've reached the weekly invitation limit" in text
        or "weekly invitation limit" in text
    ):
        return "WEEKLY_LIMIT"

    return None


# ============================================================
# 7. PROFILE VERIFICATION
# ============================================================

def open_profile(driver, row):

    driver.get(row["LinkedIn URL"].strip())

    WebDriverWait(driver, 25).until(
        lambda d: d.execute_script(
            "return document.readyState"
        ) == "complete"
    )

    time.sleep(2)

    stop_code = inspect_page(driver)

    if stop_code:
        return stop_code, ""

    if "/in/" not in driver.current_url.lower():
        return "PROFILE_NOT_FOUND", ""

    print("\n" + "=" * 55)
    print("PROFILE VERIFICATION")
    print("=" * 55)

    print("Expected name:", row["Name"])
    print("Profile:", row["LinkedIn URL"])
    print("Job ID:", row["Job ID"])

    answer = input(
        "\nCorrect person's profile? "
        "(Y = Yes, N = Name mismatch, X = Stop): "
    ).strip().upper()

    if answer == "N":
        return "NAME_MISMATCH", ""

    if answer == "X":
        return "USER_STOP", ""

    if answer != "Y":
        return "ERROR", ""

    degree = input(
        "Connection degree (1st / 2nd / 3rd): "
    ).strip()

    if degree.lower() not in ("1st", "2nd", "3rd"):
        degree = ""

    return None, degree


# ============================================================
# 8. MESSAGE PREPARATION
# ============================================================

def prepare_message(row):

    message = row["Message Text"].strip()

    if not message:
        raise ValueError("Message Text is empty.")

    pyperclip.copy(message)

    print("\n" + "=" * 55)
    print("MESSAGE READY")
    print("=" * 55)

    print("Recipient:", row["Name"])
    print("Type:", row["Message Type"])
    print("\n", message)

    print("\nCopied to clipboard.")

    print("\nIn Chrome:")
    print("1. Open the Message option.")
    print("2. Paste the message.")
    print("3. Review and send manually.")
    print("4. Verify the message in the conversation.")

    return message


# ============================================================
# 9. CONNECTION NOTE PREPARATION
# ============================================================

def prepare_connection_note(row):

    note = row["Message Text"].strip()

    if len(note) > 300:
        raise ValueError("NOTE_TOO_LONG")

    if not note:
        raise ValueError("Connection note is empty.")

    pyperclip.copy(note)

    print("\n" + "=" * 55)
    print("CONNECTION REQUEST NOTE")
    print("=" * 55)

    print("Recipient:", row["Name"])
    print("Message type:", row["Message Type"])
    print("Characters:", len(note))
    print("Remaining:", 300 - len(note))

    print("\n" + note)

    print("\nCopied to clipboard.")

    print("\nIn Chrome:")
    print("1. Locate Connect or More on the profile.")
    print("2. Choose Add a note if available.")
    print("3. Paste and review the note.")
    print("4. Send manually.")
    print("5. Verify that the button now shows Pending.")

    return note


# ============================================================
# 10. PROCESS MESSAGE
# ============================================================

def process_message(driver, row):

    row_number = row["_sheet_row"]
    attempts = get_attempts(row) + 1

    update_status(
        row_number,
        "Processing",
        "PROCESSING",
        "Message awaiting manual review.",
        attempts
    )

    try:

        stop_code, degree = open_profile(driver, row)

        if stop_code:

            if stop_code == "USER_STOP":

                update_status(
                    row_number, "Stopped", "ERROR",
                    "User stopped execution.",
                    attempts
                )

                return "USER_STOP"

            update_status(
                row_number,
                "Failed",
                stop_code,
                "Profile verification failed.",
                attempts,
                degree
            )

            if stop_code in STOP_CODES:
                return stop_code

            return None

        message = prepare_message(row)

        result = input(
            "\nS = Sent and visible in conversation\n"
            "A = Already sent\n"
            "N = Not sent / Not verified\n"
            "X = Stop\nChoice: "
        ).strip().upper()

        if result == "X":
            update_status(
                row_number, "Stopped", "ERROR",
                "User stopped execution.",
                attempts, degree
            )
            return "USER_STOP"

        if result == "A":
            update_status(
                row_number, "Skipped", "ALREADY_SENT",
                "Message already exists in conversation.",
                attempts, degree
            )
            return None

        if result == "S":

            if not record_sent_job(row):
                update_status(
                    row_number, "Skipped", "ALREADY_SENT",
                    "Duplicate ledger entry.",
                    attempts, degree
                )
                return None

            update_status(
                row_number, "Sent", "SENT",
                message[:60],
                attempts, degree
            )

            return "SENT"

        update_status(
            row_number, "Failed", "ERROR",
            "Message not verified.",
            attempts, degree
        )

        return None

    except Exception as exc:

        logging.exception("Message job failed")

        update_status(
            row_number, "Failed", "ERROR",
            str(exc), attempts
        )

        return None


# ============================================================
# 11. PROCESS CONNECTION REQUEST
# ============================================================

def process_connect(driver, row):

    row_number = row["_sheet_row"]
    attempts = get_attempts(row) + 1

    update_status(
        row_number,
        "Processing",
        "PROCESSING",
        "Connection request awaiting review.",
        attempts
    )

    try:

        note = row["Message Text"].strip()

        if len(note) > 300:

            update_status(
                row_number,
                "Skipped",
                "NOTE_TOO_LONG",
                f"Note contains {len(note)} characters.",
                attempts
            )

            return None

        if not note:

            update_status(
                row_number, "Skipped", "ERROR",
                "Connection note is empty.",
                attempts
            )

            return None

        if row["Message Type"].strip().lower() != "connect":

            update_status(
                row_number, "Skipped", "ERROR",
                "Expected Message Type Connect.",
                attempts
            )

            return None

        stop_code, degree = open_profile(driver, row)

        if stop_code:

            if stop_code == "USER_STOP":

                update_status(
                    row_number, "Stopped", "ERROR",
                    "User stopped execution.",
                    attempts
                )

                return "USER_STOP"

            update_status(
                row_number,
                "Failed",
                stop_code,
                "Profile verification failed.",
                attempts,
                degree
            )

            if stop_code in STOP_CODES:
                return stop_code

            return None

        if degree.lower() == "1st":

            update_status(
                row_number,
                "Skipped",
                "ALREADY_CONNECTED",
                "Already a first-degree connection.",
                attempts,
                degree
            )

            return None

        print("\nCheck connection state in Chrome.")

        state = input(
            "P = Already Pending\n"
            "C = Connect option available\n"
            "F = Connect unavailable\n"
            "W = Weekly limit shown\n"
            "X = Stop\nChoice: "
        ).strip().upper()

        if state == "P":

            update_status(
                row_number,
                "Skipped",
                "ALREADY_PENDING",
                "Existing invitation is pending.",
                attempts,
                degree
            )

            return None

        if state == "F":

            update_status(
                row_number,
                "Skipped",
                "CONNECT_NOT_AVAILABLE",
                "Only Follow or Message is available.",
                attempts,
                degree
            )

            return None

        if state == "W":

            update_status(
                row_number,
                "Failed",
                "WEEKLY_LIMIT",
                "Weekly invitation limit displayed.",
                attempts,
                degree
            )

            return "WEEKLY_LIMIT"

        if state == "X":

            update_status(
                row_number, "Stopped", "ERROR",
                "User stopped execution.",
                attempts, degree
            )

            return "USER_STOP"

        if state != "C":
            update_status(
                row_number, "Failed", "ERROR",
                "Invalid connection state.",
                attempts, degree
            )
            return None

        prepare_connection_note(row)

        result = input(
            "\nAfter manually sending:\n"
            "P = Button now shows Pending\n"
            "N = Not sent / Not verified\n"
            "C = CAPTCHA\n"
            "L = Logged out\n"
            "W = Weekly limit\n"
            "X = Stop\nChoice: "
        ).strip().upper()

        if result in ("C", "L", "W"):

            code = {
                "C": "CAPTCHA",
                "L": "LOGGED_OUT",
                "W": "WEEKLY_LIMIT"
            }[result]

            update_status(
                row_number, "Failed", code,
                "Account restriction encountered.",
                attempts, degree
            )

            return code

        if result == "X":

            update_status(
                row_number, "Stopped", "ERROR",
                "User stopped execution.",
                attempts, degree
            )

            return "USER_STOP"

        if result == "P":

            if not record_sent_job(row):

                update_status(
                    row_number,
                    "Skipped",
                    "ALREADY_PENDING",
                    "Duplicate ledger entry.",
                    attempts,
                    degree
                )

                return None

            update_status(
                row_number,
                "Sent",
                "SENT",
                "Pending confirmed",
                attempts,
                degree
            )

            logging.info(
                "Connection confirmed: %s",
                row["Job ID"]
            )

            return "SENT"

        update_status(
            row_number,
            "Failed",
            "ERROR",
            "Pending state not confirmed.",
            attempts,
            degree
        )

        return None

    except Exception as exc:

        logging.exception("Connection job failed")

        update_status(
            row_number,
            "Failed",
            "ERROR",
            str(exc),
            attempts
        )

        return None


# ============================================================
# 12. STOP QUEUE
# ============================================================

def stop_remaining_rows(reason):

    print("\nSTOPPING QUEUED ROWS:", reason)

    for row in get_pending_rows():

        update_status(
            row["_sheet_row"],
            "Stopped",
            reason,
            "Stopped due to account restriction or manual stop.",
            get_attempts(row)
        )


# ============================================================
# 13. MAIN EXECUTION
# ============================================================

def main():

    print("\n" + "=" * 60)
    print("LINKEDIN OUTBOX MANAGER")
    print("=" * 60)

    print("Timezone:", CONFIG.get("timezone"))
    print("Message daily cap:", MESSAGE_DAILY_LIMIT)
    print("Connection daily cap:", CONNECT_DAILY_LIMIT)
    print("Connection weekly cap:", CONNECT_WEEKLY_LIMIT)

    message_daily, _ = get_action_counts("message")
    connect_daily, connect_weekly = get_action_counts("connect")

    print("\nMessages sent today:", message_daily)
    print("Connections sent today:", connect_daily)
    print("Connections sent in rolling 7 days:", connect_weekly)

    if connect_weekly >= CONNECT_WEEKLY_LIMIT:

        stop_remaining_rows("WEEKLY_LIMIT")

        print("Weekly connection cap reached.")
        return

    pending = get_pending_rows()

    if not pending:

        print("No approved queued jobs.")
        return

    due = []

    for row in pending:

        action = row["Action"].strip().lower()

        if action not in ("message", "connect"):
            continue

        if action == "connect" and not is_weekday():
            continue

        try:

            if is_due(row["Not Before (IST)"]):
                due.append(row)

        except ValueError as exc:

            update_status(
                row["_sheet_row"],
                "Failed",
                "ERROR",
                str(exc),
                get_attempts(row) + 1
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

        for index, original_row in enumerate(due):

            current_rows = load_rows()

            row = next(
                (
                    r for r in current_rows
                    if r["_sheet_row"] == original_row["_sheet_row"]
                ),
                None
            )

            if not row:
                continue

            if row["Status"].strip().lower() != "queued":
                continue

            if not is_approved(row["Approved"]):
                continue

            if not is_due(row["Not Before (IST)"]):
                continue

            action = row["Action"].strip().lower()

            # ----------------------------------------
            # MESSAGE LIMIT
            # ----------------------------------------

            if action == "message":

                message_daily, _ = get_action_counts("message")

                if message_daily >= MESSAGE_DAILY_LIMIT:

                    print("Message daily cap reached.")
                    continue

            # ----------------------------------------
            # CONNECTION LIMITS
            # ----------------------------------------

            if action == "connect":

                if not is_weekday():
                    continue

                connect_daily, connect_weekly = get_action_counts(
                    "connect"
                )

                if connect_weekly >= CONNECT_WEEKLY_LIMIT:

                    stop_remaining_rows("WEEKLY_LIMIT")
                    break

                if connect_daily >= CONNECT_DAILY_LIMIT:

                    print("Connection daily cap reached.")
                    continue

            print("\n" + "=" * 55)
            print(f"JOB {index + 1} OF {len(due)}")
            print("Job ID:", row["Job ID"])
            print("Action:", action)
            print("Recipient:", row["Name"])
            print("=" * 55)

            if action == "message":

                result = process_message(driver, row)

            else:

                result = process_connect(driver, row)

            # ----------------------------------------
            # GLOBAL STOP CONDITIONS
            # ----------------------------------------

            if result in STOP_CODES:

                stop_remaining_rows(result)
                break

            if result == "USER_STOP":

                stop_remaining_rows("ERROR")
                break

        print("\nOutbox processing completed.")

    except KeyboardInterrupt:

        print("\nManual interruption.")

        stop_remaining_rows("ERROR")

    except Exception:

        logging.exception("Main execution error")

        print("\nExecution error. Check logs/outbox.log.")

    finally:

        if driver:

            try:
                driver.quit()

            except Exception:
                pass


# ============================================================
# 14. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

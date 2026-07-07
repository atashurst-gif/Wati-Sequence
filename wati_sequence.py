"""
WATI WhatsApp Sequence Automation
- Reads leads from Google Sheet (Sheet1)
- Sends templated WhatsApp messages via WATI API
- Tracks progress in 'WATI Tracking' tab
- Sending windows: Mon-Thu 9am-6pm, Fri 9am-2pm, no Sat/Sun
- Sends at random times within windows
- Only processes UKDT CT, BST, UKDT O campaigns
- Skips leads added before 15/04/2026 (cutoff date)
- Emails alert if service crashes
- Webhook receives WATI replies and marks leads as replied
"""

import os
import re
import json
import time
import base64
import random
import logging
import smtplib
import sys
import datetime
import threading
from email.mime.text import MIMEText
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ─────────────────────────────────────────────
# Startup — decode credentials from env
# ─────────────────────────────────────────────

load_dotenv()

_creds_b64 = os.getenv('GOOGLE_CREDENTIALS_B64', '')
_token_b64  = os.getenv('GOOGLE_TOKEN_B64', '')

if _creds_b64:
    try:
        # Add padding to ensure valid base64
        padded = _creds_b64 + '=' * (-len(_creds_b64) % 4)
        with open('credentials.json', 'wb') as f:
            f.write(base64.b64decode(padded))
    except Exception as e:
        print(f"Warning: Could not decode credentials: {e}")

if _token_b64:
    try:
        padded = _token_b64 + '=' * (-len(_token_b64) % 4)
        with open('token.json', 'wb') as f:
            f.write(base64.b64decode(padded))
    except Exception as e:
        print(f"Warning: Could not decode token: {e}")

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("wati_sequence.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────

SPREADSHEET_ID    = os.getenv("SPREADSHEET_ID")
SHEET_NAME        = os.getenv("SHEET_NAME", "Sheet1")
TRACKING_SHEET     = "WATI Tracking"
FLT_TRACKING_SHEET = "FLT Tracking"

def get_tracking_tab(tl_ref: str) -> str:
    return FLT_TRACKING_SHEET if str(tl_ref).upper().startswith("FLT-") else TRACKING_SHEET
WATI_API_URL      = os.getenv("WATI_API_URL")
WATI_TOKEN        = os.getenv("WATI_TOKEN")
POLL_INTERVAL     = int(os.getenv("WATI_POLL_INTERVAL", "300"))
WEBHOOK_PORT      = int(os.getenv("WEBHOOK_PORT", "8080"))
ALERT_EMAIL       = os.getenv("ALERT_EMAIL", "atashurst@gmail.com")
CREDENTIALS_FILE  = os.getenv("CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE        = os.getenv("TOKEN_FILE", "token.json")

# Only process leads added on or after this date
CUTOFF_DATE = datetime.datetime(2026, 4, 15)

# Only these campaigns get WhatsApp sequences
ALLOWED_CAMPAIGNS = {"ukdt ct", "bst", "ukdt o", "flt", "ukdt ct2"}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
MAX_SENDS_PER_CYCLE = int(os.getenv("MAX_SENDS_PER_CYCLE", "30"))  # throttle backlog drain
MAX_SENDS_PER_DAY = int(os.getenv("MAX_SENDS_PER_DAY", "80"))  # hard daily ceiling — flattens Monday backlog spike
_daily_sends = {}  # {date: count} — persists across cycles, auto-resets each day

UK_TZ = ZoneInfo("Europe/London")

def _today_sent():
    """How many sends so far today (UK date). Auto-prunes old days."""
    today = datetime.datetime.now(UK_TZ).date()
    for d in list(_daily_sends):
        if d != today:
            del _daily_sends[d]
    return _daily_sends.get(today, 0)

def _bump_today():
    today = datetime.datetime.now(UK_TZ).date()
    _daily_sends[today] = _daily_sends.get(today, 0) + 1


def _sync_today_from_tracking(tracking: dict):
    """Seed the daily cap from persisted tracking so redeploys cannot reset it."""
    today = datetime.datetime.now(UK_TZ).date()
    sent_today = 0
    for item in tracking.values():
        last_sent = item.get("last_sent") or ""
        if not last_sent:
            continue
        try:
            sent_date = datetime.datetime.strptime(last_sent, "%d/%m/%Y %H:%M").date()
        except ValueError:
            continue
        if sent_date == today:
            sent_today += 1
    _daily_sends[today] = max(_daily_sends.get(today, 0), sent_today)

HC_PING_URL = os.getenv("HC_PING_URL", "https://hc-ping.com/a44b8422-f5b3-44d5-9b48-3e1d30acf187")
HEALTHCHECK_HEARTBEAT_SECONDS = int(os.getenv("HEALTHCHECK_HEARTBEAT_SECONDS", "240"))
_read_failed = False  # set True by read-error handlers; checked at cycle end
_healthcheck_heartbeat_started = False

def ping(suffix=""):
    """Ping healthcheck. Keep this as liveness; detailed errors stay in logs/email."""
    if not HC_PING_URL:
        return
    try:
        requests.get(HC_PING_URL + suffix, timeout=10)
    except Exception:
        pass

def start_healthcheck_heartbeat():
    """Ping during long processing cycles so Healthchecks does not flap."""
    global _healthcheck_heartbeat_started
    if _healthcheck_heartbeat_started or not HC_PING_URL:
        return
    _healthcheck_heartbeat_started = True

    def _loop():
        while True:
            time.sleep(HEALTHCHECK_HEARTBEAT_SECONDS)
            ping()

    threading.Thread(target=_loop, daemon=True).start()

# ─────────────────────────────────────────────
# Sequence Definitions
# ─────────────────────────────────────────────

UKDT_SEQUENCE = [
    {"step": 1, "template": "ukdt_nc1", "delay_hours": 0},
    {"step": 2, "template": "ukdt_nc2", "delay_hours": 24},
    {"step": 3, "template": "ukdt_nc3", "delay_hours": 48},
    {"step": 4, "template": "ukdt_nc4", "delay_hours": 72},
    {"step": 5, "template": "ukdt_nc5", "delay_hours": 120},
    {"step": 6, "template": "ukdt_nc6", "delay_hours": 168},
    {"step": 7, "template": "ukdt_nc7", "delay_hours": 216},
]

BST_SEQUENCE = [
    {"step": 1, "template": "bst_nc1",  "delay_hours": 0},
    {"step": 2, "template": "bst_nc2",  "delay_hours": 24},
    {"step": 3, "template": "bst_nc3",  "delay_hours": 48},
    {"step": 4, "template": "bst_nc4",  "delay_hours": 72},
    {"step": 5, "template": "bst_nc_5", "delay_hours": 120},
    {"step": 6, "template": "bst_nc_6", "delay_hours": 168},
    {"step": 7, "template": "bst_nc7",  "delay_hours": 216},
    {"step": 8, "template": "bst_nc8",  "delay_hours": 288},
]

STOPPED_STATUSES = {
    "replied", "completed", "opted out", "converted",
    "do not contact", "dnc", "dnq", "callback", "interested",
    "agreed", "lead passed", "verified", "moc set", "moc approved",
    "cbna", "callback set", "call back", "cancelled money wellness",
    "cancelled not interested (provide reason in notes)",
    "transferred to non ip",
}
BOOKING_PENDING_STATUS = "booking pending"
BOOKING_SEQUENCE_ACTIVE_STATUS = "sequence active"
BOOKING_PENDING_DELAY_HOURS = int(os.getenv("BOOKING_PENDING_DELAY_HOURS", "4"))

# ─────────────────────────────────────────────
# Sending Window Logic
# ─────────────────────────────────────────────

def get_next_send_time(now: datetime.datetime) -> datetime.datetime:
    """
    Returns the next valid send time based on:
    - Mon-Thu: 9am-6pm
    - Fri: 9am-2pm
    - Sat/Sun: no sending, queue until Monday 9am
    Time is randomised within the window.
    All times are UK local time.
    """
    import zoneinfo
    uk_tz = zoneinfo.ZoneInfo("Europe/London")

    # Convert to UK time
    if now.tzinfo is None:
        uk_now = now.replace(tzinfo=uk_tz)
    else:
        uk_now = now.astimezone(uk_tz)

    def window_for_day(dt):
        """Returns (open_hour, close_hour) for a given weekday, or None if closed."""
        wd = dt.weekday()  # 0=Mon, 6=Sun
        if wd <= 3:   # Mon-Thu
            return (9, 18)
        elif wd == 4: # Fri
            return (9, 14)
        else:         # Sat/Sun
            return None

    # Check if current time is within today's window
    window = window_for_day(uk_now)
    if window:
        open_h, close_h = window
        if open_h <= uk_now.hour < close_h:
            # We're within the window — send at a random time in remaining window
            # to avoid all messages sending at exactly the same time
            remaining_mins = (close_h - uk_now.hour) * 60 - uk_now.minute
            if remaining_mins > 30:
                # Send within next 30 mins randomly
                delay_mins = random.randint(1, 30)
                return uk_now + datetime.timedelta(minutes=delay_mins)
            else:
                return uk_now  # Send now if window closing soon

    # Find the next valid window
    candidate = uk_now
    for _ in range(8):  # max 8 days ahead (covers weekend)
        candidate = candidate + datetime.timedelta(days=1)
        candidate = candidate.replace(hour=0, minute=0, second=0, microsecond=0)
        window = window_for_day(candidate)
        if window:
            open_h, close_h = window
            # Pick a random time within the window
            rand_mins = random.randint(0, (close_h - open_h) * 60 - 1)
            send_time = candidate.replace(hour=open_h, minute=0) + datetime.timedelta(minutes=rand_mins)
            return send_time

    return uk_now  # fallback


def is_within_sending_window() -> bool:
    """Returns True if current UK time is within a valid sending window."""
    import zoneinfo
    uk_now = datetime.datetime.now(zoneinfo.ZoneInfo("Europe/London"))
    wd = uk_now.weekday()
    h  = uk_now.hour

    if wd <= 3:   # Mon-Thu: 9am-6pm
        return 9 <= h < 18
    elif wd == 4: # Fri: 9am-2pm
        return 9 <= h < 14
    else:         # Sat/Sun: never
        return False

# ─────────────────────────────────────────────
# Alert Email
# ─────────────────────────────────────────────

def send_alert_email(subject: str, body: str):
    """Send an alert email when something goes wrong."""
    try:
        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER", "")
        smtp_pass = os.getenv("SMTP_PASS", "")

        if not smtp_user or not smtp_pass:
            log.warning("SMTP not configured — cannot send alert email.")
            return

        msg = MIMEText(body)
        msg['Subject'] = f"⚠️ WATI Automation Alert: {subject}"
        msg['From']    = smtp_user
        msg['To']      = ALERT_EMAIL

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        log.info(f"Alert email sent: {subject}")
    except Exception as e:
        log.error(f"Failed to send alert email: {e}")

# ─────────────────────────────────────────────
# Google Auth
# ─────────────────────────────────────────────

def get_google_credentials():
    """Service-account auth (read/write). Replaces expiring OAuth token.
    Uses GOOGLE_SERVICE_ACCOUNT_B64 — same credential as the W0 poller.
    Does not expire, so no more invalid_grant outages."""
    import base64, json
    from google.oauth2.service_account import Credentials as SACredentials
    sa_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
    if not sa_b64:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_B64 not set")
    padded = sa_b64 + "=" * (-len(sa_b64) % 4)
    sa_dict = json.loads(base64.b64decode(padded).decode())
    return SACredentials.from_service_account_info(sa_dict, scopes=SCOPES)

# ─────────────────────────────────────────────
# Phone Formatting
# ─────────────────────────────────────────────

def format_phone(raw: str) -> str:
    """Convert UK phone to international format: 07956... → 447956..."""
    digits = re.sub(r'\D', '', str(raw))
    if digits.startswith('07') and len(digits) == 11:
        return '44' + digits[1:]
    if digits.startswith('447') and len(digits) == 12:
        return digits
    if digits.startswith('44') and len(digits) >= 11:
        return digits
    # 10-digit number starting with 7 — missing leading 0
    if digits.startswith('7') and len(digits) == 10:
        return '44' + digits
    return digits

# ─────────────────────────────────────────────
# Google Sheets Helpers
# ─────────────────────────────────────────────

def ensure_tracking_sheet(service):
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    tabs = [s['properties']['title'] for s in meta.get('sheets', [])]
    headers = [["TL-REF", "Phone", "Campaign", "First Name", "Lead Date",
                "Current Step", "Last Sent", "Status", "Replied At"]]
    for tab in [TRACKING_SHEET, FLT_TRACKING_SHEET]:
        if tab not in tabs:
            log.info(f"Creating '{tab}' tab...")
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": tab}}}]}
            ).execute()
            service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{tab}'!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": headers}
            ).execute()
            log.info(f"'{tab}' created.")


def get_all_leads(service) -> list:
    leads = []
    for sheet_tab in [SHEET_NAME, "FLT"]:
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sheet_tab}'!A:F"
            ).execute()
        except Exception as e:
            log.warning(f"Could not read tab '{sheet_tab}': {e}")
            global _read_failed; _read_failed = True
            continue
        rows = result.get('values', [])
        if len(rows) < 2:
            continue
        for i, row in enumerate(rows[1:], start=2):
            if len(row) < 4:
                continue
            leads.append({
                "row":        i,
                "sheet_tab":  sheet_tab,
                "date":       row[0] if len(row) > 0 else "",
                "tl_ref":     row[1] if len(row) > 1 else "",
                "first_name": row[2] if len(row) > 2 else "",
                "phone":      row[3] if len(row) > 3 else "",
                "campaign":   row[4] if len(row) > 4 else "",
                "status":     row[5] if len(row) > 5 else "No contact",
            })
    return leads


def get_tracking_data(service) -> dict:
    tracking = {}
    for tab in [TRACKING_SHEET, FLT_TRACKING_SHEET]:
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{tab}'!A:I"
            ).execute()
            rows = result.get('values', [])
            if len(rows) < 2:
                continue
            for i, row in enumerate(rows[1:], start=2):
                if not row:
                    continue
                tl_ref = row[0].strip() if row else ""
                if not tl_ref:
                    continue
                tracking[tl_ref] = {
                    "row":          i,
                    "tracking_tab": tab,
                    "tl_ref":       tl_ref,
                    "phone":        row[1] if len(row) > 1 else "",
                    "campaign":     row[2] if len(row) > 2 else "",
                    "first_name":   row[3] if len(row) > 3 else "",
                    "lead_date":    row[4] if len(row) > 4 else "",
                    "current_step": int(row[5]) if len(row) > 5 and str(row[5]).isdigit() else 0,
                    "last_sent":    row[6] if len(row) > 6 else "",
                    "status":       row[7] if len(row) > 7 else "active",
                    "replied_at":   row[8] if len(row) > 8 else "",
                }
        except Exception as e:
            log.error(f"Failed to read tracking tab '{tab}': {e}")
            global _read_failed; _read_failed = True
    return tracking


def add_to_tracking(service, lead: dict):
    tab = get_tracking_tab(lead["tl_ref"])
    row = [lead["tl_ref"], lead["phone"], lead["campaign"], lead["first_name"],
           lead["date"], "0", "", "active", ""]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()
    log.info(f"Added {lead['tl_ref']} to {tab}.")


def update_tracking_row(service, row_num: int, step: int,
                        last_sent: str, status: str = "active",
                        replied_at: str = "", tracking_tab: str = ""):
    tab = tracking_tab if tracking_tab else TRACKING_SHEET
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!F{row_num}:I{row_num}",
        valueInputOption="RAW",
        body={"values": [[str(step), last_sent, status, replied_at]]}
    ).execute()


def update_sheet1_status(service, phone: str, status: str):
    leads = get_all_leads(service)
    formatted = format_phone(phone)
    for lead in leads:
        if format_phone(lead["phone"]) == formatted:
            tab = lead.get("sheet_tab", SHEET_NAME)
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{tab}'!F{lead['row']}",
                valueInputOption="RAW",
                body={"values": [[status]]}
            ).execute()
            log.info(f"{tab} row {lead['row']} status → '{status}'")
            return


def update_lead_status(service, lead: dict, status: str):
    tab = lead.get("sheet_tab", SHEET_NAME)
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!F{lead['row']}",
        valueInputOption="RAW",
        body={"values": [[status]]}
    ).execute()
    log.info(f"{tab} row {lead['row']} status → '{status}'")


def leads_for_phone(service, phone: str) -> list:
    formatted = format_phone(phone)
    return [
        lead for lead in get_all_leads(service)
        if format_phone(lead["phone"]) == formatted
    ]


def is_booking_pending(status: str) -> bool:
    return BOOKING_PENDING_STATUS in (status or "").lower()


def is_stopped_status(status: str) -> bool:
    status = (status or "").lower()
    return any(s in status for s in STOPPED_STATUSES)


def mark_replied_by_phone(service, phone: str):
    matching_leads = leads_for_phone(service, phone)
    if any(is_booking_pending(lead.get("status", "")) for lead in matching_leads):
        log.info(f"Phone {format_phone(phone)} is still booking pending — keeping 4h fallback active")
        return

    tracking = get_tracking_data(service)
    formatted = format_phone(phone)
    for tl_ref, data in tracking.items():
        if format_phone(data["phone"]) == formatted:
            if data["status"].lower() == "replied":
                return
            replied_at = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
            update_tracking_row(service, data["row"], data["current_step"],
                                data["last_sent"], "replied", replied_at,
                                tracking_tab=data.get("tracking_tab", TRACKING_SHEET))
            update_sheet1_status(service, phone, "Replied")
            log.info(f"Marked {tl_ref} as replied at {replied_at}")
            return
    log.warning(f"No lead found for phone {phone}")

# ─────────────────────────────────────────────
# WATI API
# ─────────────────────────────────────────────

def send_wati_template(phone: str, template_name: str, first_name: str) -> bool:
    formatted_phone = format_phone(phone)
    url = f"{WATI_API_URL}/api/v2/sendTemplateMessages"
    headers = {
        "Authorization": f"Bearer {WATI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "template_name": template_name,
        "broadcast_name": f"seq_{template_name}_{formatted_phone[-4:]}",
        "receivers": [
            {
                "whatsappNumber": formatted_phone,
                "customParams": [{"name": "first_name", "value": first_name}]
            }
        ]
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            log.info(f"✓ Sent {template_name} to {formatted_phone} ({first_name})")
            return True
        else:
            log.error(f"WATI {r.status_code} for {formatted_phone}: {r.text}")
            return False
    except Exception as e:
        log.error(f"WATI request failed for {formatted_phone}: {e}")
        return False

# ─────────────────────────────────────────────
# Sequence Logic
# ─────────────────────────────────────────────

def get_sequence(campaign: str) -> list:
    if "BST" in campaign.upper():
        return BST_SEQUENCE
    return UKDT_SEQUENCE


def is_allowed_campaign(campaign: str) -> bool:
    """Only send to UKDT CT, BST and UKDT O campaigns."""
    return campaign.strip().upper() in {c.upper() for c in ALLOWED_CAMPAIGNS}


def parse_lead_date(date_str: str):
    for fmt in (
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
        "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y",
    ):
        try:
            return datetime.datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def process_sequences(service):
    log.info("─── Processing sequences ───")
    global _read_failed
    _read_failed = False
    ping()
    now      = datetime.datetime.now()
    leads    = get_all_leads(service)
    tracking = get_tracking_data(service)
    _sync_today_from_tracking(tracking)
    log.info(f"{len(leads)} leads in sheet | {len(tracking)} in tracking")

    # Phone-level dedup: if a person is stopped under ANY reference (TL/FLT,
    # any tab), skip them everywhere. Prevents duplicate records from one
    # person being messaged after they've replied/passed on another record.
    def _norm_phone(ph):
        d = re.sub(r"\D", "", str(ph))
        return d[-10:] if len(d) >= 10 else d
    stopped_phones = {
        _norm_phone(t["phone"])
        for t in tracking.values()
        if is_stopped_status(t.get("status"))
        and len(_norm_phone(t["phone"])) == 10
    }
    stopped_phones.discard("")

    def _lead_priority(lead):
        tl_ref = lead["tl_ref"].strip()
        campaign = lead["campaign"].strip()
        status = lead["status"].lower().strip()
        lead_date = parse_lead_date(lead["date"])
        if (not lead["phone"] or not tl_ref or not lead_date or
                lead_date < CUTOFF_DATE or not is_allowed_campaign(campaign) or
                is_stopped_status(status) or _norm_phone(lead["phone"]) in stopped_phones):
            return (9, datetime.datetime.max, datetime.datetime.max, tl_ref)

        track = tracking.get(tl_ref)
        current_step = track["current_step"] if track else 0
        sequence = get_sequence(campaign)
        if current_step >= len(sequence):
            return (8, datetime.datetime.max, lead_date, tl_ref)

        delay_hours = sequence[current_step]["delay_hours"]
        if is_booking_pending(status) and current_step == 0:
            delay_hours = BOOKING_PENDING_DELAY_HOURS
        due_at = lead_date + datetime.timedelta(hours=delay_hours)

        if track is None:
            bucket = 0                  # enrol missing eligible leads first
        elif is_booking_pending(status) and current_step == 0:
            bucket = 1                  # W0W fallback leads get first W1 priority
        elif current_step == 0:
            bucket = 2                  # ordinary W1 before older later-step backlog
        else:
            bucket = 3
        return (bucket, due_at, lead_date, tl_ref)

    new_leads_added = 0
    messages_sent   = 0
    stopped_skips   = 0
    daily_cap_logged = False
    cycle_cap_logged = False

    for lead in sorted(leads, key=_lead_priority):
        tl_ref   = lead["tl_ref"].strip()
        campaign = lead["campaign"].strip()
        status   = lead["status"].lower().strip()
        booking_pending = is_booking_pending(status)

        if not lead["phone"] or not tl_ref:
            continue

        # Only process allowed campaigns
        if not is_allowed_campaign(campaign):
            continue

        # Skip if status indicates stop
        if is_stopped_status(status):
            continue

        # Skip if this PERSON (by phone) is stopped under any other reference
        if _norm_phone(lead["phone"]) in stopped_phones:
            stopped_skips += 1
            continue

        # Parse lead date
        lead_date = parse_lead_date(lead["date"])
        if not lead_date:
            continue

        # Skip leads added before cutoff date (the 148 stuck leads)
        if lead_date < CUTOFF_DATE:
            continue

        # Add to tracking if not already there
        if tl_ref not in tracking:
            add_to_tracking(service, lead)
            tracking = get_tracking_data(service)
            new_leads_added += 1

        track = tracking.get(tl_ref)
        if not track:
            continue

        # Skip if tracking says replied/completed/otherwise stopped
        if is_stopped_status(track["status"]):
            continue

        sequence     = get_sequence(campaign)
        current_step = track["current_step"]

        if current_step >= len(sequence):
            if track["status"] != "completed":
                update_tracking_row(service, track["row"], current_step,
                                    track["last_sent"], "completed",
                                    tracking_tab=track.get("tracking_tab", TRACKING_SHEET))
            continue

        next_msg = sequence[current_step]
        delay_hours = next_msg["delay_hours"]
        if booking_pending and current_step == 0:
            delay_hours = BOOKING_PENDING_DELAY_HOURS
        due_at = lead_date + datetime.timedelta(hours=delay_hours)

        # Spacing gate: don't fire step N until the real gap since the PREVIOUS
        # step's actual send has elapsed. Stops backlog leads bursting through
        # multiple steps in minutes. Normally-paced leads are unaffected.
        prev_delay = sequence[current_step - 1]["delay_hours"] if current_step > 0 else 0
        required_gap_h = next_msg["delay_hours"] - prev_delay
        last_sent_str = track.get("last_sent", "")
        if current_step > 0 and last_sent_str:
            try:
                last_dt = datetime.datetime.strptime(last_sent_str, "%d/%m/%Y %H:%M")
                if (now - last_dt).total_seconds() / 3600 < required_gap_h:
                    continue  # not enough real time since last send — wait
            except ValueError:
                pass  # unparseable date — don't block

        if now >= due_at:
            # All steps (incl W1) respect business hours — no out-of-hours sends.
            # New leads still get instant W0 from the poller (separate service).
            if not is_within_sending_window():
                log.debug(f"{tl_ref}: outside sending window, will send next window")
                continue
            if messages_sent >= MAX_SENDS_PER_CYCLE:
                if not cycle_cap_logged:
                    log.info(f"Cycle cap reached ({MAX_SENDS_PER_CYCLE}) - sends paused, enrolment continues")
                    cycle_cap_logged = True
                continue
            if _today_sent() >= MAX_SENDS_PER_DAY:
                if not daily_cap_logged:
                    log.info(f"Daily cap reached ({MAX_SENDS_PER_DAY}) - sends paused, enrolment continues")
                    daily_cap_logged = True
                continue

            if send_wati_template(lead["phone"], next_msg["template"], lead["first_name"]):
                new_step   = current_step + 1
                new_status = "completed" if new_step >= len(sequence) else "active"
                last_sent  = now.strftime("%d/%m/%Y %H:%M")
                update_tracking_row(service, track["row"], new_step, last_sent, new_status,
                                tracking_tab=track.get("tracking_tab", TRACKING_SHEET))
                if booking_pending and current_step == 0:
                    update_lead_status(service, lead, BOOKING_SEQUENCE_ACTIVE_STATUS)
                messages_sent += 1
                _bump_today()
                log.info(f"{tl_ref}: step {new_step}/{len(sequence)} — {next_msg['template']} (day total: {_today_sent()}/{MAX_SENDS_PER_DAY})")
        else:
            hrs = (due_at - now).total_seconds() / 3600
            log.debug(f"{tl_ref}: next msg in {hrs:.1f}h ({next_msg['template']})")

    log.info(f"─── Done — {new_leads_added} new leads added, {messages_sent} messages sent, {stopped_skips} phone-stopped skips ───")
    if _read_failed:
        log.warning("Sheet read issue this cycle; keeping Healthchecks as liveness to avoid false DOWN/UP alerts")
    ping()

# ─────────────────────────────────────────────
# Webhook Server
# ─────────────────────────────────────────────

sheets_service_global = None


class WatiWebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length  = int(self.headers.get('Content-Length', 0))
            body    = self.rfile.read(length)
            payload = json.loads(body)
            log.info(f"Webhook received: {json.dumps(payload)[:300]}")
            phone = (
                payload.get("waId") or
                payload.get("phone") or
                payload.get("from") or
                (payload.get("contact") or {}).get("phone", "") or
                (payload.get("contact") or {}).get("wa_id", "")
            )
            if phone and sheets_service_global:
                mark_replied_by_phone(sheets_service_global, str(phone))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        except Exception as e:
            log.error(f"Webhook error: {e}")
            self.send_response(500)
            self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"WATI Sequence Bot running.")

    def log_message(self, format, *args):
        pass


def start_webhook_server():
    server = HTTPServer(('0.0.0.0', WEBHOOK_PORT), WatiWebhookHandler)
    log.info(f"Webhook server on port {WEBHOOK_PORT}")
    threading.Thread(target=server.serve_forever, daemon=True).start()

# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def main():
    global sheets_service_global

    if not SPREADSHEET_ID:
        raise EnvironmentError("SPREADSHEET_ID not set")
    if not WATI_API_URL:
        raise EnvironmentError("WATI_API_URL not set")
    if not WATI_TOKEN:
        raise EnvironmentError("WATI_TOKEN not set")

    log.info("Starting WATI Sequence Automation...")
    log.info(f"Cutoff date: {CUTOFF_DATE.strftime('%d/%m/%Y')} (leads before this are skipped)")
    log.info(f"Allowed campaigns: {ALLOWED_CAMPAIGNS}")
    log.info(f"Sending windows: Mon-Thu 9am-6pm, Fri 9am-2pm, no Sat/Sun")
    log.info(f"Alert email: {ALERT_EMAIL}")

    consecutive_errors = 0

    while True:
        try:
            creds = get_google_credentials()
            sheets_service_global = build("sheets", "v4", credentials=creds, cache_discovery=False)
            ensure_tracking_sheet(sheets_service_global)
            start_webhook_server()
            start_healthcheck_heartbeat()

            log.info("Service started successfully.")
            consecutive_errors = 0

            while True:
                try:
                    process_sequences(sheets_service_global)
                    consecutive_errors = 0
                except Exception as e:
                    consecutive_errors += 1
                    log.exception(f"Error in sequence loop (#{consecutive_errors}): {e}")
                    if consecutive_errors >= 3:
                        send_alert_email(
                            "WATI sequence service is failing",
                            f"The WATI sequence automation has failed {consecutive_errors} times in a row.\n\nLast error: {e}\n\nPlease check Railway logs immediately."
                        )
                        consecutive_errors = 0

                log.info(f"Sleeping {POLL_INTERVAL}s...")
                time.sleep(POLL_INTERVAL)

        except Exception as e:
            log.exception(f"Fatal startup error: {e}")
            send_alert_email(
                "WATI service crashed on startup",
                f"The WATI automation crashed on startup.\n\nError: {e}\n\nPlease check Railway logs immediately."
            )
            log.info("Retrying in 60 seconds...")
            time.sleep(60)


if __name__ == "__main__":
    main()

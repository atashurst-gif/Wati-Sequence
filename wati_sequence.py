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
import datetime
import threading
from email.mime.text import MIMEText
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ─────────────────────────────────────────────
# Global 429 backoff — patches ALL Sheets API .execute() calls automatically
# ─────────────────────────────────────────────

def _install_sheets_backoff():
    """Monkey-patch googleapiclient HttpRequest.execute with exponential backoff."""
    import time as _time
    from googleapiclient import http as _ghttp
    from googleapiclient.errors import HttpError as _HttpError

    _orig = _ghttp.HttpRequest.execute

    def _execute_with_backoff(self, *args, **kwargs):
        delay = 2
        max_retries = 6
        for attempt in range(max_retries):
            try:
                return _orig(self, *args, **kwargs)
            except _HttpError as e:
                status = int(e.resp.status)
                if status in (429, 500, 502, 503) and attempt < max_retries - 1:
                    log.warning(f"Sheets API {status} — retrying in {delay}s (attempt {attempt+1}/{max_retries})")
                    _time.sleep(delay)
                    delay = min(delay * 2, 120)
                else:
                    log.error(f"Sheets API {status} failed after {attempt+1} attempts")
                    raise

    _ghttp.HttpRequest.execute = _execute_with_backoff
    log.info("✓ Sheets API backoff patch installed")



# ─────────────────────────────────────────────
# Startup — decode credentials from env
# ─────────────────────────────────────────────

load_dotenv()

_sa_b64 = os.getenv('GOOGLE_SERVICE_ACCOUNT_B64', '')
if _sa_b64:
    try:
        padded = _sa_b64 + '=' * (-len(_sa_b64) % 4)
        with open('service_account.json', 'wb') as f:
            f.write(base64.b64decode(padded))
    except Exception as e:
        print(f"Warning: Could not decode service account: {e}")

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("wati_sequence.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────

SPREADSHEET_ID    = os.getenv("SPREADSHEET_ID")
SHEET_NAME        = os.getenv("SHEET_NAME", "Sheet1")
TRACKING_SHEET    = "WATI Tracking"
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
ALLOWED_CAMPAIGNS = {"ukdt ct", "bst", "ukdt o"}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
HEALTHCHECK_URL = "https://hc-ping.com/a44b8422-f5b3-44d5-9b48-3e1d30acf187"

# Hours to wait after a client reply before resuming sequence
RESUME_AFTER_HOURS = 24

# Service account file path (used as fallback if env var not set)
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

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
    "callback set", "lead passed", "agreed", "verified",
    "moc set", "moc approved", "dnc", "dnq", "callback missed"
}

PAUSE_STATUSES = {"contacted"}
CONTACTED_RESUMED_STATUS = "contacted resumed"

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
    """Authenticate using Service Account — never expires, no browser needed."""
    from google.oauth2 import service_account
    sa_file = os.getenv('SERVICE_ACCOUNT_FILE', 'service_account.json')
    creds = service_account.Credentials.from_service_account_file(
        sa_file, scopes=SCOPES
    )
    log.info("Service account credentials loaded successfully.")
    return creds

# ─────────────────────────────────────────────
# Phone Formatting
# ─────────────────────────────────────────────

def format_phone(raw: str) -> str:
    """Convert UK phone to international format: 07956... → 447956..."""
    digits = re.sub(r'\D', '', str(raw))
    if digits.startswith('07') and len(digits) == 11:
        return '44' + digits[1:]
    if digits.startswith('447'):
        return digits
    if digits.startswith('44') and len(digits) >= 11:
        return digits
    return digits

# ─────────────────────────────────────────────
# Google Sheets Helpers
# ─────────────────────────────────────────────

def ensure_tracking_sheet(service):
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    tabs = [s['properties']['title'] for s in meta.get('sheets', [])]
    if TRACKING_SHEET not in tabs:
        log.info(f"Creating '{TRACKING_SHEET}' tab...")
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TRACKING_SHEET}}}]}
        ).execute()
        headers = [["TL-REF", "Phone", "Campaign", "First Name", "Lead Date",
                    "Current Step", "Last Sent", "Status", "Replied At"]]
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{TRACKING_SHEET}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": headers}
        ).execute()
        log.info("Tracking sheet created.")


def get_all_leads(service) -> list:
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A:F"
    ).execute()
    rows = result.get('values', [])
    if len(rows) < 2:
        return []
    leads = []
    for i, row in enumerate(rows[1:], start=2):
        if len(row) < 4:
            continue
        leads.append({
            "row":        i,
            "date":       row[0] if len(row) > 0 else "",
            "tl_ref":     row[1] if len(row) > 1 else "",
            "first_name": row[2] if len(row) > 2 else "",
            "phone":      row[3] if len(row) > 3 else "",
            "campaign":   row[4] if len(row) > 4 else "",
            "status":     row[5] if len(row) > 5 else "No contact",
        })
    return leads


def get_tracking_data(service) -> dict:
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{TRACKING_SHEET}'!A:I"
        ).execute()
        rows = result.get('values', [])
        if len(rows) < 2:
            return {}
        tracking = {}
        for i, row in enumerate(rows[1:], start=2):
            if not row:
                continue
            tl_ref = row[0].strip() if row else ""
            if not tl_ref:
                continue
            tracking[tl_ref] = {
                "row":          i,
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
        return tracking
    except Exception as e:
        log.error(f"Failed to read tracking: {e}")
        return {}


def add_to_tracking(service, lead: dict):
    row = [lead["tl_ref"], lead["phone"], lead["campaign"], lead["first_name"],
           lead["date"], "0", "", "active", ""]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{TRACKING_SHEET}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()
    log.info(f"Added {lead['tl_ref']} to tracking.")


def update_tracking_row(service, row_num: int, step: int,
                        last_sent: str, status: str = "active",
                        replied_at: str = ""):
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{TRACKING_SHEET}'!F{row_num}:I{row_num}",
        valueInputOption="RAW",
        body={"values": [[str(step), last_sent, status, replied_at]]}
    ).execute()


def mark_replied_by_phone(service, phone: str):
    tracking = get_tracking_data(service)
    formatted = format_phone(phone)
    for tl_ref, data in tracking.items():
        if format_phone(data["phone"]) == formatted:
            if data["status"].lower() == "replied":
                return
            replied_at = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
            update_tracking_row(service, data["row"], data["current_step"],
                                data["last_sent"], "contacted", replied_at)
            update_sheet1_status(service, phone, "Contacted")
            log.info(f"Marked {tl_ref} as Contacted at {replied_at}")
            return
    log.warning(f"No lead found for phone {phone}")


def update_sheet1_status(service, phone: str, status: str):
    leads = get_all_leads(service)
    formatted = format_phone(phone)
    for lead in leads:
        if format_phone(lead["phone"]) == formatted:
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!F{lead['row']}",
                valueInputOption="RAW",
                body={"values": [[status]]}
            ).execute()
            log.info(f"Sheet1 row {lead['row']} status → '{status}'")
            return

# ─────────────────────────────────────────────
# WATI API
# ─────────────────────────────────────────────

def update_wati_contact_stage(phone: str, stage: str) -> bool:
    """Update a contact's lead stage in WATI using correct attributes endpoint."""
    formatted_phone = format_phone(phone)
    # WATI updateContactAttributes requires number without leading + but with country code
    url = f"{WATI_API_URL}/api/v1/updateContactAttributes/{formatted_phone}"
    headers = {
        "Authorization": f"Bearer {WATI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "customParams": [
            {"name": "lead_stage", "value": stage}
        ]
    }
    log.info(f"WATI stage update URL: {url}")
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            log.info(f"WATI lead stage updated to '{stage}' for {formatted_phone}")
            return True
        else:
            log.warning(f"WATI stage update failed {r.status_code} for {formatted_phone}: {r.text[:500]}")
            return False
    except Exception as e:
        log.warning(f"WATI stage update error for {formatted_phone}: {e}")
        return False

def get_wati_lead_stage(phone: str) -> str:
    """Fetch the current Lead Stage for a contact from WATI. Returns lowercase string or empty string on failure."""
    formatted_phone = format_phone(phone)
    url = f"{WATI_API_URL}/api/v1/getContacts"
    headers = {"Authorization": f"Bearer {WATI_TOKEN}"}
    params = {"search": formatted_phone}
    log.info(f"WATI stage lookup: {formatted_phone}")
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        log.info(f"WATI getContacts response {r.status_code} for {formatted_phone}: {r.text[:300]}")
        if r.status_code == 200:
            data = r.json()
            # WATI returns contact_list at top level
            contacts = data.get("contact_list", [])
            log.info(f"WATI contacts found: {len(contacts)} for {formatted_phone}")
            for contact in contacts:
                wn = (contact.get("whatsappNumber") or contact.get("phone") or "").strip()
                log.info(f"WATI contact whatsappNumber: {wn}")
                if wn == formatted_phone:
                    for param in contact.get("customParams", []):
                        log.info(f"WATI param: {param}")
                        if param.get("name", "").lower() == "lead_stage":
                            stage = param.get("value", "").lower().strip()
                            log.info(f"WATI lead stage for {formatted_phone}: '{stage}'")
                            return stage
                    stage = contact.get("leadStage") or contact.get("lead_stage", "")
                    log.info(f"WATI direct stage for {formatted_phone}: '{stage}'")
                    return stage.lower().strip() if stage else ""
            log.info(f"WATI contact not found in results for {formatted_phone}")
            return ""
        else:
            log.warning(f"WATI getContacts {r.status_code} for {formatted_phone}")
            return ""
    except Exception as e:
        log.warning(f"WATI getContacts error for {formatted_phone}: {e}")
        return ""


def create_wati_contact(phone: str, first_name: str) -> bool:
    """Add contact to WATI before sending. Returns True if created or already exists."""
    formatted_phone = format_phone(phone)
    url = f"{WATI_API_URL}/api/v1/addContact/{formatted_phone}"
    headers = {
        "Authorization": f"Bearer {WATI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"name": first_name}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            log.info(f"✓ Contact created in WATI: {formatted_phone} ({first_name})")
            return True
        elif r.status_code == 400 and "already exists" in r.text.lower():
            log.info(f"Contact already exists in WATI: {formatted_phone}")
            return True
        else:
            log.error(f"WATI create contact {r.status_code} for {formatted_phone}: {r.text}")
            return False
    except Exception as e:
        log.error(f"WATI create contact failed for {formatted_phone}: {e}")
        return False


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
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def process_sequences(service):
    log.info("─── Processing sequences ───")
    now      = datetime.datetime.now()
    leads    = get_all_leads(service)
    tracking = get_tracking_data(service)
    log.info(f"{len(leads)} leads in sheet | {len(tracking)} in tracking")

    new_leads_added = 0
    messages_sent   = 0

    for lead in leads:
        tl_ref   = lead["tl_ref"].strip()
        campaign = lead["campaign"].strip()
        status   = lead["status"].lower().strip()

        if not lead["phone"] or not tl_ref:
            continue

        # Only process allowed campaigns
        if not is_allowed_campaign(campaign):
            continue

        # Skip if status indicates stop
        if any(s in status for s in STOPPED_STATUSES):
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

        # ── Get WATI Lead Stage as source of truth ──
        wati_stage = get_wati_lead_stage(lead["phone"])
        # Fall back to Sheet1 status if WATI API fails
        sheet1_status = lead["status"].lower().strip()
        effective_status = wati_stage if wati_stage else sheet1_status

        log.debug(f"{tl_ref}: WATI stage='{wati_stage}' sheet1='{sheet1_status}'")

        # Always keep Sheet1 in sync with WATI stage
        if wati_stage and wati_stage != sheet1_status:
            update_sheet1_status(service, lead["phone"], wati_stage.title())

        # Permanently stop if WATI stage indicates case is progressed
        if any(s in effective_status for s in STOPPED_STATUSES):
            if track["status"] != "stopped":
                update_tracking_row(service, track["row"], track["current_step"],
                                    track["last_sent"], "stopped")
            log.info(f"{tl_ref}: stopped — WATI stage is '{effective_status}'")
            continue

        # Handle paused (Contacted) status — resume after 24 hours
        if effective_status == CONTACTED_RESUMED_STATUS:
            pass  # treat as normal active lead
        elif any(s in effective_status for s in PAUSE_STATUSES):
            replied_at_str = track.get("replied_at", "")
            if replied_at_str:
                try:
                    replied_at = datetime.datetime.strptime(replied_at_str, "%d/%m/%Y %H:%M")
                    hours_since_reply = (now - replied_at).total_seconds() / 3600
                    if hours_since_reply < RESUME_AFTER_HOURS:
                        log.debug(f"{tl_ref}: paused — {RESUME_AFTER_HOURS - hours_since_reply:.1f}h until resume")
                        continue
                    else:
                        log.info(f"{tl_ref}: 24h passed since reply, resuming sequence")
                        update_sheet1_status(service, lead["phone"], "Contacted Resumed")
                        update_wati_contact_stage(lead["phone"], "Contacted Resumed")
                        update_tracking_row(service, track["row"], track["current_step"],
                                            track["last_sent"], "active")
                except ValueError:
                    pass
            else:
                continue  # No replied_at timestamp yet, stay paused

        # Skip if tracking says permanently stopped
        if track["status"].lower() in ("stopped", "completed"):
            continue

        sequence     = get_sequence(campaign)
        current_step = track["current_step"]

        if current_step >= len(sequence):
            if track["status"] != "completed":
                update_tracking_row(service, track["row"], current_step,
                                    track["last_sent"], "completed")
            continue

        next_msg = sequence[current_step]
        # W1 is due immediately from lead_date
        # W2+ are due delay_hours after the PREVIOUS message was sent
        if current_step == 0:
            due_at = lead_date + datetime.timedelta(hours=next_msg["delay_hours"])
        else:
            last_sent_str = track.get("last_sent", "")
            if last_sent_str:
                try:
                    last_sent_dt = datetime.datetime.strptime(last_sent_str, "%d/%m/%Y %H:%M")
                    due_at = last_sent_dt + datetime.timedelta(hours=next_msg["delay_hours"])
                except ValueError:
                    due_at = lead_date + datetime.timedelta(hours=next_msg["delay_hours"])
            else:
                due_at = lead_date + datetime.timedelta(hours=next_msg["delay_hours"])

        if now >= due_at:
            # W1 sends immediately always
            # W2+ only send within business hours
            if current_step > 0 and not is_within_sending_window():
                log.debug(f"{tl_ref}: outside sending window, will send next window")
                continue

            # Always create contact in WATI before sending W1
            if current_step == 0:
                create_wati_contact(lead["phone"], lead["first_name"])

            if send_wati_template(lead["phone"], next_msg["template"], lead["first_name"]):
                new_step   = current_step + 1
                new_status = "completed" if new_step >= len(sequence) else "active"
                last_sent  = now.strftime("%d/%m/%Y %H:%M")
                update_tracking_row(service, track["row"], new_step, last_sent, new_status)
                messages_sent += 1
                log.info(f"{tl_ref}: step {new_step}/{len(sequence)} — {next_msg['template']}")
        else:
            hrs = (due_at - now).total_seconds() / 3600
            log.debug(f"{tl_ref}: next msg in {hrs:.1f}h ({next_msg['template']})")

    log.info(f"─── Done — {new_leads_added} new leads added, {messages_sent} messages sent ───")
    try:
        requests.get(HEALTHCHECK_URL, timeout=5)
    except Exception:
        pass  # healthcheck ping failure should never crash the service

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

def validate_config():
    """Check all required constants and env vars exist at startup. Crashes with a clear message if anything is missing."""
    required_env = {
        "SPREADSHEET_ID": SPREADSHEET_ID,
        "WATI_API_URL": WATI_API_URL,
        "WATI_TOKEN": WATI_TOKEN,
    }
    required_constants = {
        "RESUME_AFTER_HOURS": RESUME_AFTER_HOURS,
        "PAUSE_STATUSES": PAUSE_STATUSES,
        "STOPPED_STATUSES": STOPPED_STATUSES,
        "CONTACTED_RESUMED_STATUS": CONTACTED_RESUMED_STATUS,
        "ALLOWED_CAMPAIGNS": ALLOWED_CAMPAIGNS,
        "CUTOFF_DATE": CUTOFF_DATE,
        "UKDT_SEQUENCE": UKDT_SEQUENCE,
        "BST_SEQUENCE": BST_SEQUENCE,
    }
    errors = []
    for name, val in required_env.items():
        if not val:
            errors.append(f"  ✗ env var {name} is not set")
    for name, val in required_constants.items():
        if val is None:
            errors.append(f"  ✗ constant {name} is not defined")
    if errors:
        msg = "STARTUP VALIDATION FAILED:\n" + "\n".join(errors)
        send_alert_email("WATI Bot — Startup Failed", msg)
        raise EnvironmentError(msg)
    log.info("✓ Startup validation passed — all constants and env vars present")
    _install_sheets_backoff()


def main():
    global sheets_service_global

    validate_config()

    log.info("Starting WATI Sequence Automation...")
    log.info(f"Cutoff date: {CUTOFF_DATE.strftime('%d/%m/%Y')} (leads before this are skipped)")
    log.info(f"Allowed campaigns: {ALLOWED_CAMPAIGNS}")
    log.info(f"Sending windows: Mon-Thu 9am-6pm, Fri 9am-2pm, no Sat/Sun")
    log.info(f"Alert email: {ALERT_EMAIL}")

    consecutive_errors = 0

    while True:
        try:
            creds = get_google_credentials()
            sheets_service_global = build("sheets", "v4", credentials=creds)
            ensure_tracking_sheet(sheets_service_global)
            start_webhook_server()

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

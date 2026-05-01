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
    "do not contact", "callback", "interested"
}

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
    """Authenticate using Service Account from env var — never expires."""
    import json
    import base64
    from google.oauth2 import service_account
    sa_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
    if sa_b64:
        padded = sa_b64 + "=" * (-len(sa_b64) % 4)
        info = json.loads(base64.b64decode(padded).decode("utf-8"))
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        log.info("Service account credentials loaded successfully.")
        return creds
    raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_B64 not set")


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
                                data["last_sent"], "replied", replied_at)
            update_sheet1_status(service, phone, "Replied")
            log.info(f"Marked {tl_ref} as replied at {replied_at}")
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

def send_wati_template(phone: str, template_name: str, first_name: str) -> bool:
    formatted_phone = format_phone(phone)
    url = f"{WATI_API_URL}/api/v1/sendTemplateMessage/{formatted_phone}"
    headers = {
        "Authorization": f"Bearer {WATI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "template_name": template_name,
        "broadcast_name": f"seq_{template_name}_{formatted_phone[-4:]}",
        "parameters": [{"name": "first_name", "value": first_name}]
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

        # Skip if tracking says replied/completed/stopped
        if track["status"].lower() in ("replied", "completed", "opted out", "stopped"):
            continue

        # Pause if Contacted — auto-resume after 24hrs
        if status == "contacted":
            replied_at_str = track.get("replied_at", "")
            if replied_at_str:
                try:
                    import datetime as dt2
                    replied_at = dt2.datetime.strptime(replied_at_str, "%d/%m/%Y %H:%M")
                    if (now - replied_at).total_seconds() / 3600 < 24:
                        continue
                except ValueError:
                    pass
            else:
                continue

        sequence     = get_sequence(campaign)
        current_step = track["current_step"]

        if current_step >= len(sequence):
            if track["status"] != "completed":
                update_tracking_row(service, track["row"], current_step,
                                    track["last_sent"], "completed")
            continue

        next_msg = sequence[current_step]
        due_at   = lead_date + datetime.timedelta(hours=next_msg["delay_hours"])

        if now >= due_at:
            # W1 sends immediately always
            # W2+ only send within business hours
            if current_step > 0 and not is_within_sending_window():
                log.debug(f"{tl_ref}: outside sending window, will send next window")
                continue

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

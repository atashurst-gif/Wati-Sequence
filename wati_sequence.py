"""
WATI WhatsApp Sequence Automation
Reads leads from Google Sheet (Sheet1), sends templated WhatsApp messages
via WATI API on a timed schedule, tracks progress in a 'WATI Tracking' tab,
and receives webhooks when clients reply.
"""

import os
import re
import json
import time
import base64
import logging
import datetime
import threading
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ─────────────────────────────────────────────
# Config & Logging
# ─────────────────────────────────────────────

load_dotenv()

# Decode Google credentials from base64 env vars (Railway deployment)
import base64 as _b64

def _safe_b64(s):
    """Fix missing padding before decoding."""
    s = s.strip()
    s += "=" * (-len(s) % 4)
    return _b64.b64decode(s)
_creds_b64 = os.getenv('GOOGLE_CREDENTIALS_B64')
_token_b64  = os.getenv('GOOGLE_TOKEN_B64')
if _creds_b64:
    with open('credentials.json', 'wb') as _f:
        _f.write(_safe_b64(_creds_b64))
if _token_b64:
    with open('token.json', 'wb') as _f:
        _f.write(_safe_b64(_token_b64))

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

SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID")
SHEET_NAME       = os.getenv("SHEET_NAME", "Sheet1")
TRACKING_SHEET   = "WATI Tracking"
WATI_API_URL     = os.getenv("WATI_API_URL")       # e.g. https://eu-api.wati.io/602557
WATI_TOKEN       = os.getenv("WATI_TOKEN")         # Bearer token (without "Bearer " prefix)
POLL_INTERVAL    = int(os.getenv("WATI_POLL_INTERVAL", "300"))  # 5 minutes
WEBHOOK_PORT     = int(os.getenv("WEBHOOK_PORT", "8080"))

CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE       = os.getenv("TOKEN_FILE", "token.json")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ─────────────────────────────────────────────
# Sequence Definitions
# ─────────────────────────────────────────────

# Hours after lead creation to send each message
# Message 1 is sent immediately (0 hours)
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

# Statuses that mean the sequence should NOT run
STOPPED_STATUSES = {"replied", "completed", "opted out", "converted", "do not contact", "pending"}

# ─────────────────────────────────────────────
# Google Auth
def get_google_credentials() -> Credentials:
    token_b64 = os.getenv("GOOGLE_TOKEN_B64", "")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_B64", "")
    if token_b64:
        token_json = json.loads(_safe_b64(token_b64))
        creds_json = json.loads(_safe_b64(creds_b64)) if creds_b64 else {}
        installed = creds_json.get("installed", {})
        creds = Credentials(
            token=token_json.get("token"),
            refresh_token=token_json.get("refresh_token"),
            token_uri=token_json.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=token_json.get("client_id") or installed.get("client_id"),
            client_secret=token_json.get("client_secret") or installed.get("client_secret"),
            scopes=token_json.get("scopes", SCOPES),
        )
        if creds.expired and creds.refresh_token:
            log.info("Refreshing expired Google token...")
            creds.refresh(Request())
        return creds
    creds = None
    if Path(TOKEN_FILE).exists():
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds
# ─────────────────────────────────────────────
# Phone Number Formatting
# ─────────────────────────────────────────────

def format_phone(raw: str) -> str:
    """
    Convert UK phone numbers to international format for WATI.
    07956766809 → 447956766809
    """
    digits = re.sub(r'\D', '', str(raw))
    if digits.startswith('07') and len(digits) == 11:
        return '44' + digits[1:]
    if digits.startswith('447') and len(digits) == 12:
        return digits
    if digits.startswith('7') and len(digits) == 10:
        return '44' + digits
    return digits

# ─────────────────────────────────────────────
# Google Sheets Helpers
# ─────────────────────────────────────────────

def ensure_tracking_sheet(service):
    """Create the WATI Tracking tab if it doesn't exist."""
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    tabs = [s['properties']['title'] for s in meta.get('sheets', [])]

    if TRACKING_SHEET not in tabs:
        log.info(f"Creating '{TRACKING_SHEET}' tab...")
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TRACKING_SHEET}}}]}
        ).execute()
        # Write header
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


def get_all_leads(service) -> list[dict]:
    """Read all rows from Sheet1."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A:F"
    ).execute()
    rows = result.get('values', [])
    if len(rows) < 2:
        return []

    leads = []
    for i, row in enumerate(rows[1:], start=2):  # skip header
        if len(row) < 4:
            continue
        leads.append({
            "row": i,
            "date":        row[0] if len(row) > 0 else "",
            "tl_ref":      row[1] if len(row) > 1 else "",
            "first_name":  row[2] if len(row) > 2 else "",
            "phone":       row[3] if len(row) > 3 else "",
            "campaign":    row[4] if len(row) > 4 else "",
            "status":      row[5] if len(row) > 5 else "No contact",
        })
    return leads


def get_tracking_data(service) -> dict:
    """
    Read WATI Tracking sheet and return a dict keyed by TL-REF.
    """
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
            tl_ref = row[0] if len(row) > 0 else ""
            if not tl_ref:
                continue
            tracking[tl_ref] = {
                "row":          i,
                "tl_ref":       tl_ref,
                "phone":        row[1] if len(row) > 1 else "",
                "campaign":     row[2] if len(row) > 2 else "",
                "first_name":   row[3] if len(row) > 3 else "",
                "lead_date":    row[4] if len(row) > 4 else "",
                "current_step": int(row[5]) if len(row) > 5 and row[5].isdigit() else 0,
                "last_sent":    row[6] if len(row) > 6 else "",
                "status":       row[7] if len(row) > 7 else "active",
                "replied_at":   row[8] if len(row) > 8 else "",
            }
        return tracking
    except Exception as e:
        log.error(f"Failed to read tracking sheet: {e}")
        return {}


def add_to_tracking(service, lead: dict):
    """Add a new lead to the tracking sheet."""
    row = [
        lead["tl_ref"],
        lead["phone"],
        lead["campaign"],
        lead["first_name"],
        lead["date"],
        "0",          # current_step
        "",           # last_sent
        "active",     # status
        "",           # replied_at
    ]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{TRACKING_SHEET}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()
    log.info(f"Added {lead['tl_ref']} to tracking sheet.")


def update_tracking_row(service, row_num: int, step: int, last_sent: str, status: str = "active", replied_at: str = ""):
    """Update a lead's tracking row after sending a message."""
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{TRACKING_SHEET}'!F{row_num}:I{row_num}",
        valueInputOption="RAW",
        body={"values": [[str(step), last_sent, status, replied_at]]}
    ).execute()


def mark_replied_by_phone(service, phone: str):
    """
    Find a lead by phone number in tracking sheet and mark as replied.
    Called when WATI webhook fires.
    """
    tracking = get_tracking_data(service)
    formatted = format_phone(phone)

    for tl_ref, data in tracking.items():
        if format_phone(data["phone"]) == formatted:
            replied_at = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
            update_tracking_row(
                service, data["row"],
                data["current_step"],
                data["last_sent"],
                status="replied",
                replied_at=replied_at
            )
            # Also update Status in Sheet1
            update_sheet1_status(service, phone, "Replied")
            log.info(f"Marked {tl_ref} as replied at {replied_at}")
            return True

    log.warning(f"Could not find lead with phone {phone} in tracking sheet.")
    return False


def update_sheet1_status(service, phone: str, status: str):
    """Update the Status column in Sheet1 for a given phone number."""
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
            log.info(f"Updated Sheet1 status for row {lead['row']} to '{status}'")
            return


# ─────────────────────────────────────────────
# WATI API
# ─────────────────────────────────────────────

def add_wati_contact(phone: str, first_name: str) -> bool:
    """Add a contact to WATI before sending a template."""
    formatted_phone = format_phone(phone)
    url = f"{WATI_API_URL}/api/v1/addContact/{formatted_phone}"
    headers = {
        "Authorization": f"Bearer {WATI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": first_name,
        "customParams": [{"name": "first_name", "value": first_name}]
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code in (200, 201):
            log.info(f"Contact added: {formatted_phone} ({first_name})")
            return True
        else:
            log.warning(f"Add contact {resp.status_code} for {formatted_phone}: {resp.text}")
            return False
    except Exception as e:
        log.error(f"Add contact failed for {formatted_phone}: {e}")
        return False


def add_wati_contact(phone, first_name):
    """Add a contact to WATI before sending a template."""
    formatted_phone = format_phone(phone)
    url = f"{WATI_API_URL}/api/v1/addContact/{formatted_phone}"
    headers = {
        "Authorization": f"Bearer {WATI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "name": first_name,
        "customParams": [{"name": "first_name", "value": first_name}]
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code in (200, 201):
            log.info(f"Contact added: {formatted_phone} ({first_name})")
            return True
        else:
            log.warning(f"Add contact {resp.status_code} for {formatted_phone}: {resp.text}")
            return False
    except Exception as e:
        log.error(f"Add contact failed for {formatted_phone}: {e}")
        return False

def send_wati_template(phone: str, template_name: str, first_name: str) -> bool:
    """
    Send a WhatsApp template message via WATI API.
    Returns True on success.
    """
    formatted_phone = format_phone(phone)
    add_wati_contact(phone, first_name)
    url = f"{WATI_API_URL}/api/v1/sendTemplateMessage/{formatted_phone}"
    headers = {
        "Authorization": f"Bearer {WATI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "template_name": template_name,
        "broadcast_name": f"sequence_{template_name}",
        "parameters": [
            {"name": "first_name", "value": first_name}
        ]
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        if response.status_code in (200, 201):
            log.info(f"✓ Sent {template_name} to {formatted_phone} ({first_name})")
            return True
        else:
            log.error(f"WATI API error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        log.error(f"WATI request failed for {formatted_phone}: {e}")
        return False


# ─────────────────────────────────────────────
# Sequence Logic
# ─────────────────────────────────────────────

def get_sequence_for_campaign(campaign: str) -> list:
    """Return the correct template sequence based on campaign."""
    c = campaign.upper()
    if "BST" in c:
        return BST_SEQUENCE
    return UKDT_SEQUENCE


def parse_lead_date(date_str: str) -> datetime.datetime | None:
    """Parse DD/MM/YYYY date string."""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def process_sequences(service):
    """
    Main sequence processing loop:
    1. Read all leads from Sheet1
    2. Add new leads to tracking
    3. For each active lead, check if next message is due
    4. Send if due, update tracking
    """
    log.info("─── Processing sequences ───")
    now = datetime.datetime.now()

    leads    = get_all_leads(service)
    tracking = get_tracking_data(service)

    log.info(f"Found {len(leads)} leads in sheet, {len(tracking)} in tracking.")

    for lead in leads:
        tl_ref   = lead["tl_ref"]
        campaign = lead["campaign"]
        status   = lead["status"].lower().strip()

        # Skip if lead status indicates they've responded
        if any(s in status for s in STOPPED_STATUSES):
            continue

        # Skip leads with no phone or ref
        if not lead["phone"] or not tl_ref:
            continue

        # Add to tracking if not already there
        if tl_ref not in tracking:
            add_to_tracking(service, lead)
            tracking = get_tracking_data(service)  # refresh

        track = tracking.get(tl_ref)
        if not track:
            continue

        # If replied, check if 24h has passed with no further reply - auto-resume
        if track["status"].lower() == "replied":
            replied_at_str = track.get("replied_at", "")
            if replied_at_str:
                try:
                    replied_at = datetime.datetime.strptime(replied_at_str, "%d/%m/%Y %H:%M")
                    hours_since = (datetime.datetime.now() - replied_at).total_seconds() / 3600
                    if hours_since < 24:
                        continue  # Still within 24h window, skip
                    # 24h passed with no new reply - resume sequence
                    log.info(f"{tl_ref}: 24h since reply, resuming sequence")
                    update_tracking_row(service, track["row"], track["current_step"], track["last_sent"], "active")
                    update_sheet1_status(service, track["phone"], "No contact")
                except ValueError:
                    continue
            else:
                continue



        # Get sequence for this campaign
        sequence = get_sequence_for_campaign(campaign)
        current_step = track["current_step"]

        # All messages sent
        if current_step >= len(sequence):
            continue

        # Parse lead date
        lead_date = parse_lead_date(lead["date"])
        if not lead_date:
            log.warning(f"Could not parse date for {tl_ref}: '{lead['date']}'")
            continue

        # Check next message due
        next_msg = sequence[current_step]
        due_at = lead_date + datetime.timedelta(hours=next_msg["delay_hours"])

        if now >= due_at:
            # Send the message
            success = send_wati_template(
                lead["phone"],
                next_msg["template"],
                lead["first_name"]
            )
            if success:
                new_step  = current_step + 1
                last_sent = now.strftime("%d/%m/%Y %H:%M")
                status_val = "completed" if new_step >= len(sequence) else "active"
                update_tracking_row(service, track["row"], new_step, last_sent, status_val)
                log.info(f"{tl_ref}: step {new_step}/{len(sequence)} sent ({next_msg['template']})")
        else:
            hours_remaining = (due_at - now).total_seconds() / 3600
            log.debug(f"{tl_ref}: next message in {hours_remaining:.1f}h ({next_msg['template']})")

    log.info("─── Sequence processing complete ───")


# ─────────────────────────────────────────────
# Webhook Server (receives WATI reply events)
# ─────────────────────────────────────────────

sheets_service_global = None  # Set in main()


class WatiWebhookHandler(BaseHTTPRequestHandler):
    """
    Handles incoming POST requests from WATI when a client replies.
    WATI sends a JSON payload with the contact's phone number.
    """

    def do_POST(self):
        try:
            length  = int(self.headers.get('Content-Length', 0))
            body    = self.rfile.read(length)
            payload = json.loads(body)

            log.info(f"Webhook received: {json.dumps(payload)[:200]}")

            # WATI sends waId (phone number) in the payload
            phone = (
                payload.get("waId") or
                payload.get("phone") or
                payload.get("from") or
                payload.get("contact", {}).get("phone", "")
            )

            if phone and sheets_service_global:
                mark_replied_by_phone(sheets_service_global, phone)

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        except Exception as e:
            log.error(f"Webhook error: {e}")
            self.send_response(500)
            self.end_headers()

    def do_GET(self):
        # Health check endpoint
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"WATI Sequence Bot is running.")

    def log_message(self, format, *args):
        pass  # Suppress default HTTP server logs


def start_webhook_server():
    """Start the webhook HTTP server in a background thread."""
    server = HTTPServer(('0.0.0.0', WEBHOOK_PORT), WatiWebhookHandler)
    log.info(f"Webhook server listening on port {WEBHOOK_PORT}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def main():
    global sheets_service_global

    if not SPREADSHEET_ID:
        raise EnvironmentError("SPREADSHEET_ID is not set")
    if not WATI_API_URL:
        raise EnvironmentError("WATI_API_URL is not set")
    if not WATI_TOKEN:
        raise EnvironmentError("WATI_TOKEN is not set")

    log.info("Starting WATI Sequence Automation...")
    log.info(f"WATI endpoint: {WATI_API_URL}")
    log.info(f"Poll interval: {POLL_INTERVAL}s")

    creds = get_google_credentials()
    sheets_service_global = build("sheets", "v4", credentials=creds)

    ensure_tracking_sheet(sheets_service_global)
    start_webhook_server()

    while True:
        try:
            process_sequences(sheets_service_global)
        except Exception as e:
            log.exception(f"Error in sequence processing: {e}")

        log.info(f"Sleeping {POLL_INTERVAL}s before next check...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()

"""
WATI WhatsApp Sequence Automation — Separate Service
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

load_dotenv()

_creds_b64 = os.getenv('GOOGLE_CREDENTIALS_B64')
_token_b64  = os.getenv('GOOGLE_TOKEN_B64')
if _creds_b64:
    with open('credentials.json', 'wb') as _f:
        _f.write(base64.b64decode(_creds_b64 + "=="))
if _token_b64:
    with open('token.json', 'wb') as _f:
        _f.write(base64.b64decode(_token_b64 + "=="))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("wati_sequence.log", encoding="utf-8")],
)
log = logging.getLogger(__name__)

SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID")
SHEET_NAME       = os.getenv("SHEET_NAME", "Sheet1")
TRACKING_SHEET   = "WATI Tracking"
WATI_API_URL     = os.getenv("WATI_API_URL")
WATI_TOKEN       = os.getenv("WATI_TOKEN")
POLL_INTERVAL    = int(os.getenv("WATI_POLL_INTERVAL", "300"))
WEBHOOK_PORT     = int(os.getenv("WEBHOOK_PORT", "8080"))
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE       = os.getenv("TOKEN_FILE", "token.json")
SCOPES           = ["https://www.googleapis.com/auth/spreadsheets"]

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

STOPPED_STATUSES = {"replied", "completed", "opted out", "converted", "do not contact", "callback", "interested"}

def get_google_credentials():
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

def format_phone(raw):
    digits = re.sub(r'\D', '', str(raw))
    if digits.startswith('07') and len(digits) == 11:
        return '44' + digits[1:]
    if digits.startswith('447'):
        return digits
    return digits

def ensure_tracking_sheet(service):
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    tabs = [s['properties']['title'] for s in meta.get('sheets', [])]
    if TRACKING_SHEET not in tabs:
        log.info(f"Creating '{TRACKING_SHEET}' tab...")
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TRACKING_SHEET}}}]}
        ).execute()
        headers = [["TL-REF", "Phone", "Campaign", "First Name", "Lead Date", "Current Step", "Last Sent", "Status", "Replied At"]]
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{TRACKING_SHEET}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": headers}
        ).execute()
        log.info("Tracking sheet created.")

def get_all_leads(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A:F"
    ).execute()
    rows = result.get('values', [])
    if len(rows) < 2:
        return []
    leads = []
    for i, row in enumerate(rows[1:], start=2):
        if len(row) < 4:
            continue
        leads.append({
            "row": i,
            "date":       row[0] if len(row) > 0 else "",
            "tl_ref":     row[1] if len(row) > 1 else "",
            "first_name": row[2] if len(row) > 2 else "",
            "phone":      row[3] if len(row) > 3 else "",
            "campaign":   row[4] if len(row) > 4 else "",
            "status":     row[5] if len(row) > 5 else "No contact",
        })
    return leads

def get_tracking_data(service):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"'{TRACKING_SHEET}'!A:I"
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

def add_to_tracking(service, lead):
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

def update_tracking_row(service, row_num, step, last_sent, status="active", replied_at=""):
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{TRACKING_SHEET}'!F{row_num}:I{row_num}",
        valueInputOption="RAW",
        body={"values": [[str(step), last_sent, status, replied_at]]}
    ).execute()

def mark_replied_by_phone(service, phone):
    tracking = get_tracking_data(service)
    formatted = format_phone(phone)
    for tl_ref, data in tracking.items():
        if format_phone(data["phone"]) == formatted:
            if data["status"].lower() == "replied":
                return
            replied_at = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
            update_tracking_row(service, data["row"], data["current_step"], data["last_sent"], "replied", replied_at)
            update_sheet1_status(service, phone, "Replied")
            log.info(f"Marked {tl_ref} as replied at {replied_at}")
            return
    log.warning(f"No lead found for phone {phone}")

def update_sheet1_status(service, phone, status):
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

def send_wati_template(phone, template_name, first_name):
    formatted_phone = format_phone(phone)
    url = f"{WATI_API_URL}/api/v1/sendTemplateMessage/{formatted_phone}"
    headers = {"Authorization": f"Bearer {WATI_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "template_name": template_name,
        "broadcast_name": f"seq_{template_name}_{formatted_phone[-4:]}",
        "parameters": [{"name": "first_name", "value": first_name}]
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            log.info(f"Sent {template_name} to {formatted_phone} ({first_name})")
            return True
        else:
            log.error(f"WATI {r.status_code} for {formatted_phone}: {r.text}")
            return False
    except Exception as e:
        log.error(f"WATI request failed: {e}")
        return False

def get_sequence(campaign):
    if "BST" in campaign.upper():
        return BST_SEQUENCE
    return UKDT_SEQUENCE

def parse_lead_date(date_str):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def is_within_sending_window():
    import zoneinfo
    uk=datetime.datetime.now(zoneinfo.ZoneInfo("Europe/London"))
    wd=uk.weekday()
    t=uk.hour*60+uk.minute
    if wd==6: return False
    close={0:1110,1:1110,2:1110,3:1110,4:930,5:810}.get(wd,0)
    return 600<=t<=close

def process_sequences(service):
    log.info("─── Processing sequences ───")
    now      = datetime.datetime.now()
    leads    = get_all_leads(service)
    tracking = get_tracking_data(service)
    log.info(f"{len(leads)} leads | {len(tracking)} in tracking")

    for lead in leads:
        tl_ref   = lead["tl_ref"].strip()
        campaign = lead["campaign"].strip()
        status   = lead["status"].lower().strip()

        if not lead["phone"] or not tl_ref:
            continue
        if any(s in status for s in STOPPED_STATUSES):
            continue

        if tl_ref not in tracking:
            add_to_tracking(service, lead)
            tracking = get_tracking_data(service)

        track = tracking.get(tl_ref)
        if not track:
            continue
        if track["status"].lower() in ("replied", "completed", "opted out"):
            continue

        sequence     = get_sequence(campaign)
        current_step = track["current_step"]

        if current_step >= len(sequence):
            continue

        lead_date = parse_lead_date(lead["date"])
        if not lead_date:
            log.warning(f"Bad date for {tl_ref}: '{lead['date']}'")
            continue

        next_msg = sequence[current_step]
        due_at   = lead_date + datetime.timedelta(hours=next_msg["delay_hours"])

        if now >= due_at:
            if current_step > 0 and not is_within_sending_window():
                log.debug(f"{tl_ref}: outside sending window")
                continue
            if send_wati_template(lead["phone"], next_msg["template"], lead["first_name"]):
                new_step   = current_step + 1
                new_status = "completed" if new_step >= len(sequence) else "active"
                update_tracking_row(service, track["row"], new_step,
                                    now.strftime("%d/%m/%Y %H:%M"), new_status)
                log.info(f"{tl_ref}: step {new_step}/{len(sequence)} sent")
        else:
            hrs = (due_at - now).total_seconds() / 3600
            log.debug(f"{tl_ref}: next in {hrs:.1f}h ({next_msg['template']})")

    log.info("─── Done ───")

sheets_service_global = None

class WatiWebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length  = int(self.headers.get('Content-Length', 0))
            body    = self.rfile.read(length)
            payload = json.loads(body)
            log.info(f"Webhook: {json.dumps(payload)[:200]}")
            phone = (payload.get("waId") or payload.get("phone") or
                     payload.get("from") or
                     (payload.get("contact") or {}).get("phone", "") or
                     (payload.get("contact") or {}).get("wa_id", ""))
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

def main():
    global sheets_service_global
    if not SPREADSHEET_ID:
        raise EnvironmentError("SPREADSHEET_ID not set")
    if not WATI_API_URL:
        raise EnvironmentError("WATI_API_URL not set")
    if not WATI_TOKEN:
        raise EnvironmentError("WATI_TOKEN not set")

    log.info("Starting WATI Sequence Automation...")
    log.info(f"Sheet: {SHEET_NAME} | WATI: {WATI_API_URL} | Poll: {POLL_INTERVAL}s")

    creds = get_google_credentials()
    sheets_service_global = build("sheets", "v4", credentials=creds)
    ensure_tracking_sheet(sheets_service_global)
    start_webhook_server()

    while True:
        try:
            process_sequences(sheets_service_global)
        except Exception as e:
            log.exception(f"Sequence error: {e}")
        log.info(f"Sleeping {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()

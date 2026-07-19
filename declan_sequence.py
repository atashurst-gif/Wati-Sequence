#!/usr/bin/env python3
"""
Declan (MDH) sequences — STANDALONE, isolated from the Regen sequence.

Runs TWO campaigns off Declan's automation spreadsheet, each on its own tab:
  - BST  bailiff cadence  -> tab "BST AUTOMATION"
  - UKDT CT cadence        -> tab "UKDT AUTOMATION"

Both tabs share the engine layout:
  A DATE | B NAME | C NUMBER | D STATUS | E STEP | F LAST_SENT
  - Leads auto-fall in (A-D). STATUS=CONTACTED stops a lead.
  - E STEP and F LAST_SENT are written by THIS script only.

Cadence (enquiry day = day 1), identical timing for both campaigns:
  step 0  <w0>            immediate (sent by POLLER, sequence skips)
  step 1  <eod>          20:00 same day as enquiry
  step 2  <day_2_midday> 13:00 next day (day 2)
  step 3  <day_3>        13:00 day 3
  step 4  <day_5>        13:00 day 5
  step 5  <day_7>        20:00 day 7
  step 6  <day_11>       13:00 day 11
  step 7  <day_14>       13:00 day 14

Environment:
  GOOGLE_SERVICE_ACCOUNT_B64  - SA with EDITOR on the automation sheet
  WATI_API_URL_DECLAN         - https://live-mt-server.wati.io/10188789
  WATI_TOKEN_DECLAN           - Declan bearer token
  DECLAN_POLL_INTERVAL        - seconds between polls (default 300)
  DECLAN_DRY_RUN              - "1" = log what WOULD send, send nothing (default "1")
  DECLAN_DAILY_CAP            - max sends per day safety cap (default 200)
"""
import os, re, json, base64, time, datetime, logging
from zoneinfo import ZoneInfo
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("declan_seq")

UK_TZ = ZoneInfo("Europe/London")

SHEET_ID = "1FsEIcfd8eY3muNLbTd31qBEcT0irKYz5dNUAffaSoJA"

WATI_API_URL_DECLAN = os.getenv("WATI_API_URL_DECLAN", "https://live-mt-server.wati.io/10188789")
WATI_TOKEN_DECLAN   = os.getenv("WATI_TOKEN_DECLAN", "")

POLL_INTERVAL = int(os.getenv("DECLAN_POLL_INTERVAL", "300"))
DAILY_CAP     = int(os.getenv("DECLAN_DAILY_CAP", "200"))
DRY_RUN       = os.getenv("DECLAN_DRY_RUN", "1") == "1"   # SAFE DEFAULT: dry run ON

# ── Sequence definitions: (step, template, day_offset, hour) ──
# day_offset is days AFTER the enquiry day (enquiry day = offset 0).
# A step is due when now >= enquiry_date + day_offset days, at >= hour:00 local.
BST_SEQUENCE = [
    (0, "__SKIP__",        0,  0),   # W0 handled by poller
    (1, "bailiff_eod",     0,  20),
    (2, "day_2_midday",    1,  13),
    (3, "bailiff_day_3",   2,  13),
    (4, "bailiff_day_5",   4,  13),
    (5, "bailiff_day_7",   6,  20),
    (6, "bailiff_day_11",  10, 13),
    (7, "bailiff_day_14",  13, 13),
]

CT_SEQUENCE = [
    (0, "__SKIP__",          0,  0),   # ukdt_ct_w0 handled by poller
    (1, "ct_eod",            0,  20),
    (2, "ct_day_2_midday",   1,  13),
    (3, "ct_day3",           2,  13),
    (4, "ct_day5",           4,  13),
    (5, "ct_day7",           6,  20),
    (6, "ct_day11",          10, 13),
    (7, "ct_day14",          13, 13),
]

# Templates that take a {{name}} param. Any template NOT listed sends with no params.
# (Sending params to a no-param template is rejected by WATI, and vice versa.)
BST_NAME_PARAMS = {"bailiff_eod", "bailiff_day_3", "bailiff_day_7", "day_2_midday"}
CT_NAME_PARAMS  = {"ct_eod", "ct_day_2_midday", "ct_day3", "ct_day7", "ct_day14"}
# ct_day5 and ct_day11 take NO param.

CAMPAIGNS = [
    {"name": "BST",  "tab": "BST AUTOMATION",  "sequence": BST_SEQUENCE, "name_params": BST_NAME_PARAMS},
    {"name": "UKDT", "tab": "UKDT AUTOMATION", "sequence": CT_SEQUENCE,  "name_params": CT_NAME_PARAMS},
]

STOP_STATUS = "contacted"

# Sending window — only send between these hours.
SEND_START_HOUR = int(os.getenv("DECLAN_SEND_START", "9"))
SEND_END_HOUR   = int(os.getenv("DECLAN_SEND_END", "21"))

_daily_sends = {}


def format_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', str(raw))
    if digits.startswith('07') and len(digits) == 11:
        return '44' + digits[1:]
    if digits.startswith('447') and len(digits) == 12:
        return digits
    if digits.startswith('44') and len(digits) >= 11:
        return digits
    if digits.startswith('7') and len(digits) == 10:
        return '44' + digits
    return digits


def get_credentials():
    b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
    if not b64:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_B64 not set")
    padded = b64 + "=" * (-len(b64) % 4)
    sa = json.loads(base64.b64decode(padded).decode())
    return Credentials.from_service_account_info(
        sa, scopes=["https://www.googleapis.com/auth/spreadsheets"])


def parse_enquiry_date(s: str):
    s = str(s).strip()
    if not s:
        return None
    try:
        serial = float(s)
        if 20000 < serial < 80000:
            epoch = datetime.datetime(1899, 12, 30, tzinfo=UK_TZ)
            return epoch + datetime.timedelta(days=serial)
    except (ValueError, TypeError):
        pass
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S",
              "%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
              "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.datetime.strptime(s, f).replace(tzinfo=UK_TZ)
        except ValueError:
            continue
    return None


def send_declan_template(phone: str, template_name: str, first_name: str, name_params: set) -> bool:
    formatted = format_phone(phone)
    if DRY_RUN:
        log.info(f"[DRY RUN] would send {template_name} to {formatted} ({first_name})")
        return True
    url = f"{WATI_API_URL_DECLAN}/api/v2/sendTemplateMessages"
    headers = {"Authorization": f"Bearer {WATI_TOKEN_DECLAN}", "Content-Type": "application/json"}
    if template_name in name_params:
        receiver = {"whatsappNumber": formatted,
                    "customParams": [{"name": "name", "value": first_name}]}
    else:
        receiver = {"whatsappNumber": formatted}
    payload = {
        "template_name": template_name,
        "broadcast_name": f"decseq_{template_name}_{formatted[-4:]}",
        "receivers": [receiver],
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            try:
                body = r.json()
            except Exception:
                body = {}
            if body.get("result") is True:
                log.info(f"\u2713 Sent {template_name} to {formatted} ({first_name})")
                return True
            log.error(f"Declan WATI accepted-but-REJECTED {template_name} for {formatted}: {str(body)[:250]}")
            return False
        log.error(f"Declan WATI {r.status_code} for {formatted}: {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Declan WATI request failed for {formatted}: {e}")
        return False


def declan_effective_day(enquiry_dt):
    """Declan's day runs 08:00-20:00. An enquiry after 20:00 counts as the NEXT day."""
    if enquiry_dt.hour >= 20:
        nextday = enquiry_dt + datetime.timedelta(days=1)
        return nextday.replace(hour=8, minute=0, second=0, microsecond=0)
    return enquiry_dt


def due_step(enquiry_dt, current_step, now, sequence):
    """Return the next step (step_no, template) that is due, or None."""
    enquiry_dt = declan_effective_day(enquiry_dt)
    if current_step >= len(sequence):
        return None
    step_no, template, day_offset, hour = sequence[current_step]
    due_at = (enquiry_dt + datetime.timedelta(days=day_offset)).replace(
        hour=hour, minute=0, second=0, microsecond=0)
    if now >= due_at:
        return (step_no, template)
    return None


def within_window(now):
    return SEND_START_HOUR <= now.hour < SEND_END_HOUR


def _bump_daily():
    today = datetime.datetime.now(UK_TZ).date()
    _daily_sends[today] = _daily_sends.get(today, 0) + 1

def _daily_count():
    today = datetime.datetime.now(UK_TZ).date()
    return _daily_sends.get(today, 0)


def process_campaign(svc, campaign, now):
    tab         = campaign["tab"]
    sequence    = campaign["sequence"]
    name_params = campaign["name_params"]
    cname       = campaign["name"]

    try:
        rows = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"'{tab}'!A:F").execute().get("values", [])
    except Exception as e:
        log.error(f"[{cname}] could not read tab '{tab}': {e}")
        return 0

    if not rows:
        return 0
    sent = 0
    stopped = 0
    for i, r in enumerate(rows[1:], start=2):  # row 1 = header
        date_raw = r[0] if len(r) > 0 else ""
        name     = r[1] if len(r) > 1 else ""
        number   = r[2] if len(r) > 2 else ""
        status   = (r[3] if len(r) > 3 else "").strip().lower()
        step_raw = r[4] if len(r) > 4 else ""

        if not number or not str(number).strip():
            continue
        if STOP_STATUS in status:
            stopped += 1
            continue

        enquiry_dt = parse_enquiry_date(date_raw)
        if not enquiry_dt:
            log.warning(f"[{cname}] row {i}: unparseable date '{date_raw}', skipping")
            continue

        current_step = int(step_raw) if str(step_raw).strip().isdigit() else 0
        if current_step >= len(sequence):
            continue

        if not within_window(now):
            continue

        due = due_step(enquiry_dt, current_step, now, sequence)
        if not due:
            continue

        if _daily_count() >= DAILY_CAP:
            log.warning(f"[{cname}] daily cap {DAILY_CAP} reached — stopping this cycle")
            break

        step_no, template = due
        first_name = str(name).split()[0].title() if name and "@" not in str(name) else "there"

        # W0 sent by poller — advance past skip step without sending.
        if template == "__SKIP__":
            new_step = current_step + 1
            stamp = now.strftime("%d/%m/%Y %H:%M")
            svc.spreadsheets().values().update(
                spreadsheetId=SHEET_ID, range=f"'{tab}'!E{i}:F{i}",
                valueInputOption="RAW", body={"values": [[str(new_step), stamp]]}
            ).execute()
            continue

        if send_declan_template(number, template, first_name, name_params):
            sent += 1
            _bump_daily()
            new_step = current_step + 1
            stamp = now.strftime("%d/%m/%Y %H:%M")
            svc.spreadsheets().values().update(
                spreadsheetId=SHEET_ID, range=f"'{tab}'!E{i}:F{i}",
                valueInputOption="RAW", body={"values": [[str(new_step), stamp]]}
            ).execute()
            time.sleep(1)
    log.info(f"\u2500\u2500\u2500 [{cname}] Done \u2014 {sent} sent, {stopped} stopped(contacted) "
             f"{'[DRY RUN]' if DRY_RUN else ''} \u2500\u2500\u2500")
    return sent


def process(svc):
    now = datetime.datetime.now(UK_TZ)
    total = 0
    for campaign in CAMPAIGNS:
        total += process_campaign(svc, campaign, now)
    return total


def main():
    log.info(f"Declan sequence starting... DRY_RUN={DRY_RUN} poll={POLL_INTERVAL}s cap={DAILY_CAP} "
             f"campaigns={[c['name'] for c in CAMPAIGNS]}")
    if not WATI_TOKEN_DECLAN and not DRY_RUN:
        log.error("WATI_TOKEN_DECLAN not set and not dry run — refusing to start")
        return
    creds = get_credentials()
    svc = build("sheets", "v4", credentials=creds)
    while True:
        try:
            process(svc)
        except Exception as e:
            log.error(f"cycle error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()

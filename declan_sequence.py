#!/usr/bin/env python3
"""
Declan (MDH) BST bailiff sequence — STANDALONE, isolated from the Regen sequence.

Reads Declan's AUTOMATION sheet, sends the bailiff cadence via DECLAN's WATI tenant,
and stops any lead whose STATUS column reads CONTACTED.

Sheet: AUTOMATION tab of 1FsEIcfd8eY3muNLbTd31qBEcT0irKYz5dNUAffaSoJA
  A DATE | B NAME | C NUMBER | D STATUS | E STEP | F LAST_SENT
  - Dec's leads auto-fall in (A-D). The Zap/Dec sets STATUS=CONTACTED to stop.
  - E STEP and F LAST_SENT are written by THIS script only. Dec never touches them.

Cadence (enquiry day = day 1):
  step 0  bst_w0         immediate (on first sight)
  step 1  bailiff_eod    20:00 same day as enquiry
  step 2  day_2_midday   13:00 next day (day 2)
  step 3  bailiff_day_3  13:00 day 3
  step 4  bailiff_day_5  13:00 day 5
  step 5  bailiff_day_7  20:00 day 7
  step 6  bailiff_day_11 13:00 day 11
  step 7  bailiff_day_14 13:00 day 14

Environment (all already on the Regen sequence service; set the same here):
  GOOGLE_SERVICE_ACCOUNT_B64  - same SA, must have EDITOR on the AUTOMATION sheet
  WATI_API_URL_DECLAN         - https://live-mt-server.wati.io/10188789
  WATI_TOKEN_DECLAN           - Declan send-scoped bearer token
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

SHEET_ID   = "1FsEIcfd8eY3muNLbTd31qBEcT0irKYz5dNUAffaSoJA"
TAB        = "AUTOMATION"

WATI_API_URL_DECLAN = os.getenv("WATI_API_URL_DECLAN", "https://live-mt-server.wati.io/10188789")
WATI_TOKEN_DECLAN   = os.getenv("WATI_TOKEN_DECLAN", "")

POLL_INTERVAL = int(os.getenv("DECLAN_POLL_INTERVAL", "300"))
DAILY_CAP     = int(os.getenv("DECLAN_DAILY_CAP", "200"))
DRY_RUN       = os.getenv("DECLAN_DRY_RUN", "1") == "1"   # SAFE DEFAULT: dry run ON

# ── Sequence definition: (step, template, day_offset, hour) ──
# day_offset is days AFTER the enquiry day (enquiry day = offset 0).
# A step is due when now >= enquiry_date + day_offset days, at >= hour:00 local.
SEQUENCE = [
    (0, "__SKIP__",        0,  0),   # W0 handled by poller — sequence skips this step
    (1, "bailiff_eod",     0,  20),  # 8pm same day
    (2, "day_2_midday",    1,  13),  # 1pm next day
    (3, "bailiff_day_3",   2,  13),  # 1pm day 3
    (4, "bailiff_day_5",   4,  13),  # 1pm day 5
    (5, "bailiff_day_7",   6,  20),  # 8pm day 7
    (6, "bailiff_day_11",  10, 13),  # 1pm day 11
    (7, "bailiff_day_14",  13, 13),  # 1pm day 14
]
STOP_STATUS = "contacted"

# Sending window — only send between these hours (matches the cadence times 13:00/20:00).
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


def send_declan_template(phone: str, template_name: str, first_name: str) -> bool:
    formatted = format_phone(phone)
    if DRY_RUN:
        log.info(f"[DRY RUN] would send {template_name} to {formatted} ({first_name})")
        return True
    url = f"{WATI_API_URL_DECLAN}/api/v2/sendTemplateMessages"
    headers = {"Authorization": f"Bearer {WATI_TOKEN_DECLAN}", "Content-Type": "application/json"}
    payload = {
        "template_name": template_name,
        "broadcast_name": f"decseq_{template_name}_{formatted[-4:]}",
        "receivers": [{"whatsappNumber": formatted,
                       "customParams": [{"name": "first_name", "value": first_name}]}],
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            log.info(f"\u2713 Sent {template_name} to {formatted} ({first_name})")
            return True
        log.error(f"Declan WATI {r.status_code} for {formatted}: {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Declan WATI request failed for {formatted}: {e}")
        return False


def due_step(enquiry_dt, current_step, now):
    """Return the next step dict that is due, or None."""
    if current_step >= len(SEQUENCE):
        return None
    step_no, template, day_offset, hour = SEQUENCE[current_step]
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


def process(svc):
    now = datetime.datetime.now(UK_TZ)
    rows = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{TAB}'!A:F").execute().get("values", [])
    if not rows:
        return 0
    sent = 0
    stopped = 0
    for i, r in enumerate(rows[1:], start=2):  # row 1 = header
        # columns: A date, B name, C number, D status, E step, F last_sent
        date_raw = r[0] if len(r) > 0 else ""
        name     = r[1] if len(r) > 1 else ""
        number   = r[2] if len(r) > 2 else ""
        status   = (r[3] if len(r) > 3 else "").strip().lower()
        step_raw = r[4] if len(r) > 4 else ""

        if not number or not str(number).strip():
            continue
        # STOP: contacted -> never send again
        if STOP_STATUS in status:
            stopped += 1
            continue

        enquiry_dt = parse_enquiry_date(date_raw)
        if not enquiry_dt:
            log.warning(f"row {i}: unparseable date '{date_raw}', skipping")
            continue

        current_step = int(step_raw) if str(step_raw).strip().isdigit() else 0
        if current_step >= len(SEQUENCE):
            continue  # sequence finished

        if not within_window(now):
            continue

        due = due_step(enquiry_dt, current_step, now)
        if not due:
            continue

        if _daily_count() >= DAILY_CAP:
            log.warning(f"daily cap {DAILY_CAP} reached — stopping this cycle")
            break

        step_no, template = due
        first_name = str(name).split()[0].title() if name and "@" not in str(name) else "there"

        # W0 is sent by the poller, not the sequence. Advance past the skip step without sending.
        if template == "__SKIP__":
            new_step = current_step + 1
            stamp = now.strftime("%d/%m/%Y %H:%M")
            svc.spreadsheets().values().update(
                spreadsheetId=SHEET_ID, range=f"'{TAB}'!E{i}:F{i}",
                valueInputOption="RAW", body={"values": [[str(new_step), stamp]]}
            ).execute()
            continue

        if send_declan_template(number, template, first_name):
            sent += 1
            _bump_daily()
            new_step = current_step + 1
            stamp = now.strftime("%d/%m/%Y %H:%M")
            # write E (step) and F (last_sent) back
            svc.spreadsheets().values().update(
                spreadsheetId=SHEET_ID, range=f"'{TAB}'!E{i}:F{i}",
                valueInputOption="RAW", body={"values": [[str(new_step), stamp]]}
            ).execute()
            time.sleep(1)  # gentle pacing
    log.info(f"\u2500\u2500\u2500 Done \u2014 {sent} sent, {stopped} stopped(contacted) "
             f"{'[DRY RUN]' if DRY_RUN else ''} \u2500\u2500\u2500")
    return sent


def main():
    log.info(f"Declan sequence starting... DRY_RUN={DRY_RUN} poll={POLL_INTERVAL}s cap={DAILY_CAP}")
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

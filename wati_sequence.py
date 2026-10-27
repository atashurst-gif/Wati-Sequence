"""
WATI WhatsApp Sequence Automation
- Reads leads from Google Sheet (Sheet1)
- Sends templated WhatsApp messages via WATI API
- Tracks progress in 'WATI Tracking' tab
- Timetable from the enquiry: end-of-day 20:00 (day 0), fu1 19:00 (day 1), fu2 (day 3), fu4 (day 7)
- Daytime steps go out while someone is on shift in the CRM (fallback: Mon-Thu 9-6, Fri 9-2)
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
W0_TRACKING_SHEET  = os.getenv("W0_TRACKING_SHEET", "W0 Tracking")

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
ALLOWED_CAMPAIGNS = {"ukdt ct", "bst", "ukdt o", "flt", "ukdt ct2", "ukdt tpa", "bst tpa"}

# First-message (W0) leads the poller logs as plain "UKDT" join the sequence from this
# date. Earlier ones are left alone so months of old leads are not chased in one go.
W0_UKDT_START = datetime.datetime.strptime(os.getenv("W0_UKDT_START", "06/10/2026"), "%d/%m/%Y")
# Records for the same phone within this many days are one enquiry: one sequence.
SAME_PERSON_DAYS = int(os.getenv("SAME_PERSON_DAYS", "14"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
MAX_SENDS_PER_CYCLE = int(os.getenv("MAX_SENDS_PER_CYCLE", "12"))  # per-cycle drip feed, not a burst
MAX_SENDS_PER_HOUR = int(os.getenv("MAX_SENDS_PER_HOUR", "14"))  # measured total send pace
MAX_SENDS_PER_DAY = int(os.getenv("MAX_SENDS_PER_DAY", "145"))  # hard daily ceiling on actual sends
MAX_BACKLOG_SENDS_PER_DAY = int(os.getenv("MAX_BACKLOG_SENDS_PER_DAY", "130"))  # allows roughly 40 more backlog today
_daily_sends = {}  # {date: count} — persists across cycles, auto-resets each day
_hourly_sends = {}  # {(date, hour): count} — smooths backlog catch-up
_backlog_sends = {}  # {date: count} — stops backlog starving new enquiries

UK_TZ = ZoneInfo("Europe/London")

def _today_sent():
    """How many sends so far today (UK date). Auto-prunes old days."""
    today = datetime.datetime.now(UK_TZ).date()
    for d in list(_daily_sends):
        if d != today:
            del _daily_sends[d]
    return _daily_sends.get(today, 0)

def _hour_sent():
    """How many sends so far this UK clock-hour. Auto-prunes old hours."""
    now = datetime.datetime.now(UK_TZ)
    key = (now.date(), now.hour)
    for k in list(_hourly_sends):
        if k != key:
            del _hourly_sends[k]
    return _hourly_sends.get(key, 0)


def _backlog_sent():
    """How many backlog sends so far today (UK date)."""
    today = datetime.datetime.now(UK_TZ).date()
    for d in list(_backlog_sends):
        if d != today:
            del _backlog_sends[d]
    return _backlog_sends.get(today, 0)


def _is_backlog_lead_date(lead_date):
    """A backlog lead was added before today; today's leads get separate headroom."""
    if not lead_date:
        return False
    return lead_date.date() < datetime.datetime.now(UK_TZ).date()


def _bump_today(is_backlog=False):
    now = datetime.datetime.now(UK_TZ)
    today = now.date()
    _daily_sends[today] = _daily_sends.get(today, 0) + 1
    hour_key = (today, now.hour)
    _hourly_sends[hour_key] = _hourly_sends.get(hour_key, 0) + 1
    if is_backlog:
        _backlog_sends[today] = _backlog_sends.get(today, 0) + 1


def _sync_today_from_tracking(tracking: dict):
    """Seed the daily cap from persisted tracking so redeploys cannot reset it."""
    today = datetime.datetime.now(UK_TZ).date()
    now = datetime.datetime.now(UK_TZ)
    current_hour = now.hour
    sent_today = 0
    sent_this_hour = 0
    backlog_sent_today = 0
    lt_today = 0
    for item in tracking.values():
        last_sent = item.get("last_sent") or ""
        if not last_sent:
            continue
        try:
            sent_dt = datetime.datetime.strptime(last_sent, "%d/%m/%Y %H:%M").replace(tzinfo=UK_TZ)
        except ValueError:
            continue
        if sent_dt.date() == today:
            if sent_dt.hour == current_hour:
                sent_this_hour += 1
            if int(item.get("current_step") or 0) >= LONGTAIL_FIRST_STEP:
                lt_today += 1       # a long-tail send: its own allowance
                continue
            sent_today += 1
            lead_date = parse_lead_date(item.get("lead_date", ""))
            if _is_backlog_lead_date(lead_date):
                backlog_sent_today += 1
    _daily_sends[today] = max(_daily_sends.get(today, 0), sent_today)
    _hourly_sends[(today, current_hour)] = max(_hourly_sends.get((today, current_hour), 0), sent_this_hour)
    _backlog_sends[today] = max(_backlog_sends.get(today, 0), backlog_sent_today)
    _longtail_sends[today] = max(_longtail_sends.get(today, 0), lt_today)

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

# Rebuilt 25/10/2026. Every step is timed from the ENQUIRY, not from the previous send:
#   "day" = days after day 0, the evening the end-of-day message goes out (the enquiry day;
#           the next day for enquiries from 18:00; never a Saturday - see sequence_day_zero)
#   "at_hour" steps are evening messages that go out in that clock hour only;
#   the others go out from 09:00 on their day, the next time someone is on shift in the CRM
#   (is_within_sending_window), so they never land in the CRM's out-of-hours booking-link window.
# Step numbers are the message's identity in the tracker's Current Step column and are kept
# from the old six-step shape, so leads already part-way through carry on correctly:
#   fu3 (old step 4, day 5) and fu5 (old step 6, day 8) are retired - 4.5% and 1.8% replies;
#   a lead showing 4 (fu3 sent) gets fu4 next, 5 or 6 (fu4/fu5 sent) is finished.
UKDT_SEQUENCE = [
    {"step": 1, "template": "eod_all",  "day": 0, "at_hour": 20},
    {"step": 2, "template": "ukdt_fu1", "day": 1, "at_hour": 19},
    {"step": 3, "template": "ukdt_fu2", "day": 3},
    {"step": 5, "template": "ukdt_fu4", "day": 7},
    # Steps 7-10 retired Aug 2026: ~4% reply rate across ~1,800 sends and the
    # main driver of the WhatsApp quality drop to Medium.
]

BST_SEQUENCE = [
    {"step": 1, "template": "eod_all", "day": 0, "at_hour": 20},
    {"step": 2, "template": "bst_fu1", "day": 1, "at_hour": 19},
    {"step": 3, "template": "bst_fu2", "day": 3},
    {"step": 5, "template": "bst_fu4", "day": 7},
]

# 26/10 19:50 (Aaron): after day 7, spread out - day 14, 28, 60, 90 and 180 (last) - using approved templates with
# nothing in them: no "closing", no "limited time", no claims we can't back. Steps 21-25 sit above every number any old
# shape wrote (up to 16), so a lead part-way through anything carries on. "longtail" steps: their own daily allowance
# after the core messages, sent only while WATI shows the template APPROVED, and day-7 finishers carry on into them.
LONGTAIL_DAYS = [(21, 14), (22, 28), (23, 60), (24, 90), (25, 180)]
# 26/10 20:05: every message says something different. New templates (submitted to WATI tonight) take over each step the
# moment WATI shows them APPROVED ("prefer"); until then the existing approved template goes out ("template").
#   UKDT: 14 check-in (council been back?), 28 bury-our-heads, 60 a plan, 90 what happens on the call, 180 rated Excellent
#   BST:  14 check-in (bailiffs been back?), 28 bury-our-heads, 60 your rights, 90 fees set by law, 180 you're not alone
_LT_UKDT = [("ukdt_fu3", None), ("ukdt_nc6", None), ("ukdt_nc7", None), ("ukdt_nc4", "ukdt_lt90"), ("ukdt_cbna4", "ukdt_lt180")]
_LT_BST  = [("bst_fu3", None), ("bst_nc_6", None), ("bst_nc4", "bst_lt60"), ("bst_cbna4", "bst_lt90"), ("bst_fu2", "bst_lt180")]
UKDT_SEQUENCE += [dict({"step": st, "template": t, "day": d, "longtail": True}, **({"prefer": [p]} if p else {})) for (st, d), (t, p) in zip(LONGTAIL_DAYS, _LT_UKDT)]
BST_SEQUENCE += [dict({"step": st, "template": t, "day": d, "longtail": True}, **({"prefer": [p]} if p else {})) for (st, d), (t, p) in zip(LONGTAIL_DAYS, _LT_BST)]
# day 7: no more "your enquiry's set to close tomorrow" now the follow-ups carry on - the check-in template once approved
for _q, _p in ((UKDT_SEQUENCE, "ukdt_day7_checkin"), (BST_SEQUENCE, "bst_day7_checkin")):
    for _c in _q:
        if _c["step"] == 5:
            _c["prefer"] = [_p]

def step_template(cfg):
    """The template to send for a step: the first of its "prefer" list that WATI shows APPROVED, else its own."""
    if cfg.get("prefer"):
        ok = approved_templates()
        for t in cfg["prefer"]:
            if t in ok:
                return t
    return cfg["template"]
LONGTAIL_FIRST_STEP = LONGTAIL_DAYS[0][0]
MAX_LONGTAIL_SENDS_PER_DAY = int(os.getenv("MAX_LONGTAIL_SENDS_PER_DAY", "60"))
_longtail_sends = {}

def _lt_sent():
    today = datetime.datetime.now(UK_TZ).date()
    for d in list(_longtail_sends):
        if d != today:
            del _longtail_sends[d]
    return _longtail_sends.get(today, 0)

_approved = {"at": 0.0, "names": set()}
def approved_templates():
    """Approved template names on Regen's WATI, re-read every 10 minutes. If WATI can't be read the last good list
    stays; with no list at all, nothing that needs approval is sent."""
    if time.time() - _approved["at"] < 600:
        return _approved["names"]
    _approved["at"] = time.time()
    try:
        r = requests.get(f"{WATI_API_URL}/api/v1/getMessageTemplates?pageSize=300&pageNumber=1",
                         headers={"Authorization": f"Bearer {WATI_TOKEN}", "accept": "application/json"}, timeout=30)
        names = {t.get("elementName") for t in (r.json().get("messageTemplates") or [])
                 if "approved" in str(t.get("status", "")).lower()}
        if names:
            _approved["names"] = names
    except Exception as e:
        log.warning(f"approved-templates check failed: {e}")
    return _approved["names"]


def next_step_cfg(sequence, current_step):
    """The next message for a lead whose Current Step (last step number sent) is current_step,
    or None when the sequence is finished for them."""
    for cfg in sequence:
        if cfg["step"] > current_step:
            return cfg
    return None


def step_position(sequence, cfg):
    """'2/4' style position of a step, for logs."""
    return f"{sequence.index(cfg) + 1}/{len(sequence)}"

STOPPED_STATUSES = {
    "replied", "completed", "opted out", "converted",
    "do not contact", "dnc", "dnq", "callback", "interested",
    "agreed", "lead passed", "verified", "moc set", "moc approved",
    "cbna", "callback set", "call back", "cancelled money wellness",
    "cancelled not interested (provide reason in notes)",
    "transferred to non ip",
    # 25/10: the CRM dialler's outcomes. Once an agent has spoken to someone, they hung up, or the
    # case was closed for any reason, the chase stops - the team owns that conversation.
    # ("no contact" is not matched: it is the dialler's no-answer outcome, so the chase continues.)
    "contacted", "client hung up", "cancelled", "not interested",
    "incorrect number", "already in an iva", "low debt level",
}
BOOKING_PENDING_STATUS = "booking pending"
BOOKING_SEQUENCE_ACTIVE_STATUS = "sequence active"
BOOKING_PENDING_DELAY_HOURS = int(os.getenv("BOOKING_PENDING_DELAY_HOURS", "4"))
# 25/10: enquiries from this hour get the NEXT evening's end-of-day message. By 18:00 nobody is on
# shift, so the CRM has already sent them its booking link; a 20:00 EOD on top of W0 + link inside
# two hours was three messages in one evening.
EOD_ROLL_HOUR = int(os.getenv("EOD_ROLL_HOUR", "18"))
# Daytime steps are due from this hour on their day.
DAY_STEP_HOUR = int(os.getenv("DAY_STEP_HOUR", "9"))
# Never two messages to the same person closer than this (EOD 20:00 -> fu1 19:00 next day is 23h).
MIN_GAP_HOURS = float(os.getenv("MIN_GAP_HOURS", "20"))
# A step this long overdue is skipped, not sent: an evening message a day late reads wrong, and a
# daytime one more than three days late (a weekend is two) is stale. The lead simply gets whatever
# is next on the timetable. This is also what stops a catch-up wave when the service was down or
# the timetable changes - nobody gets a pile of old follow-ups in one go.
AT_HOUR_MAX_LATE_HOURS = float(os.getenv("AT_HOUR_MAX_LATE_HOURS", "30"))
DAY_STEP_MAX_LATE_HOURS = float(os.getenv("DAY_STEP_MAX_LATE_HOURS", "72"))
# Skipping writes the tracker; keep one cycle's skips under Google's per-minute write quota.
MAX_SKIPS_PER_CYCLE = int(os.getenv("MAX_SKIPS_PER_CYCLE", "25"))

# ─────────────────────────────────────────────
# Sending Window Logic
# ─────────────────────────────────────────────

def sequence_day_zero(lead_date):
    """Day 0 of a lead's timetable: the evening its end-of-day message goes out.
      - the enquiry day, at midnight
      - the next day for enquiries from EOD_ROLL_HOUR (18:00): by then nobody is on
        shift and the CRM has already sent them its booking link
      - never a Saturday: the EOD copy says "tomorrow" and the office is open Mon-Sat,
        so a Saturday send would promise a Sunday call. Saturday and Sunday enquiries
        therefore both get Sunday's 20:00 message, pointing at Monday.
    Every later step counts its "day" from here, so an evening enquiry's whole
    timetable moves on a day with its EOD and the steps stay in order.
    """
    day = lead_date.replace(hour=0, minute=0, second=0, microsecond=0)
    if lead_date.hour >= EOD_ROLL_HOUR:
        day = day + datetime.timedelta(days=1)
    if day.weekday() == 5:
        day = day + datetime.timedelta(days=1)
    return day


def step_due_at(step_cfg, lead_date, fallback_hours=None):
    """When is this step due? "day" steps are timed from the enquiry (sequence_day_zero),
    at their at_hour for evening messages or DAY_STEP_HOUR (09:00) for daytime ones.
    Old-style delay_hours steps (none in the live sequences now) count from the enquiry."""
    if fallback_hours is not None:
        return lead_date + datetime.timedelta(hours=fallback_hours)
    if "day" not in step_cfg and "delay_hours" in step_cfg:
        return lead_date + datetime.timedelta(hours=step_cfg["delay_hours"])
    if step_cfg.get("dz") == "declan":
        # Declan's day (as his own tenant): after 20:00 counts as the next day; Fri/Sat/Sun start on Monday
        d0 = lead_date
        if d0.hour >= 20:
            d0 = d0 + datetime.timedelta(days=1)
        if d0.weekday() >= 4:
            d0 = d0 + datetime.timedelta(days=7 - d0.weekday())
        d0 = d0.replace(hour=0, minute=0, second=0, microsecond=0)
        return (d0 + datetime.timedelta(days=step_cfg.get("day", 0))).replace(hour=step_cfg.get("at_hour", DAY_STEP_HOUR))
    day = sequence_day_zero(lead_date) + datetime.timedelta(days=step_cfg.get("day", 0))
    return day.replace(hour=step_cfg.get("at_hour", DAY_STEP_HOUR))


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


def fixed_sending_window() -> bool:
    """The old fixed window - Mon-Thu 9am-6pm, Fri 9am-2pm, no Sat/Sun - used only when the
    CRM can't say whether anyone is on shift."""
    uk_now = datetime.datetime.now(UK_TZ)
    wd = uk_now.weekday()
    h  = uk_now.hour

    if wd <= 3:   # Mon-Thu: 9am-6pm
        return 9 <= h < 18
    elif wd == 4: # Fri: 9am-2pm
        return 9 <= h < 14
    else:         # Sat/Sun: never
        return False


# 25/10: the CRM knows who is on shift (the Hub rota on weekdays, weekend sign-ins on Saturday and
# Sunday). It is the same test the CRM uses to decide nobody is in and send its out-of-hours
# booking link, so daytime follow-ups and booking links never go out in the same window.
CRM_PRESENCE_URL = os.getenv("CRM_PRESENCE_URL", "https://regen-crm.onrender.com/api/public/presence")
# 26/10 (Aaron): stop the follow-ups once a client has booked a callback or been spoken to, whether or not
# they ever wrote back on WhatsApp (booking from the link isn't a reply). Covers Regen's and TPA's enquiries.
# The CRM gives hashed phones; fail-open (if the CRM can't be reached, the reply checks still apply).
CRM_STOPS_URL = os.getenv("CRM_STOPS_URL", "https://regen-crm.onrender.com/api/public/chase-stops")
_crm_stops = {"at": 0.0, "booked": set(), "spoken": set(), "dnc": set(), "tpa": set(), "tpa_hours": None}

def crm_stop_reason(phone):
    import hashlib
    if time.time() - _crm_stops["at"] > 120:
        try:
            r = requests.get(CRM_STOPS_URL, timeout=30)
            j = r.json() if r.status_code == 200 else {}
            if j.get("ok"):
                _crm_stops.update(at=time.time(), booked=set(j.get("booked") or []), spoken=set(j.get("spoken") or []), dnc=set(j.get("dnc") or []),
                                  tpa=set(j.get("tpa") or []), tpa_hours=j.get("tpaHours") or None)
        except Exception as e:
            log.warning(f"CRM stop list unavailable: {e}")
    d = re.sub(r"\D", "", str(phone or ""))[-10:]
    if len(d) < 10:
        return None
    h = hashlib.sha256(d.encode()).hexdigest()[:20]
    if h in _crm_stops["dnc"]:
        return "asked to be removed"
    if h in _crm_stops["booked"]:
        return "callback set"
    if h in _crm_stops["spoken"]:
        return "contacted"
    return None


# 26/10 (Aaron): Declan's (TPA) clients follow HIS hours (Mon-Fri 9:00-18:30, Sat/Sun 9:00-15:00, from the CRM), not
# Regen's shifts, for the daytime messages. The CRM's stop list says which numbers are his (his TrustLink + his rota share).
TPA_DEFAULT_HOURS = {"weekday": ["09:00", "18:30"], "weekend": ["09:00", "15:00"]}

def crm_is_tpa(phone):
    import hashlib
    crm_stop_reason(phone)          # refreshes the list when it's stale
    d = re.sub(r"\D", "", str(phone or ""))[-10:]
    return len(d) == 10 and hashlib.sha256(d.encode()).hexdigest()[:20] in _crm_stops["tpa"]

def tpa_open_now():
    h = _crm_stops.get("tpa_hours") or TPA_DEFAULT_HOURS
    now = datetime.datetime.now(UK_TZ)
    rng = h.get("weekend") if now.weekday() >= 5 else h.get("weekday")
    try:
        a, b = [int(x.split(":")[0]) * 60 + int(x.split(":")[1]) for x in rng]
    except Exception:
        a, b = 9 * 60, 18 * 60 + 30
    m = now.hour * 60 + now.minute
    return a <= m <= b
PRESENCE_CACHE_SECONDS = int(os.getenv("PRESENCE_CACHE_SECONDS", "60"))
SEND_EARLIEST_HOUR = int(os.getenv("SEND_EARLIEST_HOUR", "8"))    # daytime steps never before 08:00 ...
SEND_LATEST_HOUR   = int(os.getenv("SEND_LATEST_HOUR", "20"))     # ... or from 20:00, whatever the rota says
_presence = {"at": 0.0, "someone_in": None}


def crm_someone_in():
    """Is anyone on shift in the CRM right now? True / False, or None if the CRM can't say."""
    if not CRM_PRESENCE_URL:
        return None
    if time.time() - _presence["at"] < PRESENCE_CACHE_SECONDS:
        return _presence["someone_in"]
    val = None
    try:
        r = requests.get(CRM_PRESENCE_URL, timeout=30)
        j = r.json() if r.status_code == 200 else {}
        if j.get("ok") and "someoneIn" in j:
            val = bool(j["someoneIn"])
        else:
            log.warning(f"CRM presence check: http {r.status_code} - using the fixed window")
    except Exception as e:
        log.warning(f"CRM presence check failed ({e}) - using the fixed window")
    _presence.update(at=time.time(), someone_in=val)
    return val


def is_within_sending_window() -> bool:
    """Daytime follow-ups go out while someone is on shift in the CRM (within 08:00-20:00 UK);
    if the CRM can't be reached, the old fixed window applies."""
    h = datetime.datetime.now(UK_TZ).hour
    if not (SEND_EARLIEST_HOUR <= h < SEND_LATEST_HOUR):
        return False
    live = crm_someone_in()
    if live is not None:
        return live
    return fixed_sending_window()

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
    if W0_TRACKING_SHEET not in tabs:
        log.info(f"Creating '{W0_TRACKING_SHEET}' tab...")
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": W0_TRACKING_SHEET}}}]}
        ).execute()
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{W0_TRACKING_SHEET}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [["Sent At", "TL-REF", "First Name", "Phone", "Campaign", "Status"]]}
        ).execute()
        log.info(f"'{W0_TRACKING_SHEET}' created.")


def get_all_leads(service) -> list:
    leads = []
    for sheet_tab in [SHEET_NAME, "FLT", W0_TRACKING_SHEET]:
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
                "source":     "w0" if sheet_tab == W0_TRACKING_SHEET else "crm",
            })
    return leads


def get_tracking_data(service) -> dict:
    global _read_failed
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
            _read_failed = True
    return tracking


_NOT_A_REPLY = re.compile(r"filled in your form|full name:|phone number:|can i get more info on this|read your article", re.I)
_EMAIL_RE = re.compile(r"\S+@\S+\.\S+")


def _prior_reply_at(phone, days=7):
    """30/09: the reply webhook can only stamp leads already in the tracker, so a
    lead who replied to their W0 before enrolment got chased anyway (Hayley/Vicky).
    Look at the thread once: a real inbound after our first message, within `days`,
    ignoring Meta's form echo / click-to-WhatsApp greeting. Fail-open (None)."""
    try:
        r = requests.get(f"{WATI_API_URL}/api/v1/getMessages/{format_phone(phone)}?pageSize=40",
                         headers={"Authorization": f"Bearer {WATI_TOKEN}", "accept": "application/json"}, timeout=20)
        items = ((r.json().get("messages") or {}).get("items")) or []
    except Exception as e:
        log.warning(f"prior-reply check failed for {phone}: {e}")
        return None
    def uk(v):
        try:
            return datetime.datetime.fromisoformat(str(v)[:19]).replace(tzinfo=datetime.timezone.utc).astimezone(UK_TZ)
        except Exception:
            return None
    msgs = sorted([(uk(m.get("created")), m) for m in items if uk(m.get("created"))], key=lambda x: x[0])
    outs = [d for d, m in msgs if m.get("owner") is not False and m.get("statusString")]
    if not outs:
        return None
    first_out, cutoff = min(outs), datetime.datetime.now(UK_TZ) - datetime.timedelta(days=days)
    for d, m in reversed(msgs):
        t = m.get("text") or ""
        if m.get("owner") is False and d > first_out and d >= cutoff \
           and not _NOT_A_REPLY.search(t) and not (_EMAIL_RE.search(t) and re.search(r"\d{9,}", t)):
            return d
    return None


def add_to_tracking(service, lead: dict):
    tab = get_tracking_tab(lead["tl_ref"])
    status, replied_at = "active", ""
    prior = _prior_reply_at(lead["phone"])
    if prior:
        status, replied_at = "replied", prior.strftime("%d/%m/%Y %H:%M")
    row = [lead["tl_ref"], lead["phone"], lead["campaign"], lead["first_name"],
           lead["date"], "0", "", status, replied_at]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()
    log.info(f"Added {lead['tl_ref']} to {tab}." + (f" (already replied {replied_at})" if prior else ""))
    if prior:
        try:
            update_sheet1_status(service, lead["phone"], "Replied")
        except Exception:
            pass


def update_w0_tracking_status(service, phone: str, status: str):
    formatted = format_phone(phone)
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{W0_TRACKING_SHEET}'!A:F"
        ).execute()
    except Exception as e:
        log.warning(f"Could not read '{W0_TRACKING_SHEET}' for status update: {e}")
        return

    rows = result.get("values", [])
    if len(rows) < 2:
        return

    for idx, row in enumerate(rows[1:], start=2):
        phone_cell = format_phone(row[3] if len(row) > 3 else "")
        if phone_cell == formatted:
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{W0_TRACKING_SHEET}'!F{idx}",
                valueInputOption="RAW",
                body={"values": [[status]]}
            ).execute()
            log.info(f"{W0_TRACKING_SHEET} row {idx} status → '{status}'")
            return


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


NOT_STOP_PHRASES = ("not contacted", "uncontacted")   # contain "contacted" but mean the opposite


def is_stopped_status(status: str) -> bool:
    status = (status or "").lower()
    for phrase in NOT_STOP_PHRASES:
        status = status.replace(phrase, "")
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
            update_w0_tracking_status(service, phone, "replied")
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

def _w0_first_step_delay(sequence):
    """First-message (W0) leads: when step 1 is the 20:00 end-of-day message it follows
    the normal same-evening rule, exactly like a TrustLink lead. The old 24-hour hold
    only applies if step 1 is a timed follow-up."""
    return None if "at_hour" in sequence[0] else 24


# 27/10: Declan's (TPA) council tax and bailiff leads on Regen's WATI - Susan Dec video, his UKDT CT and BST rota share,
# logged "UKDT TPA" / "BST TPA" by the poller - get his DHD sequence: same days, hours and wording as his own tenant's
# DHD CT / DHD BAI. Fixed clock hours (his team's times, not Regen's rota). Step numbers line up with the Regen
# sequence (1 EOD, 2 day 1, 3 day 2/3, 5 day 6/7), so a lead switching over mid-way carries on at the right point.
# Used only once all ten templates are APPROVED on Regen's WATI; until then his leads stay on the Regen sequence.
TPA_UKDT_SEQUENCE = [
    {"step": 1, "template": "eod_all",        "day": 0,  "at_hour": 20, "dz": "declan", "max_late_h": 4},
    {"step": 2, "template": "ukdt_tpa_day2",  "day": 1,  "at_hour": 13, "min_gap_h": 12, "dz": "declan", "max_late_h": 4},
    {"step": 3, "template": "ukdt_tpa_day3",  "day": 2,  "at_hour": 11, "dz": "declan", "max_late_h": 4},
    {"step": 4, "template": "ukdt_tpa_day5",  "day": 4,  "at_hour": 13, "dz": "declan", "max_late_h": 4},
    {"step": 5, "template": "ukdt_tpa_day7",  "day": 6,  "at_hour": 20, "dz": "declan", "max_late_h": 4, "prefer": ["ukdt_tpa_day7_v2"]},
    {"step": 6, "template": "ukdt_tpa_day11", "day": 10, "at_hour": 13, "dz": "declan", "max_late_h": 4},
]
TPA_BST_SEQUENCE = [
    {"step": 1, "template": "eod_all",        "day": 0,  "at_hour": 20, "dz": "declan", "max_late_h": 4},
    {"step": 2, "template": "bst_tpa_day2",   "day": 1,  "at_hour": 13, "min_gap_h": 12, "dz": "declan", "max_late_h": 4},
    {"step": 3, "template": "bst_tpa_day3",   "day": 2,  "at_hour": 11, "dz": "declan", "max_late_h": 4},
    {"step": 4, "template": "bst_tpa_day5",   "day": 4,  "at_hour": 13, "dz": "declan", "max_late_h": 4, "prefer": ["bst_tpa_day5_v2"]},
    {"step": 5, "template": "bst_tpa_day7",   "day": 6,  "at_hour": 20, "dz": "declan", "max_late_h": 4},
    {"step": 6, "template": "bst_tpa_day11",  "day": 10, "at_hour": 13, "dz": "declan", "max_late_h": 4},
]
_TPA_TEMPLATES = {c["template"] for c in TPA_UKDT_SEQUENCE + TPA_BST_SEQUENCE}
_tpa_ready = {"at": 0.0, "ok": False, "missing": None}


def tpa_templates_ready() -> bool:
    """True once every TPA template is APPROVED on Regen's WATI (checked every 10 min)."""
    if time.time() - _tpa_ready["at"] < 600:
        return _tpa_ready["ok"]
    ok = _tpa_ready["ok"]
    try:
        r = requests.get(f"{WATI_API_URL}/api/v1/getMessageTemplates?pageSize=1000",
                         headers={"Authorization": f"Bearer {WATI_TOKEN}"}, timeout=30)
        approved = {t.get("elementName") for t in (r.json().get("messageTemplates") or [])
                    if str(t.get("status", "")).upper() == "APPROVED"}
        missing = sorted(_TPA_TEMPLATES - approved)
        ok = not missing
        if missing != _tpa_ready["missing"]:
            log.info("TPA sequence: " + ("all templates approved - Declan's leads on his DHD sequence" if ok
                                         else f"waiting on {len(missing)} template(s) {missing} - his leads stay on the Regen sequence"))
        _tpa_ready["missing"] = missing
    except Exception as e:
        log.warning(f"TPA template check failed ({e}) - keeping last answer ({ok})")
    _tpa_ready.update(at=time.time(), ok=ok)
    return ok


def get_sequence(campaign: str) -> list:
    c = campaign.upper()
    if "TPA" in c and tpa_templates_ready():
        return TPA_BST_SEQUENCE if "BST" in c else TPA_UKDT_SEQUENCE
    if "BST" in c:
        return BST_SEQUENCE
    return UKDT_SEQUENCE


def is_allowed_campaign(campaign: str) -> bool:
    """Only send to UKDT CT, BST and UKDT O campaigns."""
    return campaign.strip().upper() in {c.upper() for c in ALLOWED_CAMPAIGNS}


def lead_allowed(lead: dict) -> bool:
    """Allowed campaigns, plus first-message leads logged as plain UKDT (UKDT CT and
    UKDT JOSH VIDS enquiries) from W0_UKDT_START onwards."""
    campaign = lead["campaign"].strip()
    if is_allowed_campaign(campaign):
        return True
    if lead.get("sheet_tab") == W0_TRACKING_SHEET and campaign.upper() == "UKDT":
        d = parse_lead_date(lead["date"])
        return bool(d and d >= W0_UKDT_START)
    return False


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
    # UK clock time. Lead dates and the 20:00 end-of-day step are UK times, so the
    # server clock (UTC) made the EOD "not due" at 20:00 in summer and it slipped a day.
    now      = datetime.datetime.now(UK_TZ).replace(tzinfo=None)
    leads    = get_all_leads(service)
    _leads_read_failed = _read_failed
    _read_failed = False
    tracking = get_tracking_data(service)
    if _read_failed:
        # Half a tracker makes tracked people look new: they would be enrolled again
        # and sent step 1 again. Do nothing this cycle; the next one is 5 minutes away.
        log.warning("Tracker read failed - skipping this cycle so nobody is enrolled or sent twice")
        ping()
        return
    _read_failed = _leads_read_failed
    _sync_today_from_tracking(tracking)
    log.info(f"{len(leads)} leads in sheet | {len(tracking)} in tracking")

    # Phone-level dedup: if a person is stopped under ANY reference (TL/FLT,
    # any tab), skip them everywhere. Prevents duplicate records from one
    # person being messaged after they've replied/passed on another record.
    def _norm_phone(ph):
        d = re.sub(r"\D", "", str(ph))
        return d[-10:] if len(d) >= 10 else d
    # 26/10: "completed" alone now means "finished the old day-7 sequence without replying" - it carries on into the
    # long tail, so it no longer stops anyone. Every other stop ("replied", "completed - duplicate record"...) still does.
    def _track_stop(t):
        st = (t.get("status") or "").strip().lower()
        return is_stopped_status(st) and st != "completed"
    stopped_phones = {
        _norm_phone(t["phone"])
        for t in tracking.values()
        if _track_stop(t)
        and len(_norm_phone(t["phone"])) == 10
    }
    stopped_phones.discard("")
    # 26/10: a person who enquires AGAIN is a new enquiry. Their old record (replied in May, sequence
    # completed in June...) used to stop the new one for good, so a re-enquiry got the first message and
    # nothing else (TPA: Annie 18/10, Stewart 25/10). A stop now only counts against an enquiry when the
    # stopped record is from within SAME_PERSON_DAYS before it (or later). Only enquiries from the last
    # SAME_PERSON_DAYS are judged this way; anything older keeps the old rule, so no old lead wakes up.
    _stop_dates = {}
    for t in tracking.values():
        if _track_stop(t):
            _p = _norm_phone(t["phone"])
            if len(_p) == 10:
                _stop_dates.setdefault(_p, []).append(parse_lead_date(t.get("lead_date", "") or ""))

    def _phone_stopped(lead):
        ph = _norm_phone(lead["phone"])
        if ph not in stopped_phones:
            return False
        d = parse_lead_date(lead["date"])
        if not d or now - d > datetime.timedelta(days=SAME_PERSON_DAYS):
            return True
        return any(sd is None or sd >= d - datetime.timedelta(days=SAME_PERSON_DAYS) for sd in _stop_dates.get(ph, []))

    # One sequence per person. Someone on both the first-message log and the TrustLink
    # feed has two records; only one tracker row may carry their sequence, and a stop
    # status on any of their recent records stops it.
    def _finished_early(lead):
        """Finished at day 7 without replying (tracker "completed") and the long tail still has steps for them."""
        t = tracking.get(lead["tl_ref"].strip())
        if not t or (t.get("status") or "").strip().lower() != "completed":
            return False
        return next_step_cfg(get_sequence(lead["campaign"].strip()), t["current_step"]) is not None

    def _lead_stop(lead):
        st = (lead["status"] or "").lower().strip()
        if not is_stopped_status(st):
            return False
        # finishing wrote "replied" on the lead's W0 row; the tracker row says "completed" when it was no real reply
        if lead.get("sheet_tab") == W0_TRACKING_SHEET and st == "replied" and _finished_early(lead):
            return False
        return True

    same_person = datetime.timedelta(days=SAME_PERSON_DAYS)
    recent_stops = {}
    for _l in leads:
        if _lead_stop(_l):
            _d = parse_lead_date(_l["date"])
            _p = _norm_phone(_l["phone"])
            if _d and len(_p) == 10:
                recent_stops.setdefault(_p, []).append(_d)

    def _stopped_recently(ph, lead_date):
        return any(abs(_d - lead_date) <= same_person for _d in recent_stops.get(ph, []))

    def _index_tracking(trk):
        idx = {}
        for _ref, _t in trk.items():
            _p = _norm_phone(_t["phone"])
            _d = parse_lead_date(_t.get("lead_date", ""))
            if _d and len(_p) == 10:
                idx.setdefault(_p, []).append((_d, -_t["current_step"], _t.get("tracking_tab", ""), _t["row"], _ref))
        return idx
    tracked_by_phone = _index_tracking(tracking)

    def _sequence_owner(ph, lead_date):
        """The tracker ref carrying this person's sequence (furthest along, then the
        earliest enrolled), or None if they are not tracked yet."""
        rows = [r for r in tracked_by_phone.get(ph, []) if abs(r[0] - lead_date) <= same_person]
        return min(rows, key=lambda r: r[1:4])[4] if rows else None

    today_uk = datetime.datetime.now(UK_TZ).date()

    def _lead_priority(lead):
        tl_ref = lead["tl_ref"].strip()
        campaign = lead["campaign"].strip()
        status = lead["status"].lower().strip()
        lead_date = parse_lead_date(lead["date"])
        if (not lead["phone"] or not tl_ref or not lead_date or
                lead_date < CUTOFF_DATE or not lead_allowed(lead) or
                _lead_stop(lead) or _phone_stopped(lead)):
            return (9, datetime.datetime.max, datetime.datetime.max, tl_ref)

        source_is_w0 = lead.get("sheet_tab") == W0_TRACKING_SHEET
        track = tracking.get(tl_ref)
        current_step = track["current_step"] if track else 0
        sequence = get_sequence(campaign)
        next_cfg = next_step_cfg(sequence, current_step)
        if next_cfg is None:
            return (8, datetime.datetime.max, lead_date, tl_ref)

        override_h = None
        if source_is_w0 and current_step == 0:
            override_h = BOOKING_PENDING_DELAY_HOURS if is_booking_pending(status) else _w0_first_step_delay(sequence)
        if is_booking_pending(status) and current_step == 0 and not source_is_w0:
            override_h = BOOKING_PENDING_DELAY_HOURS
        due_at = step_due_at(next_cfg, lead_date, override_h)

        is_today_lead = lead_date.date() == today_uk
        if next_cfg.get("longtail"):
            return (6, due_at, -lead_date.timestamp(), tl_ref)   # long tail only uses what the core messages leave
        if is_today_lead:
            bucket = 0                  # today's live leads before catch-up work
            freshness = lead_date.timestamp()
        elif source_is_w0:
            bucket = 1                  # W0 tracking handoff before CRM backlog
            freshness = -lead_date.timestamp()
        elif track is None:
            bucket = 2                  # enrol missing eligible leads first
            freshness = -lead_date.timestamp()
        elif is_booking_pending(status) and current_step == 0:
            bucket = 3                  # W0W fallback leads before older backlog
            freshness = -lead_date.timestamp()
        elif current_step == 0:
            bucket = 4                  # ordinary W1 before later-step backlog
            freshness = -lead_date.timestamp()
        else:
            bucket = 5
            freshness = -lead_date.timestamp()
        return (bucket, due_at, freshness, tl_ref)

    new_leads_added = 0
    messages_sent   = 0
    stopped_skips   = 0
    person_skips    = 0
    skips           = 0     # steps too late to send, moved past without sending
    daily_cap_logged = False
    backlog_cap_logged = False
    hourly_cap_logged = False
    cycle_cap_logged = False

    for lead in sorted(leads, key=_lead_priority):
        tl_ref   = lead["tl_ref"].strip()
        campaign = lead["campaign"].strip()
        status   = lead["status"].lower().strip()
        booking_pending = is_booking_pending(status)

        if not lead["phone"] or not tl_ref:
            continue

        # Only process allowed campaigns (and UKDT first-message leads from the start date)
        if not lead_allowed(lead):
            continue

        # Skip if status indicates stop
        if _lead_stop(lead):
            continue

        # Skip if this PERSON (by phone) is stopped under any other reference
        if _phone_stopped(lead):
            stopped_skips += 1
            continue

        # Parse lead date
        lead_date = parse_lead_date(lead["date"])
        if not lead_date:
            continue

        # Skip leads added before cutoff date (the 148 stuck leads)
        if lead_date < CUTOFF_DATE:
            continue

        source_is_w0 = lead.get("sheet_tab") == W0_TRACKING_SHEET

        # One sequence per person
        _ph = _norm_phone(lead["phone"])
        if len(_ph) == 10:
            if _stopped_recently(_ph, lead_date):
                stopped_skips += 1
                continue
            _owner = _sequence_owner(_ph, lead_date)
            if _owner is not None and _owner != tl_ref:
                person_skips += 1
                continue

        # Add to tracking if not already there
        if tl_ref not in tracking:
            add_to_tracking(service, lead)
            new_leads_added += 1
            time.sleep(2)   # many leads can enrol at once; stay under Google's 60 reads a minute
            _earlier_failed = _read_failed
            _read_failed = False
            tracking = get_tracking_data(service)
            if _read_failed:
                log.warning("Tracker re-read failed after enrolling - stopping this cycle, the next one carries on")
                break
            _read_failed = _earlier_failed
            tracked_by_phone = _index_tracking(tracking)

        track = tracking.get(tl_ref)
        if not track:
            continue

        # Skip if tracking says replied/otherwise stopped ("completed" with long-tail steps left carries on)
        if is_stopped_status(track["status"]) and not _finished_early(lead):
            continue

        sequence     = get_sequence(campaign)
        current_step = track["current_step"]
        next_msg     = next_step_cfg(sequence, current_step)

        if next_msg is None:
            if track["status"] != "completed" and skips < MAX_SKIPS_PER_CYCLE:
                update_tracking_row(service, track["row"], current_step,
                                    track["last_sent"], "completed",
                                    tracking_tab=track.get("tracking_tab", TRACKING_SHEET))
                skips += 1          # housekeeping writes share the per-cycle write budget
                time.sleep(1)
            continue

        override_h = None
        if source_is_w0 and current_step == 0:
            override_h = BOOKING_PENDING_DELAY_HOURS if booking_pending else _w0_first_step_delay(sequence)
        if booking_pending and current_step == 0 and not source_is_w0:
            override_h = BOOKING_PENDING_DELAY_HOURS
        due_at = step_due_at(next_msg, lead_date, override_h)

        # Minimum gap between two messages to the same person, measured from the last real
        # send. (The old gate waited the step's whole designed gap after the PREVIOUS send,
        # which compounded: fu1 landed 2 days in, fu2 nearly 5, fu4 at 10.)
        last_sent_str = track.get("last_sent", "")
        if last_sent_str:
            try:
                last_dt = datetime.datetime.strptime(last_sent_str, "%d/%m/%Y %H:%M")
                if (now - last_dt).total_seconds() / 3600 < next_msg.get("min_gap_h", MIN_GAP_HOURS):
                    continue  # too soon after the last message - wait
            except ValueError:
                pass  # unparseable date — don't block

        if now >= due_at:
            is_at_hour = "at_hour" in next_msg
            is_lt = bool(next_msg.get("longtail"))
            # Too late to send this one? Skip it without sending and move the lead on to the
            # next step on their timetable. Evening copy a day late reads wrong; a daytime step
            # more than three days late is stale. Nobody ever gets a pile of catch-up messages.
            _late_h = (now - due_at).total_seconds() / 3600
            if _late_h > next_msg.get("max_late_h", AT_HOUR_MAX_LATE_HOURS if is_at_hour else DAY_STEP_MAX_LATE_HOURS):
                if skips >= MAX_SKIPS_PER_CYCLE:
                    continue        # the rest next cycle - tracker writes are rate-limited
                # 26/10: jump past every step that is already too late in ONE write (a lead from May joining the
                # long tail would otherwise take five cycles of writes to reach the step that is actually due)
                target, skipped = next_msg, [next_msg["template"]]
                for cfg in sequence[sequence.index(next_msg) + 1:]:
                    _lim = AT_HOUR_MAX_LATE_HOURS if "at_hour" in cfg else DAY_STEP_MAX_LATE_HOURS
                    if (now - step_due_at(cfg, lead_date, None)).total_seconds() / 3600 > _lim:
                        target = cfg
                        skipped.append(cfg["template"])
                    else:
                        break
                # a lead carrying on from "completed" keeps that status until a long-tail message actually goes
                # (its W0 row says "replied" from finishing; "completed" is what tells the two apart)
                _keep = "completed" if (track.get("status") or "").strip().lower() == "completed" else "active"
                update_tracking_row(service, track["row"], target["step"],
                                    track.get("last_sent", ""), _keep,
                                    tracking_tab=track.get("tracking_tab", TRACKING_SHEET))
                skips += 1
                time.sleep(1)
                log.info(f"{tl_ref}: {', '.join(skipped)} too late (first was due {due_at:%d/%m %H:%M}, "
                         f"{_late_h / 24:.1f} days ago) - skipped without sending")
                continue
            if is_at_hour:
                # Evening messages only ever go out in their own hour (20:xx for the EOD,
                # 19:xx for fu1). Never 2am, never 10pm.
                if datetime.datetime.now(UK_TZ).hour != next_msg["at_hour"]:
                    continue
            elif not (tpa_open_now() if crm_is_tpa(lead["phone"]) else is_within_sending_window()):
                log.debug(f"{tl_ref}: outside sending window, will send next window")
                continue
            if is_lt:
                if next_msg["template"] not in approved_templates():
                    continue        # not approved in WATI yet: wait (no send, no skip)
                if _lt_sent() >= MAX_LONGTAIL_SENDS_PER_DAY:
                    continue
            # at_hour steps (the evening messages) bypass the cycle and hourly caps so
            # a same-day message never queues behind fu catch-up and lands the
            # next day. They still count toward, and are bounded by, the daily
            # and backlog caps below.
            if not is_at_hour and messages_sent >= MAX_SENDS_PER_CYCLE:
                if not cycle_cap_logged:
                    log.info(f"Cycle cap reached ({MAX_SENDS_PER_CYCLE}) - sends paused, enrolment continues")
                    cycle_cap_logged = True
                continue
            if not is_at_hour and _hour_sent() >= MAX_SENDS_PER_HOUR:
                if not hourly_cap_logged:
                    log.info(f"Hourly cap reached ({MAX_SENDS_PER_HOUR}) - catch-up paced, enrolment continues")
                    hourly_cap_logged = True
                continue
            is_backlog_send = _is_backlog_lead_date(lead_date)
            if is_lt:
                pass                # long tail: its own daily allowance (checked above), not the core caps
            elif is_backlog_send and _backlog_sent() >= MAX_BACKLOG_SENDS_PER_DAY:
                if not backlog_cap_logged:
                    log.info(f"Backlog cap reached ({MAX_BACKLOG_SENDS_PER_DAY}) - today/new leads still allowed")
                    backlog_cap_logged = True
                continue
            elif _today_sent() >= MAX_SENDS_PER_DAY:
                if not daily_cap_logged:
                    log.info(f"Daily cap reached ({MAX_SENDS_PER_DAY}) - sends paused, enrolment continues")
                    daily_cap_logged = True
                continue

            # 25/10: look at the WhatsApp thread before every send, not only at enrolment. If they have
            # written to us in the last week the team owns that chat - stop and mark them replied,
            # even when the reply webhook missed it. Fail-open: if WATI can't be asked, send as before.
            # 27/10 (Aaron): asked to be removed -> never another message, booking pending or not
            _stop = crm_stop_reason(lead["phone"])
            if booking_pending and _stop != "asked to be removed":
                _stop = None
            if _stop:
                update_tracking_row(service, track["row"], current_step, track.get("last_sent", ""),
                                    _stop, tracking_tab=track.get("tracking_tab", TRACKING_SHEET))
                log.info(f"{tl_ref}: {_stop} in the CRM - sequence stopped before {next_msg['template']}")
                continue
            # long tail: ANY real reply since we first wrote, however long ago, ends it (not just the last week)
            _replied_at = None if booking_pending else _prior_reply_at(lead["phone"], days=400 if is_lt else 7)
            if _replied_at:
                _stamp = _replied_at.strftime("%d/%m/%Y %H:%M")
                update_tracking_row(service, track["row"], current_step, track.get("last_sent", ""),
                                    "replied", _stamp, tracking_tab=track.get("tracking_tab", TRACKING_SHEET))
                update_lead_status(service, lead, "replied" if source_is_w0 else "Replied")
                log.info(f"{tl_ref}: replied {_stamp} - sequence stopped before {next_msg['template']}")
                continue

            _tpl = step_template(next_msg)
            if send_wati_template(lead["phone"], _tpl, lead["first_name"]):
                new_step   = next_msg["step"]
                finished   = next_step_cfg(sequence, new_step) is None
                new_status = "completed" if finished else "active"
                last_sent  = now.strftime("%d/%m/%Y %H:%M")
                # Advance the sequence step + write last_sent for EVERY lead,
                # including W0-sourced ones. Previously W0-sourced leads only had
                # their W0-tracking status stamped and never advanced current_step,
                # so they re-qualified for nc1 every cycle and were re-sent on a loop.
                update_tracking_row(service, track["row"], new_step, last_sent, new_status,
                                    tracking_tab=track.get("tracking_tab", TRACKING_SHEET))
                if source_is_w0:
                    # the lead IS its W0 Tracking row - write it directly rather than re-reading the
                    # whole tab for every send (66 reads in a minute at 20:00 on 25/10 blew the
                    # 60/min read quota, and replies arriving by webhook in that minute were lost)
                    update_lead_status(service, lead, "replied" if finished else "w0 sent")
                if booking_pending and current_step == 0 and not source_is_w0:
                    update_lead_status(service, lead, BOOKING_SEQUENCE_ACTIVE_STATUS)
                messages_sent += 1
                if is_lt:
                    today_k = datetime.datetime.now(UK_TZ).date()
                    _longtail_sends[today_k] = _longtail_sends.get(today_k, 0) + 1
                    hk = (today_k, datetime.datetime.now(UK_TZ).hour)
                    _hourly_sends[hk] = _hourly_sends.get(hk, 0) + 1     # still part of the hourly pace
                else:
                    _bump_today(is_backlog_send)
                log.info(f"{tl_ref}: {_tpl} ({step_position(sequence, next_msg)}, step {new_step}) (hour total: {_hour_sent()}/{MAX_SENDS_PER_HOUR}, backlog total: {_backlog_sent()}/{MAX_BACKLOG_SENDS_PER_DAY}, day total: {_today_sent()}/{MAX_SENDS_PER_DAY})")
        else:
            hrs = (due_at - now).total_seconds() / 3600
            log.debug(f"{tl_ref}: next msg in {hrs:.1f}h ({next_msg['template']})")

    log.info(f"─── Done — {new_leads_added} new leads added, {messages_sent} messages sent, {skips} late steps skipped, {stopped_skips} phone-stopped skips, {person_skips} same-person skips ───")
    if _read_failed:
        log.warning("Sheet read issue this cycle; keeping Healthchecks as liveness to avoid false DOWN/UP alerts")
    ping()

# ─────────────────────────────────────────────
# Webhook Server
# ─────────────────────────────────────────────

sheets_service_global = None
# 19 Oct: the service froze at 18:11 on 18 Oct when a WhatsApp reply (webhook thread) and the sequence pass used the one
# Google client at the same moment - that client is not thread-safe. One lock for every sheet use, a socket timeout so a
# stalled call can't hang forever, and a watchdog that exits (Railway restarts it) if no pass completes for 30 minutes.
SHEETS_LOCK = threading.Lock()
_last_pass = [time.time()]
import socket as _socket
_socket.setdefaulttimeout(int(os.getenv("SHEETS_TIMEOUT", "60")))


def _watchdog():
    limit = int(os.getenv("WATCHDOG_SECS", "1800"))
    while True:
        time.sleep(60)
        if time.time() - _last_pass[0] > limit:
            log.error("watchdog: no sequence pass for %ss - exiting so Railway restarts the service" % limit)
            try:
                send_alert_email("WATI sequence service froze - restarting", "No sequence pass for %s seconds; the service exited to be restarted." % limit)
            except Exception:
                pass
            os._exit(1)


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
                with SHEETS_LOCK:
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
    log.info(f"Daytime steps send while someone is on shift in the CRM ({CRM_PRESENCE_URL or 'off'}); fallback window Mon-Thu 9am-6pm, Fri 9am-2pm")
    log.info(f"Alert email: {ALERT_EMAIL}")

    consecutive_errors = 0

    while True:
        try:
            creds = get_google_credentials()
            sheets_service_global = build("sheets", "v4", credentials=creds, cache_discovery=False)
            ensure_tracking_sheet(sheets_service_global)
            start_webhook_server()
            start_healthcheck_heartbeat()
            threading.Thread(target=_watchdog, daemon=True).start()

            log.info("Service started successfully.")
            consecutive_errors = 0

            while True:
                try:
                    with SHEETS_LOCK:
                        process_sequences(sheets_service_global)
                    _last_pass[0] = time.time()
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

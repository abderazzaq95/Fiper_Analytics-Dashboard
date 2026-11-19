"""Reply reminders: one clean queue of customers waiting for an agent.

Every few minutes (scheduled in main.py) ``run_cycle()``:

1. Reads the recent WhatsApp timeline + Maqsam calls and works out which
   customers are waiting (see reply_tracker.compute_wait_state).
2. Keeps the ``pending_replies`` table in sync: one open row per customer
   phone; rows close automatically when an agent replies or calls.
3. During working hours, sends a short reminder ladder per customer:
      REMINDER_FIRST_MIN    (default 15 working min) -> reminder 1 to agent
      REMINDER_SECOND_MIN   (default 45)             -> reminder 2 to agent
      REMINDER_ESCALATE_MIN (default 120)            -> supervisor escalation
   Then it stops pinging the agent. If the customer is still unanswered the
   supervisor gets follow-ups REMINDER_FOLLOWUP_HOURS after the first
   escalation (default 24h and 72h). Waits older than
   REMINDER_AGENT_MAX_WORKING_HOURS (default 10 = one working day) get no new
   pings or escalations; instead each agent gets ONE morning list per working
   day (REMINDER_MORNING_LIST_TIME). Waits are closed as expired after
   REMINDER_TRACK_MAX_AGE_HOURS.
   AI check (pipeline/reply_ai.py, REMINDER_AI_MODE): Gemini decides per wait
   whether a reply is needed and how urgent it is. In "live" it skips waits
   that need no reply and uses faster (urgent) or lighter (low) ladders.
4. Re-checks the database right before sending, so a customer who was just
   answered is never included. Each step is recorded BEFORE it is sent, so a
   database error can never make the same message repeat every cycle.

REMINDER_MODE:
  off    - do nothing
  shadow - (default) track and log everything; agents receive NOTHING.
           Escalations + a digest of "would have sent" go to
           REMINDER_ESCALATION_EMAILS marked [SHADOW].
  live   - send to agents (email, Telegram, WhatsApp) and escalate.
"""
from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline import agent_names
from pipeline import email_notifications as notify
from pipeline import reminder_templates as templates
from pipeline import reply_ai
from pipeline.reply_tracker import (
    Ladder,
    Schedule,
    compute_wait_state,
    due_actions,
    fmt_minutes,
    parse_dt,
    parse_hours_list,
    parse_work_days,
    phone_key,
)

log = logging.getLogger("fiper.reminders")
supabase = notify.supabase

TABLE = "pending_replies"
_LINE_LABELS = {"96897245526": "Oman", "905318880855": "Turkey"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def mode() -> str:
    value = os.getenv("REMINDER_MODE", "shadow").strip().lower()
    return value if value in {"off", "shadow", "live"} else "shadow"


SCHEDULE = Schedule(
    tz=ZoneInfo(os.getenv("REMINDER_TIMEZONE", os.getenv("REPORT_TIMEZONE", "Etc/GMT-2"))),
    work_days=parse_work_days(os.getenv("REMINDER_WORK_DAYS", "mon-fri")),
    start_hour=_env_int("REMINDER_WORK_START_HOUR", 9),
    end_hour=_env_int("REMINDER_WORK_END_HOUR", 19),
)
TZ_LABEL = os.getenv("REMINDER_TIMEZONE_LABEL", os.getenv("REPORT_TIMEZONE_LABEL", "UTC+2"))
LADDER = Ladder(
    first_min=_env_int("REMINDER_FIRST_MIN", 15),
    second_min=_env_int("REMINDER_SECOND_MIN", 45),
    escalate_min=_env_int("REMINDER_ESCALATE_MIN", 120),
    followup_hours=parse_hours_list(os.getenv("REMINDER_FOLLOWUP_HOURS", "24,72")),
    agent_max_working_hours=_env_int("REMINDER_AGENT_MAX_WORKING_HOURS", 10),
)
_urgent = parse_hours_list(os.getenv("REMINDER_URGENT_MINUTES", "5,20,60"), (5, 20, 60))
URGENT_STEPS = _urgent if len(_urgent) == 3 else (5, 20, 60)
LOW_FIRST_MIN = _env_int("REMINDER_LOW_FIRST_MIN", 30)
AI_MAX_PER_CYCLE = _env_int("REMINDER_AI_MAX_PER_CYCLE", 8)
# "HH:MM" local time for the daily list of older waits per agent; "off" disables it.
MORNING_LIST_AT = os.getenv("REMINDER_MORNING_LIST_TIME", "09:15").strip().lower()
MORNING_LIST_MAX_CARDS = 15


def ladder_for(row: dict) -> Ladder:
    """Priority ladder, only once the AI check is live (shadow = record only)."""
    if reply_ai.mode() != "live":
        return LADDER
    return LADDER.for_priority(row.get("priority"), URGENT_STEPS, LOW_FIRST_MIN)


def no_reply_needed(row: dict) -> bool:
    return reply_ai.mode() == "live" and row.get("needs_reply") is False
# How long an unanswered customer is tracked (calendar). Must cover the last
# follow-up plus a weekend; after this the wait is closed as "expired".
MAX_AGE_HOURS = max(_env_int("REMINDER_TRACK_MAX_AGE_HOURS", 168), int(max(LADDER.followup_hours, default=0)) + 72)
LOOKBACK_HOURS = MAX_AGE_HOURS + 6
AGENT_MAX_PER_HOUR = _env_int("REMINDER_AGENT_MAX_PER_HOUR", 6)
SHADOW_DIGEST_MINUTES = _env_int("REMINDER_SHADOW_DIGEST_MINUTES", 60)

# Process-local state (reset on restart, which is harmless).
_agent_send_log: dict[str, list[datetime]] = defaultdict(list)
_shadow_buffer: list[dict] = []
_last_shadow_flush: datetime | None = None
_last_cycle: dict = {}
_morning_sent: dict[str, set[str]] = {}  # local date -> agents that got today's list
_ai_columns_warned = False


def _escalation_emails() -> list[str]:
    # Deliberately no fallback to the sales supervisor list: escalations go
    # only where REMINDER_ESCALATION_EMAILS points.
    return notify._split_recipients(os.getenv("REMINDER_ESCALATION_EMAILS", ""))


def _escalation_telegram() -> str:
    return os.getenv("REMINDER_ESCALATION_TELEGRAM_CHAT_ID", "").strip() or notify.TELEGRAM_ADMIN_CHAT_ID


def _escalation_whatsapp() -> str:
    return os.getenv("REMINDER_ESCALATION_WHATSAPP", "").strip()


# ── Data loading ──────────────────────────────────────────────────────────────

def _paginate(build_query) -> list[dict]:
    return notify._paginate(build_query)


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _clean_agent(name: str | None) -> str | None:
    value = (name or "").strip()
    if not value or value.lower() in {"unknown", "team", "n/a", "none"}:
        return None
    if notify._looks_like_uuid(value):
        resolved = notify.resolve_agent_name(value)
        if not resolved or notify._looks_like_uuid(resolved):
            return None
        value = resolved.strip()
    return value or None


def _load_conversations(now: datetime) -> dict[str, dict]:
    """Group recent WhatsApp messages by customer phone."""
    since = (now - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    messages = _paginate(
        lambda: supabase.table("messages")
        .select("lead_id,direction,body,sent_at,agent_name,whatsapp_business_number")
        .gte("sent_at", since)
        .order("sent_at", desc=False)
    )
    lead_ids = list({m["lead_id"] for m in messages if m.get("lead_id")})
    leads: dict[str, dict] = {}
    for chunk in _chunks(lead_ids, 150):
        rows = (
            supabase.table("leads")
            .select("id,phone,wa_contact_id,name,assigned_agent,channel,updated_at,whatsapp_business_number")
            .in_("id", chunk)
            .execute()
            .data
            or []
        )
        leads.update({r["id"]: r for r in rows})

    convs: dict[str, dict] = {}
    for m in messages:
        lead = leads.get(m.get("lead_id"))
        if not lead:
            continue
        key = phone_key(lead.get("phone")) or phone_key(lead.get("wa_contact_id")) or f"lead:{lead['id']}"
        conv = convs.setdefault(key, {"phone": key, "messages": [], "leads": {}})
        conv["messages"].append(m)
        conv["leads"][lead["id"]] = lead
    return convs


def _load_call_times(phones: list[str], since: datetime) -> dict[str, list[datetime]]:
    """Maqsam calls with these phone numbers since `since`, keyed by phone."""
    real_phones = [p for p in phones if not p.startswith("lead:")]
    if not real_phones:
        return {}
    lead_phone: dict[str, str] = {}
    for chunk in _chunks(real_phones, 100):
        variants = list({v for p in chunk for v in (p, f"+{p}")})
        rows = supabase.table("leads").select("id,phone").in_("phone", variants).execute().data or []
        for r in rows:
            key = phone_key(r.get("phone"))
            if key:
                lead_phone[r["id"]] = key
    calls_by_phone: dict[str, list[datetime]] = defaultdict(list)
    for chunk in _chunks(list(lead_phone), 150):
        rows = (
            supabase.table("calls")
            .select("lead_id,called_at")
            .in_("lead_id", chunk)
            .gte("called_at", since.isoformat())
            .execute()
            .data
            or []
        )
        for r in rows:
            dt = parse_dt(r.get("called_at"))
            if dt and r.get("lead_id") in lead_phone:
                calls_by_phone[lead_phone[r["lead_id"]]].append(dt)
    return calls_by_phone


def _canonical_agent(name: str | None) -> str | None:
    """Same person, one name (pipeline/agent_names.py, then the contact sheet)."""
    if not name:
        return None
    known = agent_names.canonical(name)
    if known:
        return known
    try:
        contact = notify._agent_contact(name)
    except Exception:
        contact = None
    return agent_names.display((contact or {}).get("name") or name)


def _pick_agent(conv: dict, last_agent: str | None) -> str | None:
    """Current owner: assigned agent on the most recently updated WhatsApp row."""
    rows = sorted(
        conv["leads"].values(),
        key=lambda r: (r.get("channel") == "whatsapp", r.get("updated_at") or ""),
        reverse=True,
    )
    for r in rows:
        agent = _clean_agent(r.get("assigned_agent"))
        if agent:
            return _canonical_agent(agent)
    return _canonical_agent(_clean_agent(last_agent))


def _pick_name(conv: dict) -> str:
    for r in conv["leads"].values():
        if (r.get("name") or "").strip():
            return r["name"].strip()
    return ""


# ── Sync pending_replies ──────────────────────────────────────────────────────

def _sync(now: datetime) -> tuple[list[dict], dict]:
    convs = _load_conversations(now)
    open_rows = _paginate(lambda: supabase.table(TABLE).select("*").eq("status", "open"))
    open_by_phone = {r["phone"]: r for r in open_rows}

    phones_to_check = set(open_by_phone)
    oldest = now - timedelta(hours=LOOKBACK_HOURS)
    # Pre-pass without calls to find candidates, then fetch calls only for them.
    states = {k: compute_wait_state(c["messages"]) for k, c in convs.items()}
    phones_to_check |= {k for k, s in states.items() if s.waiting_since}
    calls = _load_call_times(sorted(phones_to_check), oldest)
    for k in phones_to_check:
        if k in convs:
            states[k] = compute_wait_state(convs[k]["messages"], calls.get(k))

    max_age = timedelta(hours=MAX_AGE_HOURS)
    stats = {"waiting": 0, "opened": 0, "answered": 0, "expired": 0}
    current: list[dict] = []

    for key, state in states.items():
        if not state.waiting_since:
            continue
        conv = convs[key]
        row = open_by_phone.pop(key, None)
        if now - state.waiting_since > max_age:
            if row:
                _close(row, "expired", "too_old", now)
                stats["expired"] += 1
            continue

        inbound = state.last_inbound or {}
        fields = {
            "lead_id": inbound.get("lead_id"),
            "customer_name": _pick_name(conv) or None,
            "agent_name": _pick_agent(conv, state.last_agent_name),
            "business_line": inbound.get("whatsapp_business_number"),
            "last_inbound_at": inbound.get("sent_at"),
            "last_inbound_body": (inbound.get("body") or "")[:1000],
            "updated_at": now.isoformat(),
        }
        if row and parse_dt(row["waiting_since"]) != state.waiting_since:
            # A new episode: the old one was answered between cycles.
            _close(row, "answered", "agent_contact", now)
            stats["answered"] += 1
            row = None
        if row:
            supabase.table(TABLE).update(fields).eq("id", row["id"]).execute()
            row.update(fields)
        else:
            new_row = {"phone": key, "waiting_since": state.waiting_since.isoformat(), "status": "open", **fields}
            try:
                res = supabase.table(TABLE).insert(new_row).execute()
                row = (res.data or [new_row])[0]
                stats["opened"] += 1
            except Exception as exc:  # unique-index race with a parallel run
                log.warning(f"[reminders] insert skipped for {key[-4:]}: {exc}")
                continue
        stats["waiting"] += 1
        row["_context"] = _ai_context(conv, state.waiting_since)  # in memory only, never written
        current.append(row)

    # Open rows whose customer is no longer waiting: answered (or expired).
    for key, row in open_by_phone.items():
        state = states.get(key)
        if state and state.last_agent_contact_at and state.last_agent_contact_at >= (parse_dt(row["waiting_since"]) or now):
            _close(row, "answered", "agent_contact", now, answered_at=state.last_agent_contact_at)
            stats["answered"] += 1
        elif now - (parse_dt(row["waiting_since"]) or now) > max_age:
            _close(row, "expired", "too_old", now)
            stats["expired"] += 1
        else:
            # Only banal messages left, or data left the lookback window.
            _close(row, "answered", "no_longer_waiting", now)
            stats["answered"] += 1
    return current, stats


def _ai_context(conv: dict, waiting_since: datetime) -> dict:
    """What the AI check reads: customer messages of this wait + the agent's last message."""
    customer, agent_last, any_outbound = [], None, False
    for m in conv["messages"]:
        dt = parse_dt(m.get("sent_at"))
        direction = (m.get("direction") or "").lower()
        if direction == "outbound":
            any_outbound = True
            if dt and dt < waiting_since:
                agent_last = m.get("body")
        elif direction == "inbound" and dt and dt >= waiting_since:
            customer.append((m.get("body") or "").strip() or "[media]")
    return {"customer": customer[-5:], "agent_last": agent_last, "first_contact": not any_outbound}


# ── Does it need a reply? How urgent? ─────────────────────────────────────────

def _classify(rows: list[dict], now: datetime) -> dict:
    """Store an AI (or word-rule) decision for each wait whose latest customer
    message has not been checked yet. At most AI_MAX_PER_CYCLE Gemini calls;
    the rest get the word rules now and the AI on a later cycle."""
    global _ai_columns_warned
    stats = {"ai": 0, "rules": 0, "no_reply_needed": 0}
    if reply_ai.mode() == "off" or not rows:
        return stats
    if "ai_checked_inbound_at" not in rows[0]:
        if not _ai_columns_warned:
            log.warning("[reminders] AI check skipped: run migrations/003_reply_priority.sql in Supabase")
            _ai_columns_warned = True
        return stats

    def checked(row):
        return parse_dt(row.get("ai_checked_inbound_at")) == parse_dt(row.get("last_inbound_at"))

    def in_window(row):
        return SCHEDULE.working_minutes_between(parse_dt(row["waiting_since"]), now) <= LADDER.agent_max_working_hours * 60

    todo = [r for r in rows if r.get("_context") and (not checked(r) or (r.get("ai_source") == "rules" and reply_ai.available()))]
    # Fresh waits first (their reminders depend on it), never-checked before rule-checked.
    todo.sort(key=lambda r: (not in_window(r), checked(r), r.get("waiting_since") or ""))
    ai_budget = AI_MAX_PER_CYCLE
    for row in todo:
        if ai_budget > 0 and reply_ai.available():
            ai_budget -= 1
            decision = reply_ai.classify(row["_context"])
        elif checked(row):
            continue  # already has the word-rule decision; wait for AI budget
        else:
            decision = reply_ai.rules(row["_context"])
        fields = {
            "needs_reply": decision["needs_reply"],
            "priority": decision["priority"],
            "ai_reason": decision["reason"] or None,
            "ai_source": decision["source"],
            "ai_checked_inbound_at": row.get("last_inbound_at"),
        }
        try:
            supabase.table(TABLE).update(fields).eq("id", row["id"]).execute()
        except Exception as exc:
            log.warning(f"[reminders] could not store AI decision: {notify_safe(exc)}")
            continue
        row.update(fields)
        stats[decision["source"]] += 1
        stats["no_reply_needed"] += int(decision["needs_reply"] is False)
    return stats


def _close(row: dict, status: str, reason: str, now: datetime, answered_at: datetime | None = None) -> None:
    supabase.table(TABLE).update({
        "status": status,
        "resolved_by": reason,
        "resolved_at": (answered_at or now).isoformat(),
        "updated_at": now.isoformat(),
    }).eq("id", row["id"]).eq("status", "open").execute()


# ── Pre-send guard ────────────────────────────────────────────────────────────

def _still_waiting(rows: list[dict]) -> list[dict]:
    """Fresh re-read right before sending: drop anyone who got an agent reply."""
    if not rows:
        return rows
    phones = [r["phone"] for r in rows if not r["phone"].startswith("lead:")]
    lead_ids_by_phone: dict[str, set[str]] = defaultdict(set)
    for chunk in _chunks(phones, 100):
        variants = list({v for p in chunk for v in (p, f"+{p}")})
        for r in supabase.table("leads").select("id,phone").in_("phone", variants).execute().data or []:
            key = phone_key(r.get("phone"))
            if key:
                lead_ids_by_phone[key].add(r["id"])
    for r in rows:
        if r.get("lead_id"):
            lead_ids_by_phone[r["phone"]].add(r["lead_id"])

    all_ids = list({i for ids in lead_ids_by_phone.values() for i in ids})
    since = min(parse_dt(r["waiting_since"]) for r in rows).isoformat()
    latest_out: dict[str, datetime] = {}
    for chunk in _chunks(all_ids, 150):
        for m in (
            supabase.table("messages").select("lead_id,sent_at")
            .in_("lead_id", chunk).eq("direction", "outbound").gte("sent_at", since)
            .execute().data or []
        ):
            dt = parse_dt(m.get("sent_at"))
            if dt and (m["lead_id"] not in latest_out or dt > latest_out[m["lead_id"]]):
                latest_out[m["lead_id"]] = dt

    keep = []
    for r in rows:
        since_r = parse_dt(r["waiting_since"])
        replied = any(latest_out.get(i) and latest_out[i] >= since_r for i in lead_ids_by_phone.get(r["phone"], ()))
        if replied:
            log.info(f"[reminders] pre-send guard dropped …{r['phone'][-4:]} (agent replied)")
        else:
            keep.append(r)
    return keep


# ── Message rendering ─────────────────────────────────────────────────────────

def _local(dt_value) -> str:
    dt = parse_dt(dt_value)
    return dt.astimezone(SCHEDULE.tz).strftime("%d %b %H:%M") if dt else "-"


def _customer_label(row: dict) -> str:
    phone = row["phone"] if not row["phone"].startswith("lead:") else ""
    name = (row.get("customer_name") or "").strip()
    shown = f"+{phone}" if phone else "unknown number"
    return f"{name} ({shown})" if name else shown


def _line_label(row: dict) -> str:
    line = re.sub(r"\D+", "", row.get("business_line") or "")
    return _LINE_LABELS.get(line, line)


def _short(text: str | None, limit: int = 180) -> str:
    value = re.sub(r"\s+", " ", (text or "").strip())
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _tpl_item(item: dict) -> dict:
    """Engine item -> template input (see pipeline/reminder_templates.py)."""
    r = item["row"]
    return {
        "name": r.get("customer_name"),
        "phone": r.get("phone"),
        "line": r.get("business_line"),
        "elapsed_min": item["elapsed"],
        "since": parse_dt(r.get("waiting_since")),
        "body": r.get("last_inbound_body"),
        "stage": item["stage"],
        "agent": r.get("agent_name"),
        "reminders": r.get("reminder_count") or 0,
        "esc_stage": item.get("esc_stage", 0),  # 0 new escalation, 1 first follow-up, 2 final ...
        # Shown only once the AI check is live; in shadow it is only recorded.
        "priority": r.get("priority") if reply_ai.mode() == "live" else None,
    }


def _agent_texts(agent: str, items: list[dict], other_waiting: int) -> tuple[str, str, str]:
    return templates.agent_reminder(agent, [_tpl_item(i) for i in items], other_waiting, SCHEDULE.tz, TZ_LABEL)


def _escalation_texts(items: list[dict], shadow: bool) -> tuple[str, str, str]:
    after = min((ladder_for(i["row"]).escalate_min or LADDER.escalate_min for i in items), default=LADDER.escalate_min)
    return templates.escalation([_tpl_item(i) for i in items], SCHEDULE.tz, TZ_LABEL, after, shadow,
                                followup_hours=LADDER.followup_hours)


# ── Delivery ──────────────────────────────────────────────────────────────────

def _log(row: dict | None, agent: str | None, channel: str, recipient: str | None, status: str,
         subject: str, preview: str, meta: dict, error: str | None = None) -> None:
    notify._log_notification_delivery(
        alert_id=None,
        lead_id=(row or {}).get("lead_id"),
        agent_name=agent,
        channel=channel,
        recipient=recipient,
        status=status,
        subject=subject,
        message_preview=preview,
        provider_response={"kind": "reply_reminder", **meta},
        error_message=error,
    )


def _send(channel: str, recipient: str | None, subject: str, text: str, body_html: str) -> tuple[str, str | None]:
    if not recipient:
        return "skipped", f"no {channel} recipient"
    try:
        if channel == "email":
            ok = notify._send_email(recipient, subject, body_html)
        elif channel == "telegram":
            ok = _send_telegram_plain(recipient, text)
        elif channel == "whatsapp":
            ok = notify._send_whatsapp_callbell(recipient, text)
        else:
            return "skipped", f"unknown channel {channel}"
        return ("sent", None) if ok else ("skipped", f"{channel} not configured")
    except Exception as exc:
        return "failed", str(exc)[:500]


def _send_telegram_plain(chat_id: str, text: str) -> bool:
    # Plain text on purpose: customer messages may contain * or _ which break Markdown.
    if not (notify.TELEGRAM_BOT_TOKEN and chat_id and text.strip()):
        return False
    response = notify.requests.post(
        f"https://api.telegram.org/bot{notify.TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True},
        timeout=20,
    )
    response.raise_for_status()
    return True


def _deliver_to_agent(agent: str, items: list[dict], other_waiting: int, now: datetime) -> bool:
    subject, text, body_html = _agent_texts(agent, items, other_waiting)
    meta = {"stage": max(i["stage"] for i in items), "pending_reply_ids": [i["row"].get("id") for i in items]}
    first_row = items[0]["row"]

    if mode() == "shadow":
        for i in items:
            _log(i["row"], agent, "all", None, "shadow", subject, text, {**meta, "stage": i["stage"]})
        _shadow_buffer.append({"agent": agent, "items": [_tpl_item(i) for i in items], "other": other_waiting, "at": now})
        return True

    recent = [t for t in _agent_send_log[agent] if now - t < timedelta(hours=1)]
    _agent_send_log[agent] = recent
    if len(recent) >= AGENT_MAX_PER_HOUR:
        _log(first_row, agent, "all", None, "skipped", subject, text, meta, "agent hourly cap reached")
        return False

    contact = notify.resolve_agent_contact(agent)
    targets = {
        "email": (contact.get("email") or "").strip(),
        "telegram": notify._agent_telegram_id(agent),
        "whatsapp": (contact.get("phone") or "").strip(),
    }
    delivered = False
    for channel, recipient in targets.items():
        status, error = _send(channel, recipient, subject, text, body_html)
        _log(first_row, agent, channel, recipient, status, subject, text, meta, error)
        delivered = delivered or status == "sent"
    if delivered:
        _agent_send_log[agent].append(now)
    else:
        log.warning(f"[reminders] no channel delivered for agent {agent!r} (check contacts)")
    return delivered


def _deliver_morning_list(agent: str, items: list[dict], now: datetime) -> bool:
    subject, text, body_html = templates.morning_list(agent, [_tpl_item(i) for i in items], SCHEDULE.tz, TZ_LABEL,
                                                      max_cards=MORNING_LIST_MAX_CARDS)
    meta = {"kind": "morning_list", "stage": "morning", "pending_reply_ids": [i["row"].get("id") for i in items]}
    first_row = items[0]["row"]
    if mode() == "shadow":
        _log(first_row, agent, "all", None, "shadow", subject, text, meta)
        return True
    contact = notify.resolve_agent_contact(agent)
    targets = {"email": (contact.get("email") or "").strip(), "telegram": notify._agent_telegram_id(agent)}
    delivered = False
    for channel, recipient in targets.items():
        status, error = _send(channel, recipient, subject, text, body_html)
        _log(first_row, agent, channel, recipient, status, subject, text, meta, error)
        delivered = delivered or status == "sent"
    return delivered


def _morning_window(now: datetime) -> tuple[str, datetime, datetime] | None:
    """(local day, local midnight UTC, work start UTC) while today's list may go out."""
    if MORNING_LIST_AT in {"", "off", "none", "0"} or not SCHEDULE.is_working_time(now):
        return None
    try:
        hour, minute = (int(x) for x in MORNING_LIST_AT.split(":"))
    except ValueError:
        return None
    local = now.astimezone(SCHEDULE.tz)
    at = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if not (at <= local < at + timedelta(hours=3)):  # a restart at 15:00 sends no "morning" list
        return None
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    work_start = midnight + timedelta(hours=SCHEDULE.start_hour)
    return local.date().isoformat(), midnight.astimezone(timezone.utc), work_start.astimezone(timezone.utc)


def _agents_with_morning_list(since: datetime) -> set[str]:
    rows = (
        supabase.table("notification_deliveries").select("agent_name")
        .gte("sent_at", since.isoformat()).eq("provider_response->>kind", "morning_list")
        .execute().data or []
    )
    return {r["agent_name"] for r in rows if r.get("agent_name")}


def _morning_lists(rows: list[dict], now: datetime) -> int:
    """Once per working day: each agent gets ONE message with the customers
    still waiting from earlier days, instead of separate pings about them."""
    window = _morning_window(now)
    if not window:
        return 0
    day, midnight, work_start = window
    if day not in _morning_sent:
        _morning_sent.clear()
        _morning_sent[day] = _agents_with_morning_list(midnight)  # survives restarts
    done = _morning_sent[day]

    by_agent: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        agent = row.get("agent_name")
        snoozed = parse_dt(row.get("snoozed_until"))
        if not agent or agent in done or no_reply_needed(row) or (snoozed and snoozed > now):
            continue
        since, reminded = parse_dt(row["waiting_since"]), parse_dt(row.get("last_reminded_at"))
        if since and since < work_start and not (reminded and reminded >= work_start):
            by_agent[agent].append(row)
    if not by_agent:
        return 0

    keep_ids = {r["id"] for r in _still_waiting([r for agent_rows in by_agent.values() for r in agent_rows])}
    order = {"urgent": 0, "normal": 1, "low": 2}
    sent = 0
    for agent, agent_rows in by_agent.items():
        done.add(agent)  # before sending: never twice, even if a channel fails
        items = [{"row": r, "elapsed": SCHEDULE.working_minutes_between(parse_dt(r["waiting_since"]), now), "stage": 1}
                 for r in agent_rows if r["id"] in keep_ids]
        if not items:
            continue
        items.sort(key=lambda i: (order.get(i["row"].get("priority") if reply_ai.mode() == "live" else None, 1),
                                  -i["elapsed"]))
        if _deliver_morning_list(agent, items, now):
            sent += 1
    return sent


def _deliver_escalation(items: list[dict]) -> None:
    shadow = mode() == "shadow"
    subject, text, body_html = _escalation_texts(items, shadow)
    meta = {"stage": "escalation", "pending_reply_ids": [i["row"].get("id") for i in items]}
    for email in _escalation_emails() or [None]:
        status, error = _send("email", email, subject, text, body_html)
        _log(items[0]["row"], "supervisor", "email", email, status, subject, text, meta, error)
    if not shadow:
        for channel, recipient in (("telegram", _escalation_telegram()), ("whatsapp", _escalation_whatsapp())):
            if recipient:
                status, error = _send(channel, recipient, subject, text, body_html)
                _log(items[0]["row"], "supervisor", channel, recipient, status, subject, text, meta, error)


def _flush_shadow_digest(now: datetime, force: bool = False) -> None:
    global _last_shadow_flush
    if not _shadow_buffer:
        return
    if not force and _last_shadow_flush and now - _last_shadow_flush < timedelta(minutes=SHADOW_DIGEST_MINUTES):
        return
    recipients = _escalation_emails()
    if not recipients:
        _shadow_buffer.clear()
        return
    subject, body = templates.shadow_digest(list(_shadow_buffer), SCHEDULE.tz, TZ_LABEL)
    try:
        notify._send_email(recipients, subject, body)
    except Exception as exc:
        log.error(f"[reminders] shadow digest failed: {exc}")
    _shadow_buffer.clear()
    _last_shadow_flush = now


# ── Main entry point ──────────────────────────────────────────────────────────

def _mark(row_id: str, fields: dict, now: datetime) -> bool:
    """Record a step BEFORE sending it. If this fails, the step is not sent,
    so a database problem can never turn into the same message every cycle."""
    try:
        supabase.table(TABLE).update({**fields, "updated_at": now.isoformat()}).eq("id", row_id).execute()
        return True
    except Exception as exc:
        log.error(f"[reminders] could not record step for {row_id}: {notify_safe(exc)}")
        return False


def notify_safe(exc: Exception) -> str:
    return re.sub(r"(sb_secret_|eyJ)[A-Za-z0-9._-]+", "[redacted]", str(exc))[:300]


def run_cycle(now: datetime | None = None) -> dict:
    global _last_cycle
    now = now or datetime.now(timezone.utc)
    current_mode = mode()
    if current_mode == "off":
        _last_cycle = {"at": now.isoformat(), "mode": "off"}
        return _last_cycle

    rows, stats = _sync(now)
    result = {"at": now.isoformat(), "mode": current_mode, **stats, "reminded": 0, "escalated": 0}
    try:
        # Also outside working hours, so the morning starts with checked messages.
        result["checked"] = _classify(rows, now)
    except Exception as exc:
        log.error(f"[reminders] AI check failed, reminders continue: {notify_safe(exc)}")

    if not SCHEDULE.is_working_time(now):
        result["note"] = "outside working hours; tracking only"
        _last_cycle = result
        return result

    remind_by_agent: dict[str, list[dict]] = defaultdict(list)
    supervisor: list[dict] = []  # new escalations + follow-ups, one email per cycle
    waiting_per_agent: dict[str, int] = defaultdict(int)
    for row in rows:
        if row.get("agent_name"):
            waiting_per_agent[row["agent_name"]] += 1
        snoozed = parse_dt(row.get("snoozed_until"))
        if (snoozed and snoozed > now) or no_reply_needed(row):
            continue
        elapsed = SCHEDULE.working_minutes_between(parse_dt(row["waiting_since"]), now)
        escalated = bool(row.get("escalated_at"))
        esc_count = int(row.get("escalation_count") or (1 if escalated else 0))
        actions = due_actions(
            elapsed_working_min=elapsed,
            reminder_count=int(row.get("reminder_count") or 0),
            last_reminded_at=parse_dt(row.get("last_reminded_at")),
            escalated=escalated,
            has_agent=bool(row.get("agent_name")),
            now=now,
            ladder=ladder_for(row),
            escalation_count=esc_count,
            first_escalated_at=parse_dt(row.get("escalated_at")),
        )
        item = {"row": row, "elapsed": elapsed, "stage": int(row.get("reminder_count") or 0) + 1}
        if "remind" in actions:
            remind_by_agent[row["agent_name"]].append(item)
        if "escalate" in actions:
            supervisor.append({**item, "esc_stage": 0})
        if "followup" in actions:
            supervisor.append({**item, "esc_stage": esc_count})

    due_rows = [i["row"] for items in remind_by_agent.values() for i in items] + [i["row"] for i in supervisor]
    keep_ids = {r["id"] for r in _still_waiting(due_rows)} if due_rows else set()

    for agent, items in remind_by_agent.items():
        items = [i for i in items if i["row"]["id"] in keep_ids]
        items = [i for i in items if _mark(i["row"]["id"], {
            "reminder_count": i["stage"],
            "last_reminded_at": now.isoformat(),
        }, now)]
        if not items:
            continue
        other = max(0, waiting_per_agent.get(agent, 0) - len(items))
        if _deliver_to_agent(agent, items, other, now):
            result["reminded"] += len(items)

    supervisor = [i for i in supervisor if i["row"]["id"] in keep_ids]
    marked = []
    for i in supervisor:
        if i["esc_stage"] == 0:
            fields = {"escalated_at": now.isoformat(), "escalation_count": 1, "last_escalated_at": now.isoformat()}
        else:
            fields = {"escalation_count": i["esc_stage"] + 1, "last_escalated_at": now.isoformat()}
        if _mark(i["row"]["id"], fields, now):
            marked.append(i)
    if marked:
        _deliver_escalation(marked)
        result["escalated"] = sum(1 for i in marked if i["esc_stage"] == 0)
        result["followups"] = sum(1 for i in marked if i["esc_stage"] > 0)

    try:
        result["morning_lists"] = _morning_lists(rows, now)
    except Exception as exc:
        log.error(f"[reminders] morning list failed: {notify_safe(exc)}")

    if current_mode == "shadow":
        _flush_shadow_digest(now)

    _last_cycle = result
    if result["reminded"] or result["escalated"] or result.get("followups") or result.get("morning_lists") or stats["opened"]:
        log.info(f"[reminders] {result}")
    return result


def status_snapshot() -> dict:
    """Read-only view for the debug endpoint."""
    now = datetime.now(timezone.utc)
    rows = _paginate(lambda: supabase.table(TABLE).select("*").eq("status", "open").order("waiting_since"))
    waiting = []
    for r in rows:
        elapsed = SCHEDULE.working_minutes_between(parse_dt(r["waiting_since"]), now)
        waiting.append({
            "customer": _customer_label(r),
            "agent": r.get("agent_name") or "UNASSIGNED",
            "line": _line_label(r),
            "waiting_since_local": _local(r["waiting_since"]),
            "waiting_working_time": fmt_minutes(elapsed),
            "reminders_sent": r.get("reminder_count") or 0,
            "escalated": bool(r.get("escalated_at")),
            "supervisor_emails_sent": r.get("escalation_count") or (1 if r.get("escalated_at") else 0),
            "last_message": _short(r.get("last_inbound_body")),
            "needs_reply": r.get("needs_reply"),
            "priority": r.get("priority"),
            "ai_reason": r.get("ai_reason"),
            "ai_source": r.get("ai_source"),
        })
    return {
        "mode": mode(),
        "now_is_working_time": SCHEDULE.is_working_time(now),
        "config": {
            "work_days": sorted(SCHEDULE.work_days),
            "work_hours": f"{SCHEDULE.start_hour:02d}:00-{SCHEDULE.end_hour:02d}:00 {TZ_LABEL}",
            "first_min": LADDER.first_min,
            "second_min": LADDER.second_min,
            "escalate_min": LADDER.escalate_min,
            "followup_hours_after_escalation": list(LADDER.followup_hours),
            "agent_max_working_hours": LADDER.agent_max_working_hours,
            "ai_mode": reply_ai.mode(),
            "ai_available_now": reply_ai.available(),
            "urgent_minutes": list(URGENT_STEPS),
            "low_first_min": LOW_FIRST_MIN,
            "morning_list_at": MORNING_LIST_AT,
            "track_max_age_hours": MAX_AGE_HOURS,
            "escalation_emails_configured": len(_escalation_emails()),
        },
        "last_cycle": _last_cycle,
        "waiting_now": waiting,
    }

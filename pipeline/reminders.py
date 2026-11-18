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
   Then it stops pinging the agent. Customers waiting longer than
   REMINDER_MAX_AGE_HOURS are expired and never reminded.
4. Re-checks the database right before sending, so a customer who was just
   answered is never included.

REMINDER_MODE:
  off    - do nothing
  shadow - (default) track and log everything; agents receive NOTHING.
           Escalations + a digest of "would have sent" go to
           REMINDER_ESCALATION_EMAILS marked [SHADOW].
  live   - send to agents (email, Telegram, WhatsApp) and escalate.
"""
from __future__ import annotations

import html
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline import email_notifications as notify
from pipeline.reply_tracker import (
    Ladder,
    Schedule,
    compute_wait_state,
    due_actions,
    fmt_minutes,
    parse_dt,
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
)
MAX_AGE_HOURS = _env_int("REMINDER_MAX_AGE_HOURS", 48)
LOOKBACK_HOURS = max(MAX_AGE_HOURS + 6, _env_int("REMINDER_LOOKBACK_HOURS", 72))
AGENT_MAX_PER_HOUR = _env_int("REMINDER_AGENT_MAX_PER_HOUR", 6)
SHADOW_DIGEST_MINUTES = _env_int("REMINDER_SHADOW_DIGEST_MINUTES", 60)

# Process-local state (reset on restart, which is harmless).
_agent_send_log: dict[str, list[datetime]] = defaultdict(list)
_shadow_buffer: list[dict] = []
_last_shadow_flush: datetime | None = None
_last_cycle: dict = {}


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
            return agent
    return _clean_agent(last_agent)


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


def _agent_texts(agent: str, items: list[dict], other_waiting: int) -> tuple[str, str, str]:
    second = any(i["stage"] == 2 for i in items)
    title_en = "Still waiting — please reply now" if second else "Customers waiting for your reply"
    title_ar = "ما زالوا بانتظار ردك — يرجى الرد الآن" if second else "عملاء بانتظار ردك"
    lines = []
    for i in items:
        r = i["row"]
        line = f" · {_line_label(r)} line" if _line_label(r) else ""
        lines.append(
            f"• {_customer_label(r)}{line}\n"
            f"  waiting {fmt_minutes(i['elapsed'])} (since {_local(r['waiting_since'])} {TZ_LABEL})\n"
            f"  \"{_short(r.get('last_inbound_body'))}\""
        )
    extra = f"\n(+{other_waiting} more customer(s) waiting for you)" if other_waiting else ""
    text = (
        f"⏰ {title_en}\n{title_ar}\n\n"
        f"Hello {agent},\n\n" + "\n\n".join(lines) + extra +
        "\n\nPlease reply in ManyContacts or call the customer.\n"
        "يرجى الرد عبر ManyContacts أو الاتصال بالعميل."
    )
    rows_html = "".join(
        "<tr>"
        f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(_customer_label(i['row']))}</td>"
        f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(_line_label(i['row']) or '-')}</td>"
        f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(fmt_minutes(i['elapsed']))}<br>"
        f"<small>since {html.escape(_local(i['row']['waiting_since']))} {html.escape(TZ_LABEL)}</small></td>"
        f"<td style='padding:8px;border:1px solid #e6eaf2;white-space:pre-wrap'>{html.escape(_short(i['row'].get('last_inbound_body'), 400))}</td>"
        "</tr>"
        for i in items
    )
    body_html = f"""
    <h2>{html.escape(title_en)}</h2>
    <p dir="rtl">{html.escape(title_ar)}</p>
    <p>Hello {html.escape(agent)},</p>
    <table style="border-collapse:collapse;font-size:14px">
      <tr><th align="left">Customer</th><th align="left">Line</th><th align="left">Waiting</th><th align="left">Last message</th></tr>
      {rows_html}
    </table>
    {f"<p>+{other_waiting} more customer(s) waiting for you.</p>" if other_waiting else ""}
    <p>Please reply in ManyContacts or call the customer.</p>
    <p dir="rtl">يرجى الرد عبر ManyContacts أو الاتصال بالعميل.</p>
    """
    subject = f"Fiper — {len(items)} customer(s) waiting for your reply"
    return subject, text, body_html


def _escalation_texts(items: list[dict], shadow: bool) -> tuple[str, str, str]:
    prefix = "[SHADOW] " if shadow else ""
    lines = []
    for i in items:
        r = i["row"]
        lines.append(
            f"• {_customer_label(r)} — agent: {r.get('agent_name') or 'UNASSIGNED'}\n"
            f"  waiting {fmt_minutes(i['elapsed'])} working time (since {_local(r['waiting_since'])} {TZ_LABEL}), "
            f"reminders sent: {r.get('reminder_count') or 0}\n"
            f"  \"{_short(r.get('last_inbound_body'))}\""
        )
    text = f"{prefix}🚨 Customers still unanswered\n\n" + "\n\n".join(lines)
    rows_html = "".join(
        "<tr>"
        f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(_customer_label(i['row']))}</td>"
        f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(i['row'].get('agent_name') or 'UNASSIGNED')}</td>"
        f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(fmt_minutes(i['elapsed']))}<br>"
        f"<small>since {html.escape(_local(i['row']['waiting_since']))} {html.escape(TZ_LABEL)}</small></td>"
        f"<td style='padding:8px;border:1px solid #e6eaf2'>{i['row'].get('reminder_count') or 0}</td>"
        f"<td style='padding:8px;border:1px solid #e6eaf2;white-space:pre-wrap'>{html.escape(_short(i['row'].get('last_inbound_body'), 400))}</td>"
        "</tr>"
        for i in items
    )
    body_html = f"""
    <h2>{prefix}Customers still unanswered</h2>
    <p>These customers have waited longer than {fmt_minutes(LADDER.escalate_min)} of working time
       (or nobody owns the chat).</p>
    <table style="border-collapse:collapse;font-size:14px">
      <tr><th align="left">Customer</th><th align="left">Agent</th><th align="left">Waiting</th>
          <th align="left">Reminders</th><th align="left">Last message</th></tr>
      {rows_html}
    </table>
    """
    subject = f"{prefix}Fiper escalation — {len(items)} customer(s) unanswered"
    return subject, text, body_html


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
        _shadow_buffer.append({"agent": agent, "text": text, "at": now})
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
    blocks = "".join(
        f"<h3>To {html.escape(b['agent'])} at {html.escape(_local(b['at']))}</h3>"
        f"<pre style='white-space:pre-wrap;font-family:inherit;background:#f6f8fb;padding:10px'>{html.escape(b['text'])}</pre>"
        for b in _shadow_buffer
    )
    body = (
        "<h2>[SHADOW] Reminders that WOULD have been sent to agents</h2>"
        "<p>Nothing below was sent to agents. Set REMINDER_MODE=live on Render to start sending.</p>" + blocks
    )
    try:
        notify._send_email(recipients, f"[SHADOW] Fiper reminders preview — {len(_shadow_buffer)} message(s)", body)
    except Exception as exc:
        log.error(f"[reminders] shadow digest failed: {exc}")
    _shadow_buffer.clear()
    _last_shadow_flush = now


# ── Main entry point ──────────────────────────────────────────────────────────

def run_cycle(now: datetime | None = None) -> dict:
    global _last_cycle
    now = now or datetime.now(timezone.utc)
    current_mode = mode()
    if current_mode == "off":
        _last_cycle = {"at": now.isoformat(), "mode": "off"}
        return _last_cycle

    rows, stats = _sync(now)
    result = {"at": now.isoformat(), "mode": current_mode, **stats, "reminded": 0, "escalated": 0}

    if not SCHEDULE.is_working_time(now):
        result["note"] = "outside working hours; tracking only"
        _last_cycle = result
        return result

    remind_by_agent: dict[str, list[dict]] = defaultdict(list)
    escalate: list[dict] = []
    waiting_per_agent: dict[str, int] = defaultdict(int)
    for row in rows:
        if row.get("agent_name"):
            waiting_per_agent[row["agent_name"]] += 1
        snoozed = parse_dt(row.get("snoozed_until"))
        if snoozed and snoozed > now:
            continue
        elapsed = SCHEDULE.working_minutes_between(parse_dt(row["waiting_since"]), now)
        actions = due_actions(
            elapsed_working_min=elapsed,
            reminder_count=int(row.get("reminder_count") or 0),
            last_reminded_at=parse_dt(row.get("last_reminded_at")),
            escalated=bool(row.get("escalated_at")),
            has_agent=bool(row.get("agent_name")),
            now=now,
            ladder=LADDER,
        )
        item = {"row": row, "elapsed": elapsed, "stage": int(row.get("reminder_count") or 0) + 1}
        if "remind" in actions:
            remind_by_agent[row["agent_name"]].append(item)
        if "escalate" in actions:
            escalate.append(item)

    due_rows = [i["row"] for items in remind_by_agent.values() for i in items] + [i["row"] for i in escalate]
    keep_ids = {r["id"] for r in _still_waiting(due_rows)} if due_rows else set()

    for agent, items in remind_by_agent.items():
        items = [i for i in items if i["row"]["id"] in keep_ids]
        if not items:
            continue
        other = max(0, waiting_per_agent.get(agent, 0) - len(items))
        if _deliver_to_agent(agent, items, other, now):
            for i in items:
                supabase.table(TABLE).update({
                    "reminder_count": i["stage"],
                    "last_reminded_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }).eq("id", i["row"]["id"]).execute()
            result["reminded"] += len(items)

    escalate = [i for i in escalate if i["row"]["id"] in keep_ids]
    if escalate:
        _deliver_escalation(escalate)
        for i in escalate:
            supabase.table(TABLE).update({
                "escalated_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }).eq("id", i["row"]["id"]).execute()
        result["escalated"] = len(escalate)

    if current_mode == "shadow":
        _flush_shadow_digest(now)

    _last_cycle = result
    if result["reminded"] or result["escalated"] or stats["opened"]:
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
            "last_message": _short(r.get("last_inbound_body")),
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
            "max_age_hours": MAX_AGE_HOURS,
            "escalation_emails_configured": len(_escalation_emails()),
        },
        "last_cycle": _last_cycle,
        "waiting_now": waiting,
    }

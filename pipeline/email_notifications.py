from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import csv
import html
import io
import os
import re
import time
import unicodedata

import requests
from dotenv import load_dotenv
from supabase import create_client
from pipeline.whatsapp import resolve_agent_name

load_dotenv()

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "Fiper Alerts <onboarding@resend.dev>")
SALES_SUPERVISOR_EMAIL = os.getenv("SALES_SUPERVISOR_EMAIL", "")
AGENT_CONTACTS_CSV_URL = os.getenv(
    "AGENT_CONTACTS_CSV_URL",
    "https://docs.google.com/spreadsheets/d/"
    "1PB6P7V_wJkg6AFBNOGJ6Vb7-g3Mw7G4CPmQzJaZR1a8/"
    "gviz/tq?tqx=out:csv&gid=0",
)
CONTACT_CACHE_SECONDS = int(os.getenv("AGENT_CONTACT_CACHE_SECONDS", "600"))
_contacts_cache: dict = {"loaded_at": 0.0, "contacts": {}}


def _agent_env_key(agent_name: str | None) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", (agent_name or "").upper()).strip("_")
    return f"AGENT_EMAIL_{slug}" if slug else ""


def _normalize_name(name: str | None) -> str:
    value = unicodedata.normalize("NFKD", name or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    value = re.sub(r"\s+", " ", value)
    # Fiper data contains both Jehad and Jihad spellings for the same agent.
    value = value.replace("jihad", "jehad")
    return value


def _name_keys(name: str | None) -> set[str]:
    normalized = _normalize_name(name)
    if not normalized:
        return set()
    compact = normalized.replace(" ", "")
    no_vowels = re.sub(r"[aeiou]", "", compact)
    return {normalized, compact, no_vowels}


def _looks_like_uuid(value: str | None) -> bool:
    if not value:
        return False
    return bool(re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", str(value).strip()))


def _display_agent_name(*values: str | None) -> str | None:
    for value in values:
        if not value:
            continue
        if _looks_like_uuid(value):
            value = resolve_agent_name(value)
        normalized = _normalize_name(value)
        if not normalized:
            continue
        if normalized.lower() in ("unknown", "team", "n/a"):
            continue
        if _looks_like_uuid(value):
            continue
        return normalized
    return None


def _load_agent_contacts() -> dict[str, dict]:
    now = time.time()
    if now - (_contacts_cache.get("loaded_at") or 0) < CONTACT_CACHE_SECONDS:
        return _contacts_cache.get("contacts") or {}

    contacts: dict[str, dict] = {}
    if not AGENT_CONTACTS_CSV_URL:
        _contacts_cache.update({"loaded_at": now, "contacts": contacts})
        return contacts

    try:
        response = requests.get(AGENT_CONTACTS_CSV_URL, timeout=20)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        for row in reader:
            name = (row.get("NAME") or row.get("Name") or "").strip()
            email = (row.get("EMAIL") or row.get("Email") or "").strip()
            phone = (
                row.get("PHONE NUMBER")
                or row.get("Phone Number")
                or row.get("PHONE")
                or row.get("Phone")
                or ""
            ).strip()
            if not (name and email):
                continue
            contact = {"name": name, "email": email, "phone": phone}
            for key in _name_keys(name):
                contacts[key] = contact
    except Exception:
        contacts = _contacts_cache.get("contacts") or {}

    _contacts_cache.update({"loaded_at": now, "contacts": contacts})
    return contacts


def _agent_contact(agent_name: str | None) -> dict | None:
    contacts = _load_agent_contacts()
    for key in _name_keys(agent_name):
        if key in contacts:
            return contacts[key]
    return None


def _lead_agent_name(lead: dict | None, latest_msg: dict | None = None) -> str | None:
    return _display_agent_name(
        lead.get("assigned_agent") if lead else None,
        latest_msg.get("agent_name") if latest_msg else None,
    )


def _lead_phone(lead: dict | None) -> str | None:
    if not lead:
        return None
    phone = (lead.get("phone") or "").strip()
    if phone:
        return phone
    return (lead.get("wa_contact_id") or "").strip() or None


def _paginate(build_query) -> list:
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def _agent_email(agent_name: str | None) -> str:
    env_email = os.getenv(_agent_env_key(agent_name), "")
    if env_email:
        return env_email.strip()
    contact = _agent_contact(agent_name)
    return (contact or {}).get("email", "").strip()


def resolve_agent_contact(agent_name: str | None) -> dict:
    env_email = os.getenv(_agent_env_key(agent_name), "").strip()
    contact = _agent_contact(agent_name) or {}
    return {
        "agent": agent_name or "",
        "name": contact.get("name") or agent_name or "",
        "email": env_email or contact.get("email") or "",
        "phone": contact.get("phone") or "",
        "source": "env" if env_email else ("sheet" if contact else "none"),
    }


def _send_email(to: str, subject: str, html_body: str) -> bool:
    if not (RESEND_API_KEY and to):
        return False
    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": EMAIL_FROM,
            "to": [to],
            "subject": subject,
            "html": html_body,
        },
        timeout=20,
    )
    response.raise_for_status()
    return True


def send_test_notification(to: str, phone: str = "") -> bool:
    html_body = f"""
    <h2>Fiper Test Alert</h2>
    <p>This is a test notification from the Fiper Analytics Dashboard.</p>
    <ul>
      <li><b>Severity:</b> HIGH</li>
      <li><b>Type:</b> test alert</li>
      <li><b>Requested phone:</b> {html.escape(phone or "not provided")}</li>
    </ul>
    """
    return _send_email(to, "Fiper test alert notification", html_body)


def notify_agent_alert(alert: dict) -> bool:
    agent = _display_agent_name(alert.get("agent_name")) or "unknown"
    recipient = _agent_email(agent)
    if not recipient:
        lead_id = alert.get("lead_id")
        if lead_id:
            try:
                lead_rows = _paginate(
                    lambda: supabase.table("leads")
                    .select("id,assigned_agent")
                    .eq("id", lead_id)
                    .limit(1)
                )
                lead = lead_rows[0] if lead_rows else None
                latest_msg_rows = _paginate(
                    lambda: supabase.table("messages")
                    .select("lead_id,direction,body,agent_name,sent_at")
                    .eq("lead_id", lead_id)
                    .order("sent_at", desc=True)
                )
                latest_msg = latest_msg_rows[0] if latest_msg_rows else None
                resolved = _display_agent_name(
                    alert.get("agent_name"),
                    lead.get("assigned_agent") if lead else None,
                    latest_msg.get("agent_name") if latest_msg else None,
                )
                if resolved:
                    agent = resolved
                    recipient = _agent_email(resolved)
            except Exception:
                pass
    if not recipient:
        return False

    severity = alert.get("severity") or "Alert"
    alert_type = (alert.get("type") or "alert").replace("_", " ").title()
    message = alert.get("message") or ""
    lead_id = alert.get("lead_id") or ""
    html_body = f"""
    <h2>New Fiper Alert</h2>
    <p><b>Agent:</b> {html.escape(agent)}</p>
    <p><b>Severity:</b> {html.escape(severity)}</p>
    <p><b>Type:</b> {html.escape(alert_type)}</p>
    <p><b>Message:</b> {html.escape(message)}</p>
    <p><b>Lead ID:</b> {html.escape(str(lead_id))}</p>
    """
    return _send_email(recipient, f"Fiper Alert - {severity} - {alert_type}", html_body)


def send_webhook_health_alert(details: dict) -> bool:
    if not SALES_SUPERVISOR_EMAIL:
        return False

    lag_min = details.get("lag_min")
    latest_activity = details.get("latest_activity_at") or "unknown"
    latest_stored = details.get("latest_stored_at") or "none"
    active_chats = details.get("active_chats") or 0
    stale_lines = details.get("stale_lines") or []
    stale_rows = "".join(
        "<li>"
        f"<b>Line:</b> {html.escape(str(item.get('number') or 'unknown'))}"
        f" | <b>Reason:</b> {html.escape(str(item.get('reason') or 'stale'))}"
        f" | <b>Last seen:</b> {html.escape(str(item.get('last_seen') or 'none'))}"
        f" | <b>Lag:</b> {html.escape(str(item.get('lag_min') if item.get('lag_min') is not None else 'n/a'))} minutes"
        "</li>"
        for item in stale_lines
    )

    html_body = f"""
    <h2>Fiper WhatsApp Webhook Warning</h2>
    <p>
      One or more WhatsApp webhook lines appear to be delayed, disabled, or missing heartbeat updates.
    </p>
    <ul>
      <li><b>Active WhatsApp chats:</b> {html.escape(str(active_chats))}</li>
      <li><b>Latest ManyContacts activity:</b> {html.escape(str(latest_activity))}</li>
      <li><b>Latest stored message:</b> {html.escape(str(latest_stored))}</li>
      <li><b>Lag:</b> {html.escape(str(lag_min))} minutes</li>
    </ul>
    <h3>Stale lines</h3>
    <ul>{stale_rows or '<li>No line details available</li>'}</ul>
    <p>
      Please check ManyContacts -> API / Developers and make sure
      <b>Enable WhatsApp API webhook forwarding</b> is ON and saved.
    </p>
    """
    return _send_email(
        SALES_SUPERVISOR_EMAIL,
        "Fiper Alert - WhatsApp webhook may be disabled",
        html_body,
    )


def send_supervisor_report(report_label: str = "") -> bool:
    if not SALES_SUPERVISOR_EMAIL:
        return False

    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    alerts = (
        supabase.table("alerts")
        .select("severity,type,agent_name,message,created_at,resolved,lead_id")
        .gte("created_at", today)
        .execute()
        .data
    ) or []
    open_alerts = [a for a in alerts if not a.get("resolved")]

    alert_lead_ids = list({a.get("lead_id") for a in open_alerts if a.get("lead_id")})
    lead_map: dict[str, dict] = {}
    latest_msg_map: dict[str, dict] = {}
    latest_call_map: dict[str, dict] = {}
    if alert_lead_ids:
        try:
            lead_rows = _paginate(
                lambda: supabase.table("leads")
                .select("id,phone,wa_contact_id,name,assigned_agent")
                .in_("id", alert_lead_ids)
            )
            lead_map = {r["id"]: r for r in lead_rows if r.get("id")}
        except Exception:
            lead_map = {}
        try:
            msg_rows = _paginate(
                lambda: supabase.table("messages")
                .select("lead_id,direction,body,agent_name,sent_at")
                .in_("lead_id", alert_lead_ids)
                .order("sent_at", desc=True)
            )
            for msg in msg_rows:
                lead_id = msg.get("lead_id")
                if lead_id and lead_id not in latest_msg_map:
                    latest_msg_map[lead_id] = msg
        except Exception:
            latest_msg_map = {}
        try:
            call_rows = _paginate(
                lambda: supabase.table("calls")
                .select("lead_id,agent_name,called_at")
                .in_("lead_id", alert_lead_ids)
                .order("called_at", desc=True)
            )
            for call in call_rows:
                lead_id = call.get("lead_id")
                if lead_id and lead_id not in latest_call_map:
                    latest_call_map[lead_id] = call
        except Exception:
            latest_call_map = {}

    for alert in open_alerts:
        lead = lead_map.get(alert.get("lead_id") or "")
        latest_msg = latest_msg_map.get(alert.get("lead_id") or "", {})
        latest_call = latest_call_map.get(alert.get("lead_id") or "", {})
        if lead:
            alert["lead_phone"] = _lead_phone(lead)
            alert["lead_name"] = lead.get("name")
        alert["agent_name"] = _display_agent_name(
            alert.get("agent_name"),
            _lead_agent_name(lead, latest_msg) if lead else None,
            latest_call.get("agent_name") if latest_call else None,
        ) or alert.get("agent_name")

    calls = _paginate(
        lambda: supabase.table("calls")
        .select("id,outcome,duration_seconds,called_at")
        .gte("called_at", today)
    )
    messages = _paginate(
        lambda: supabase.table("messages")
        .select("id,direction,sent_at")
        .gte("sent_at", today)
    )
    leads = _paginate(
        lambda: supabase.table("leads")
        .select("id,channel,created_at,last_message_at")
        .gte("created_at", today)
    )

    severity_counts = Counter(a.get("severity") or "UNKNOWN" for a in open_alerts)
    type_counts = Counter(a.get("type") or "unknown" for a in open_alerts)
    by_agent: dict[str, list] = defaultdict(list)
    for alert in open_alerts:
        agent = _display_agent_name(alert.get("agent_name")) or "unknown"
        by_agent[agent].append(alert)

    completed = sum(1 for c in calls if (c.get("outcome") or "").lower() == "completed")
    no_answer = sum(1 for c in calls if (c.get("outcome") or "").lower() == "no_answer")
    busy = sum(1 for c in calls if (c.get("outcome") or "").lower() == "busy")
    pickup_base = completed + no_answer + busy
    pickup_rate = round(completed / pickup_base * 100, 1) if pickup_base else 0
    avg_duration = round(
        sum(c.get("duration_seconds") or 0 for c in calls) / len(calls), 1
    ) if calls else 0

    agent_rows = "".join(
        f"<li>{html.escape(agent)}: {len(items)} open alerts</li>"
        for agent, items in sorted(by_agent.items(), key=lambda kv: len(kv[1]), reverse=True)[:10]
    )
    type_rows = "".join(
        f"<li>{html.escape(k.replace('_', ' ').title())}: {v}</li>"
        for k, v in type_counts.most_common(10)
    )

    html_body = f"""
    <h2>Fiper Sales Report {html.escape(report_label)}</h2>
    <p><b>UTC time:</b> {now.strftime('%Y-%m-%d %H:%M')}</p>
    <h3>Work today</h3>
    <ul>
      <li>New leads: {len(leads)}</li>
      <li>Total calls: {len(calls)}</li>
      <li>Pickup rate: {pickup_rate}%</li>
      <li>Avg call duration: {avg_duration}s</li>
      <li>Stored WhatsApp messages: {len(messages)}</li>
      <li>Inbound / outbound messages: {sum(1 for m in messages if m.get('direction') == 'inbound')} / {sum(1 for m in messages if m.get('direction') == 'outbound')}</li>
    </ul>
    <h3>Alerts today</h3>
    <ul>
      <li>Open alerts: {len(open_alerts)}</li>
      <li>High: {severity_counts.get('HIGH', 0)}</li>
      <li>Medium: {severity_counts.get('MED', 0)}</li>
      <li>Low: {severity_counts.get('LOW', 0)}</li>
    </ul>
    <h3>Alert types</h3>
    <ul>{type_rows or '<li>No alerts</li>'}</ul>
    <h3>Agents with alerts</h3>
    <ul>{agent_rows or '<li>No agent alerts</li>'}</ul>
    """
    return _send_email(
        SALES_SUPERVISOR_EMAIL,
        f"Fiper Sales Report {report_label}".strip(),
        html_body,
    )

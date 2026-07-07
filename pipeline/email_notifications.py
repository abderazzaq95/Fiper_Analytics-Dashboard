from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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
SALES_SUPERVISOR_EMAILS = os.getenv("SALES_SUPERVISOR_EMAILS", "")
AGENT_CONTACTS_CSV_URL = os.getenv(
    "AGENT_CONTACTS_CSV_URL",
    "https://docs.google.com/spreadsheets/d/"
    "1PB6P7V_wJkg6AFBNOGJ6Vb7-g3Mw7G4CPmQzJaZR1a8/"
    "gviz/tq?tqx=out:csv&gid=0",
)
CONTACT_CACHE_SECONDS = int(os.getenv("AGENT_CONTACT_CACHE_SECONDS", "600"))
AGENT_CONTACTS_FALLBACK_ENABLED = os.getenv("AGENT_CONTACTS_FALLBACK_ENABLED", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
REPORT_TIMEZONE = os.getenv("REPORT_TIMEZONE", "Asia/Riyadh")
REPORT_TIMEZONE_LABEL = os.getenv("REPORT_TIMEZONE_LABEL", "UTC+3")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "")
_contacts_cache: dict = {"loaded_at": 0.0, "contacts": {}}


def _agent_phone_env_key(agent_name: str | None) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", _normalize_name(agent_name).upper()).strip("_")
    return f"AGENT_WHATSAPP_{slug}" if slug else ""


def _agent_env_key(agent_name: str | None) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", _normalize_name(agent_name).upper()).strip("_")
    return f"AGENT_EMAIL_{slug}" if slug else ""


def _normalize_name(name: str | None) -> str:
    value = unicodedata.normalize("NFKD", name or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    value = re.sub(r"\s+", " ", value)
    # Common spelling variants in Fiper data.
    value = value.replace("jihad", "jehad")
    value = value.replace("mohamad", "mohamed")
    value = value.replace("husein", "hussein")
    value = value.replace("basher", "bashir")
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


def _fmt_open_age(created_at: str | None, now: datetime | None = None) -> str:
    if not created_at:
        return "unknown"
    try:
        created = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        now = now or datetime.now(timezone.utc)
        total_min = max(0, int((now - created).total_seconds() // 60))
    except Exception:
        return "unknown"
    days, rem = divmod(total_min, 1440)
    hours, mins = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if mins or not parts:
        parts.append(f"{mins}m")
    return " ".join(parts)


def _fmt_relative_age(created_at: str | None, now: datetime | None = None) -> str:
    if not created_at:
        return "unknown"
    try:
        created = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        now = now or datetime.now(timezone.utc)
        total_min = max(0, int((now - created).total_seconds() // 60))
    except Exception:
        return "unknown"
    if total_min < 60:
        return f"{total_min}m ago"
    total_hours = total_min // 60
    if total_hours < 24:
        return f"{total_hours}h ago"
    total_days = total_hours // 24
    return f"{total_days}d ago"


def _clean_alert_message(message: str | None) -> str:
    msg = (message or "").strip()
    if not msg:
        return ""
    if re.search(r"unanswered for .+", msg, re.IGNORECASE):
        return "Inbound message unanswered."
    if re.search(r"not contacted in .+", msg, re.IGNORECASE):
        return "Callback lead not contacted."
    if re.search(r"response time is .+", msg, re.IGNORECASE):
        return "Average response time above threshold."
    if re.search(r"Low treatment score \(\d+/100\)", msg):
        return "Low treatment score. Interaction quality needs review."
    if "negative sentiment" in msg.lower():
        return "AI analysis detected negative sentiment in this conversation."
    if "beginner lead" in msg.lower():
        return "Beginner lead needs trading education."
    if "profit expectations" in msg.lower():
        return "Lead raised unrealistic profit expectations."
    return msg


def _alert_type_title(value: str | None) -> str:
    return (value or "alert").replace("_", " ").title()



def _topic_title(value: str | None) -> str:
    return (value or "topic").replace("_", " ").title()


def _short_text(value: str | None, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _pct(part: int, total: int) -> str:
    return f"{round((part / total) * 100, 1)}%" if total else "0%"

def _severity_ar(value: str | None) -> str:
    mapping = {
        "high": "مرتفع",
        "medium": "متوسط",
        "low": "منخفض",
    }
    return mapping.get(str(value or "").strip().lower(), str(value or "تنبيه"))

def _alert_type_ar(value: str | None) -> str:
    slug = str(value or "").strip().lower().replace(" ", "_")
    mapping = {
        "no_reply": "عدم الرد",
        "slow_response": "بطء الرد",
        "beginner_risk": "مخاطر للمبتدئين",
        "poor_treatment": "ضعف جودة التعامل",
        "negative_sentiment": "انطباع سلبي",
        "profit_expectations": "توقعات أرباح غير واقعية",
        "weak_engagement": "ضعف التفاعل",
        "stale_callback": "تأخر المتابعة",
        "alert": "تنبيه",
    }
    return mapping.get(slug, _alert_type_title(value))

def _paginate(build_query) -> list:
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def _split_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[;,\n]+", value)
    return [p.strip() for p in parts if p and p.strip()]


def _supervisor_recipients() -> list[str]:
    recipients = []
    seen = set()
    for raw in [SALES_SUPERVISOR_EMAILS, SALES_SUPERVISOR_EMAIL]:
        for email in _split_recipients(raw):
            key = email.lower()
            if key in seen:
                continue
            seen.add(key)
            recipients.append(email)
    return recipients


def _agent_email(agent_name: str | None) -> str:
    env_email = os.getenv(_agent_env_key(agent_name), "")
    if env_email:
        return env_email.strip()
    if not AGENT_CONTACTS_FALLBACK_ENABLED:
        return ""
    contact = _agent_contact(agent_name)
    return (contact or {}).get("email", "").strip()


def resolve_agent_contact(agent_name: str | None) -> dict:
    env_email = os.getenv(_agent_env_key(agent_name), "").strip()
    env_phone = os.getenv(_agent_phone_env_key(agent_name), "").strip()
    contact = _agent_contact(agent_name) if AGENT_CONTACTS_FALLBACK_ENABLED else None
    contact = contact or {}
    return {
        "agent": agent_name or "",
        "name": contact.get("name") or agent_name or "",
        "email": env_email or contact.get("email") or "",
        "phone": env_phone or contact.get("phone") or "",
        "source": "env" if (env_email or env_phone) else ("sheet" if contact else "none"),
    }


def _send_email(to: str | list[str], subject: str, html_body: str) -> bool:
    recipients = [to] if isinstance(to, str) else list(to or [])
    recipients = [email.strip() for email in recipients if email and email.strip()]
    if not (RESEND_API_KEY and recipients):
        return False
    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": EMAIL_FROM,
            "to": recipients,
            "subject": subject,
            "html": html_body,
        },
        timeout=20,
    )
    response.raise_for_status()
    return True




def _normalize_whatsapp_target(phone: str | None) -> str:
    digits = re.sub(r"\D+", "", phone or "")
    if not digits:
        return ""
    if phone and str(phone).strip().startswith("+"):
        return f"whatsapp:{str(phone).strip()}"
    return f"whatsapp:+{digits}"


def _send_whatsapp(phone: str | None, body: str) -> bool:
    target = _normalize_whatsapp_target(phone)
    sender = (TWILIO_WHATSAPP_FROM or "").strip()
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and sender and target and body.strip()):
        return False
    sender = sender if sender.startswith("whatsapp:") else f"whatsapp:{sender}"
    response = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        data={
            "From": sender,
            "To": target,
            "Body": body.strip(),
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
    wa_body = (
        "Hello Test Agent / ???????\n"
        "You have a High Test Alert that needs action.\n"
        "???? ????? ?????? ????? ?????? ??????.\n\n"
        "Lead / ??????: Test Lead\n"
        f"Phone / ?????: {phone or 'not provided'}\n"
        "Last message / آخر رسالة:\n"
        '"This is a test notification from the Fiper Analytics Dashboard."\n\n'
        "Please contact the lead and update the conversation now.\n"
        "???? ??????? ?? ?????? ?????? ???????? ????."
    )
    email_sent = _send_email(to, "Fiper test alert notification", html_body)
    whatsapp_sent = _send_whatsapp(phone, wa_body) if phone else False
    return email_sent or whatsapp_sent


def notify_agent_alert(alert: dict) -> bool:
    agent = _display_agent_name(alert.get("agent_name")) or "unknown"
    contact = resolve_agent_contact(agent)
    recipient = (contact.get("email") or "").strip()
    whatsapp_phone = (contact.get("phone") or "").strip()
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
                    contact = resolve_agent_contact(resolved)
                    recipient = (contact.get("email") or "").strip()
                    whatsapp_phone = (contact.get("phone") or "").strip()
            except Exception:
                pass
    if not recipient and not whatsapp_phone:
        return False

    severity = alert.get("severity") or "Alert"
    lead = None
    alert_type = (alert.get("type") or "alert").replace("_", " ").title()
    message = alert.get("message") or ""
    lead_id = alert.get("lead_id") or ""
    lead_label = str(lead_id)
    if lead_id and lead_id != "test":
        try:
            lead_rows = _paginate(
                lambda: supabase.table("leads")
                .select("id,phone,name")
                .eq("id", lead_id)
                .limit(1)
            )
            if lead_rows:
                lead = lead_rows[0]
                bits = [b for b in [lead.get("phone"), lead.get("name")] if b]
                if bits:
                    lead_label = " | ".join(bits)
        except Exception:
            pass
    html_body = f"""
    <h2>New Fiper Alert</h2>
    <p><b>Agent:</b> {html.escape(agent)}</p>
    <p><b>Severity:</b> {html.escape(severity)}</p>
    <p><b>Type:</b> {html.escape(alert_type)}</p>
    <p><b>Lead:</b> {html.escape(lead_label)}</p>
    <p><b>Message:</b> {html.escape(message)}</p>
    <p><b>Lead ID:</b> {html.escape(str(lead_id))}</p>
    """
    latest_customer_message = (alert.get("last_customer_message") or "").strip()
    if not latest_customer_message and lead_id and lead_id != "test":
        try:
            msg_rows = _paginate(
                lambda: supabase.table("messages")
                .select("direction,body,sent_at")
                .eq("lead_id", lead_id)
                .eq("direction", "inbound")
                .order("sent_at", desc=True)
                .limit(1)
            )
            if msg_rows:
                latest_customer_message = (msg_rows[0].get("body") or "").strip()
        except Exception:
            pass
    latest_customer_message = latest_customer_message or "No customer message saved."
    wa_body = (
        f"Hello {agent} / مرحباً {agent}،\n"
        f"You have a {severity} {alert_type} alert that needs action.\n"
        f"لديك تنبيه {_severity_ar(severity)} من نوع {_alert_type_ar(alert.get('type') or alert_type)} ويحتاج متابعة.\n\n"
        f"Lead / العميل: {lead_label}\n"
        f"Phone / الرقم: {_lead_phone(lead) or ''}\n"
        "Last message / آخر رسالة:\n"
        f'"{latest_customer_message}"\n\n'
        "Please contact the lead and update the conversation now.\n"
        "يرجى التواصل مع العميل وتحديث المحادثة الآن."
    )
    email_sent = _send_email(recipient, f"Fiper Alert - {severity} - {alert_type}", html_body) if recipient else False
    whatsapp_sent = _send_whatsapp(whatsapp_phone, wa_body) if whatsapp_phone else False
    return email_sent or whatsapp_sent


def send_webhook_health_alert(details: dict) -> bool:
    recipients = _split_recipients(SALES_SUPERVISOR_EMAIL)
    if not recipients:
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
        f" | <b>Latest ManyContacts activity:</b> {html.escape(str(item.get('latest_activity') or 'unknown'))}"
        f" | <b>Latest stored message:</b> {html.escape(str(item.get('last_seen') or 'none'))}"
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
        recipients,
        "Fiper Alert - WhatsApp webhook may be disabled",
        html_body,
    )


def send_supervisor_report(report_label: str = "") -> bool:
    recipients = _supervisor_recipients()
    if not recipients:
        return False

    report_tz = ZoneInfo(REPORT_TIMEZONE)
    now = datetime.now(report_tz)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()

    alerts = (
        supabase.table("alerts")
        .select("severity,type,agent_name,message,created_at,resolved,lead_id")
        .gte("created_at", today)
        .eq("resolved", False)
        .order("created_at", desc=True)
        .execute()
        .data
    ) or []
    open_alerts = list(alerts)

    alert_lead_ids = list({a.get("lead_id") for a in open_alerts if a.get("lead_id")})
    lead_map: dict[str, dict] = {}
    latest_msg_map: dict[str, dict] = {}
    latest_msg_by_phone: dict[str, dict] = {}
    alert_phone_values: set[str] = set()
    latest_call_map: dict[str, dict] = {}
    if alert_lead_ids:
        try:
            lead_rows = _paginate(
                lambda: supabase.table("leads")
                .select("id,phone,wa_contact_id,name,assigned_agent")
                .in_("id", alert_lead_ids)
            )
            lead_map = {r["id"]: r for r in lead_rows if r.get("id")}
            alert_phone_values = {
                phone
                for r in lead_rows
                for phone in [r.get("phone"), r.get("wa_contact_id")]
                if phone
            }
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
            if alert_phone_values:
                duplicate_leads = _paginate(
                    lambda: supabase.table("leads")
                    .select("id,phone,wa_contact_id")
                    .or_(",".join([
                        f"phone.eq.{phone},wa_contact_id.eq.{phone}"
                        for phone in alert_phone_values
                    ]))
                )
                duplicate_lead_ids = [r["id"] for r in duplicate_leads if r.get("id")]
                lead_phone_map = {}
                for r in duplicate_leads:
                    for phone in [r.get("phone"), r.get("wa_contact_id")]:
                        if phone:
                            lead_phone_map[r.get("id")] = phone
                            break
                if duplicate_lead_ids:
                    duplicate_msgs = _paginate(
                        lambda: supabase.table("messages")
                        .select("lead_id,direction,body,agent_name,sent_at")
                        .in_("lead_id", duplicate_lead_ids)
                        .eq("direction", "inbound")
                        .order("sent_at", desc=True)
                    )
                    for msg in duplicate_msgs:
                        phone = lead_phone_map.get(msg.get("lead_id"))
                        body = (msg.get("body") or "").strip()
                        if phone and body and phone not in latest_msg_by_phone:
                            latest_msg_by_phone[phone] = msg
        except Exception:
            latest_msg_by_phone = {}

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
        if not (latest_msg.get("body") or "").strip() and lead:
            for phone in [lead.get("phone"), lead.get("wa_contact_id")]:
                if phone and latest_msg_by_phone.get(phone):
                    latest_msg = latest_msg_by_phone[phone]
                    break
        latest_call = latest_call_map.get(alert.get("lead_id") or "", {})
        if lead:
            alert["lead_phone"] = _lead_phone(lead)
            alert["lead_name"] = lead.get("name")
        alert["agent_name"] = _display_agent_name(
            alert.get("agent_name"),
            _lead_agent_name(lead, latest_msg) if lead else None,
            latest_call.get("agent_name") if latest_call else None,
        ) or alert.get("agent_name")
        alert["open_for"] = _fmt_relative_age(alert.get("created_at"), now)
        last_body = (latest_msg.get("body") or "").strip()
        alert["last_message_body"] = last_body[:180] if last_body else ""

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
        f"<li>{html.escape(_alert_type_title(k))}: {v}</li>"
        for k, v in type_counts.most_common(10)
    )
    severity_rank = {"HIGH": 0, "MED": 1, "LOW": 2}
    detail_rows = []
    for alert in sorted(open_alerts, key=lambda a: (severity_rank.get(a.get("severity") or "", 9), a.get("created_at") or "")):
        lead_bits = [b for b in [alert.get("lead_phone"), alert.get("lead_name")] if b]
        lead_label = " | ".join(lead_bits) or "unknown lead"
        agent_label = alert.get("agent_name") or "unknown"
        alert_message = _clean_alert_message(alert.get("message"))
        last_message = alert.get("last_message_body") or ""
        detail_rows.append(
            "<tr>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'><b>{html.escape(alert.get('severity') or 'MED')}</b></td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(_alert_type_title(alert.get('type')))}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(alert.get('open_for') or 'unknown')}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(lead_label)}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(agent_label)}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(alert_message)}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(last_message or '-')}</td>"
            "</tr>"
        )
    alert_details_html = (
        "<table style='border-collapse:collapse;width:100%;font-size:13px'>"
        "<thead><tr>"
        "<th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Severity</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Type</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Open for</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Lead</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Agent</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Alert</th>"
        "<th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Latest customer message</th>"
        "</tr></thead><tbody>"
        + "".join(detail_rows)
        + "</tbody></table>"
    ) if detail_rows else "<p>No alerts today.</p>"

    html_body = f"""
    <h2>Fiper Sales Report {html.escape(report_label)}</h2>
    <p><b>Report time ({REPORT_TIMEZONE_LABEL}):</b> {now.strftime('%Y-%m-%d %H:%M')}</p>
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
    <h3>Alert details today</h3>
    {alert_details_html}
    """
    return _send_email(
        recipients,
        f"Fiper Sales Report {report_label}".strip(),
        html_body,
    )



def send_weekly_intelligence_report(report_label: str = "Weekly Intelligence Report") -> bool:
    """Send a weekly management report without changing the daily report path."""
    recipients = _supervisor_recipients()
    if not recipients:
        return False

    report_tz = ZoneInfo(REPORT_TIMEZONE)
    now_local = datetime.now(report_tz)
    end_utc = now_local.astimezone(timezone.utc)
    start_local = (now_local - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_local.astimezone(timezone.utc)
    prev_start_utc = (start_local - timedelta(days=7)).astimezone(timezone.utc)

    since = start_utc.isoformat()
    previous_since = prev_start_utc.isoformat()
    previous_until = start_utc.isoformat()

    leads = _paginate(
        lambda: supabase.table("leads")
        .select("id,phone,wa_contact_id,name,channel,status,score,assigned_agent,created_at,last_message_at")
        .gte("created_at", since)
    )
    prev_leads = _paginate(
        lambda: supabase.table("leads")
        .select("id")
        .gte("created_at", previous_since)
        .lt("created_at", previous_until)
    )
    calls = _paginate(
        lambda: supabase.table("calls")
        .select("id,lead_id,agent_name,duration_seconds,outcome,called_at")
        .gte("called_at", since)
    )
    prev_calls = _paginate(
        lambda: supabase.table("calls")
        .select("id")
        .gte("called_at", previous_since)
        .lt("called_at", previous_until)
    )
    messages = _paginate(
        lambda: supabase.table("messages")
        .select("id,lead_id,direction,body,agent_name,sent_at")
        .gte("sent_at", since)
    )
    alerts = _paginate(
        lambda: supabase.table("alerts")
        .select("id,lead_id,agent_name,severity,type,message,resolved,created_at")
        .gte("created_at", since)
    )
    analyses = _paginate(
        lambda: supabase.table("ai_analysis")
        .select("lead_id,sentiment,topics,risk_flags,treatment_score,outcome,summary,source,analyzed_at")
        .gte("analyzed_at", since)
    )

    lead_ids = list({row.get("id") for row in leads if row.get("id")})
    analysis_lead_ids = list({row.get("lead_id") for row in analyses if row.get("lead_id")})
    alert_lead_ids = list({row.get("lead_id") for row in alerts if row.get("lead_id")})
    all_lead_ids = list({*lead_ids, *analysis_lead_ids, *alert_lead_ids})

    lead_map: dict[str, dict] = {row["id"]: row for row in leads if row.get("id")}
    if all_lead_ids:
        try:
            extra_leads = _paginate(
                lambda: supabase.table("leads")
                .select("id,phone,wa_contact_id,name,channel,status,score,assigned_agent,created_at,last_message_at")
                .in_("id", all_lead_ids)
            )
            for row in extra_leads:
                if row.get("id"):
                    lead_map[row["id"]] = row
        except Exception:
            pass

    latest_message_map: dict[str, dict] = {}
    if all_lead_ids:
        try:
            msg_rows = _paginate(
                lambda: supabase.table("messages")
                .select("lead_id,direction,body,agent_name,sent_at")
                .in_("lead_id", all_lead_ids)
                .order("sent_at", desc=True)
            )
            for msg in msg_rows:
                lead_id = msg.get("lead_id")
                if lead_id and lead_id not in latest_message_map:
                    latest_message_map[lead_id] = msg
        except Exception:
            pass

    completed = sum(1 for c in calls if (c.get("outcome") or "").lower() == "completed")
    no_answer = sum(1 for c in calls if (c.get("outcome") or "").lower() == "no_answer")
    busy = sum(1 for c in calls if (c.get("outcome") or "").lower() == "busy")
    pickup_base = completed + no_answer + busy
    pickup_rate = round(completed / pickup_base * 100, 1) if pickup_base else 0
    avg_duration = round(sum(c.get("duration_seconds") or 0 for c in calls) / len(calls), 1) if calls else 0
    inbound_count = sum(1 for m in messages if m.get("direction") == "inbound")
    outbound_count = sum(1 for m in messages if m.get("direction") == "outbound")

    open_alerts = [a for a in alerts if not a.get("resolved")]
    severity_counts = Counter(a.get("severity") or "UNKNOWN" for a in open_alerts)
    type_counts = Counter(a.get("type") or "unknown" for a in open_alerts)

    agent_stats: dict[str, dict] = defaultdict(lambda: {
        "alerts": 0, "open": 0, "resolved": 0, "high": 0, "no_reply": 0,
        "calls": 0, "wa_msgs": 0, "treatment_scores": [], "risk_leads": set(),
    })
    for call in calls:
        agent = _display_agent_name(call.get("agent_name")) or "unknown"
        agent_stats[agent]["calls"] += 1
    for msg in messages:
        agent = _display_agent_name(msg.get("agent_name"))
        if agent:
            agent_stats[agent]["wa_msgs"] += 1
    for alert in alerts:
        lead = lead_map.get(alert.get("lead_id") or "", {})
        latest_msg = latest_message_map.get(alert.get("lead_id") or "", {})
        agent = _display_agent_name(alert.get("agent_name"), _lead_agent_name(lead, latest_msg)) or "unknown"
        stats = agent_stats[agent]
        stats["alerts"] += 1
        if alert.get("resolved"):
            stats["resolved"] += 1
        else:
            stats["open"] += 1
        if alert.get("severity") == "HIGH":
            stats["high"] += 1
        if alert.get("type") == "no_reply":
            stats["no_reply"] += 1
        if alert.get("lead_id"):
            stats["risk_leads"].add(alert.get("lead_id"))
    for analysis in analyses:
        lead = lead_map.get(analysis.get("lead_id") or "", {})
        agent = _display_agent_name(_lead_agent_name(lead, latest_message_map.get(analysis.get("lead_id") or "", {})))
        if agent and analysis.get("treatment_score") is not None and not (analysis.get("source") == "maqsam" and analysis.get("treatment_score") == 0):
            agent_stats[agent]["treatment_scores"].append(analysis.get("treatment_score"))

    agent_rows = []
    for agent, stats in sorted(agent_stats.items(), key=lambda kv: (kv[1]["open"], kv[1]["alerts"]), reverse=True):
        total = stats["alerts"]
        resolved = stats["resolved"]
        treatment_scores = stats["treatment_scores"]
        avg_treatment = round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else "-"
        agent_rows.append(
            "<tr>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{html.escape(agent)}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{total}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{resolved}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{stats['open']}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{_pct(resolved, total)}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{stats['high']}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{stats['no_reply']}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{len(stats['risk_leads'])}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{stats['calls']}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{stats['wa_msgs']}</td>"
            f"<td style='padding:8px;border:1px solid #e6eaf2'>{avg_treatment}</td>"
            "</tr>"
        )

    excluded_topics = {"greetings", "no_answer"}
    topic_counter = Counter(
        t for a in analyses for t in (a.get("topics") or []) if t and t not in excluded_topics
    )
    risk_counter = Counter(f for a in analyses for f in (a.get("risk_flags") or []) if f)
    total_topics = sum(topic_counter.values())
    topic_examples: dict[str, list[dict]] = defaultdict(list)
    for analysis in sorted(analyses, key=lambda a: a.get("analyzed_at") or "", reverse=True):
        summary = _short_text(analysis.get("summary"), 260)
        if not summary:
            continue
        lead = lead_map.get(analysis.get("lead_id") or "", {})
        latest_msg = latest_message_map.get(analysis.get("lead_id") or "", {})
        agent = _display_agent_name(_lead_agent_name(lead, latest_msg)) or "unknown"
        lead_label = " | ".join([x for x in [_lead_phone(lead), lead.get("name")] if x]) or "unknown lead"
        for topic in [t for t in (analysis.get("topics") or []) if t and t not in excluded_topics]:
            bucket = topic_examples[topic]
            if len(bucket) >= 2:
                continue
            if any(e.get("lead") == lead_label and e.get("text") == summary for e in bucket):
                continue
            bucket.append({"lead": lead_label, "agent": agent, "text": summary})
            break

    topic_blocks = []
    for topic, count in topic_counter.most_common(10):
        examples = "".join(
            "<li>"
            f"<b>Lead:</b> {html.escape(ex.get('lead') or 'unknown')} | "
            f"<b>Agent:</b> {html.escape(ex.get('agent') or 'unknown')}<br>"
            f"{html.escape(ex.get('text') or '')}"
            "</li>"
            for ex in topic_examples.get(topic, [])
        )
        topic_blocks.append(
            f"<h4>{html.escape(_topic_title(topic))} — {count} ({_pct(count, total_topics)})</h4>"
            f"<ul>{examples or '<li>No examples saved yet.</li>'}</ul>"
        )

    sentiment_values = []
    treatment_scores = []
    for analysis in analyses:
        sentiment = (analysis.get("sentiment") or "").lower()
        if sentiment in ("positive", "neutral", "negative"):
            sentiment_values.append(sentiment)
        score = analysis.get("treatment_score")
        if score is not None and not (analysis.get("source") == "maqsam" and score == 0):
            treatment_scores.append(score)
    avg_treatment = round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0

    recommendations = []
    if type_counts.get("no_reply", 0) >= 5:
        recommendations.append("No Reply is high this week. Review follow-up discipline and make supervisors check unresolved WhatsApp conversations daily.")
    if risk_counter.get("beginner_risk", 0) >= 3:
        recommendations.append("Beginner-risk conversations are frequent. Prepare a short education script for new traders before discussing deposits.")
    if risk_counter.get("profit_expectations", 0) >= 2:
        recommendations.append("Profit-expectation risk appeared repeatedly. Reinforce risk disclaimer and realistic return language in agent scripts.")
    if topic_counter.get("account_info", 0) >= 5:
        recommendations.append("Account and setup questions are common. Add a standard FAQ reply for account opening, verification, deposits, and withdrawals.")
    if avg_treatment and avg_treatment < 60:
        recommendations.append("Average treatment score is low. Review weak conversations and coach agents on clarity, empathy, and next-step ownership.")
    if not recommendations:
        recommendations.append("Keep monitoring alert response discipline and review the top topics with the sales team in the weekly meeting.")

    def _delta(current: int, previous: int) -> str:
        diff = current - previous
        sign = "+" if diff >= 0 else ""
        return f"{sign}{diff} vs previous week"

    html_body = f"""
    <h2>Fiper Weekly Intelligence Report</h2>
    <p><b>Report time ({REPORT_TIMEZONE_LABEL}):</b> {now_local.strftime('%Y-%m-%d %H:%M')}</p>
    <p><b>Period:</b> {start_local.strftime('%Y-%m-%d')} to {now_local.strftime('%Y-%m-%d')}</p>

    <h3>Executive summary</h3>
    <ul>
      <li>New leads: {len(leads)} ({html.escape(_delta(len(leads), len(prev_leads)))})</li>
      <li>Total calls: {len(calls)} ({html.escape(_delta(len(calls), len(prev_calls)))})</li>
      <li>Pickup rate: {pickup_rate}%</li>
      <li>Avg call duration: {avg_duration}s</li>
      <li>WhatsApp messages: {len(messages)} ({inbound_count} inbound / {outbound_count} outbound)</li>
      <li>Open alerts created this week: {len(open_alerts)} | High: {severity_counts.get('HIGH', 0)} | Medium: {severity_counts.get('MED', 0)} | Low: {severity_counts.get('LOW', 0)}</li>
      <li>Avg treatment score: {avg_treatment}/100</li>
      <li>Sentiment: positive {sentiment_values.count('positive')}, neutral {sentiment_values.count('neutral')}, negative {sentiment_values.count('negative')}</li>
    </ul>

    <h3>Agent alert accountability</h3>
    <table style='border-collapse:collapse;width:100%;font-size:13px'>
      <thead><tr>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Agent</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Alerts</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Resolved</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Open</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Resolved %</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>High</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>No Reply</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Risk leads</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Calls</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>WA msgs</th>
        <th style='text-align:left;padding:8px;border:1px solid #e6eaf2'>Treatment</th>
      </tr></thead>
      <tbody>{''.join(agent_rows) or '<tr><td colspan="11" style="padding:8px;border:1px solid #e6eaf2">No agent alert data this week.</td></tr>'}</tbody>
    </table>

    <h3>Top business topics</h3>
    {''.join(topic_blocks) or '<p>No analyzed topic data this week.</p>'}

    <h3>Risk flags</h3>
    <ul>{''.join(f'<li>{html.escape(_alert_type_title(k))}: {v}</li>' for k, v in risk_counter.most_common(10)) or '<li>No AI risk flags this week.</li>'}</ul>

    <h3>Open alert types</h3>
    <ul>{''.join(f'<li>{html.escape(_alert_type_title(k))}: {v}</li>' for k, v in type_counts.most_common(10)) or '<li>No open alerts created this week.</li>'}</ul>

    <h3>Recommended management actions</h3>
    <ol>{''.join(f'<li>{html.escape(item)}</li>' for item in recommendations)}</ol>
    """

    return _send_email(
        recipients,
        f"Fiper Weekly Intelligence Report {report_label}".strip(),
        html_body,
    )

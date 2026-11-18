from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import csv
import html
import io
import os
import re
import time

import requests
from dotenv import load_dotenv
from supabase import create_client
from pipeline import agent_names
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
REPORT_TIMEZONE = os.getenv("REPORT_TIMEZONE", "Etc/GMT-2")
REPORT_TIMEZONE_LABEL = os.getenv("REPORT_TIMEZONE_LABEL", "UTC+2")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "")
META_WHATSAPP_PHONE_ID = os.getenv("META_WHATSAPP_PHONE_ID", "")
META_WHATSAPP_TOKEN = os.getenv("META_WHATSAPP_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
CALLBELL_API_KEY = os.getenv("CALLBELL_API_KEY", "")
CALLBELL_CHANNEL_UUID = os.getenv("CALLBELL_CHANNEL_UUID", "")
_contacts_cache: dict = {"loaded_at": 0.0, "contacts": {}}

AGENT_CONTACT_ALIASES = agent_names.contact_aliases()


def _agent_phone_env_key(agent_name: str | None) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", _normalize_name(agent_name).upper()).strip("_")
    return f"AGENT_WHATSAPP_{slug}" if slug else ""


def _agent_telegram_env_key(agent_name: str | None) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", _normalize_name(agent_name).upper()).strip("_")
    return f"AGENT_TELEGRAM_{slug}" if slug else ""


def _telegram_chat_id_from_db(username: str) -> str:
    if not username:
        return ""
    try:
        rows = (
            supabase.table("telegram_chat_ids")
            .select("chat_id")
            .eq("username", username.lower().lstrip("@"))
            .limit(1)
            .execute()
            .data or []
        )
        return (rows[0].get("chat_id") or "") if rows else ""
    except Exception:
        return ""


def _agent_telegram_id(agent_name: str | None) -> str:
    # 1. Explicit env var override
    env_id = os.getenv(_agent_telegram_env_key(agent_name), "").strip()
    if env_id:
        return env_id
    # 2. Google Sheet lookup
    contact = _agent_contact(agent_name)
    if not contact:
        return ""
    # 2a. Direct chat_id column in Sheet — most reliable, no DB round-trip
    direct_id = contact.get("telegram_id", "").strip()
    if direct_id:
        return direct_id
    # 2b. Fallback: @username → telegram_chat_ids table
    username = contact.get("telegram", "").strip()
    if not username:
        return ""
    return _telegram_chat_id_from_db(username)


def _agent_env_key(agent_name: str | None) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", _normalize_name(agent_name).upper()).strip("_")
    return f"AGENT_EMAIL_{slug}" if slug else ""


def _normalize_name(name: str | None) -> str:
    return agent_names.normalize_name(name)


def _name_keys(name: str | None) -> set[str]:
    normalized = _normalize_name(name)
    if not normalized:
        return set()
    words = normalized.split()
    keys = set()
    for variant in [normalized, " ".join(reversed(words))]:
        compact = variant.replace(" ", "")
        keys.update({variant, compact, re.sub(r"[aeiou]", "", compact)})
    return keys


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


def _clean_report_text(value: str | None) -> str:
    text = str(value or "")
    if not text.strip():
        return ""
    # Repair common mojibake where UTF-8 Arabic was decoded as latin-1/cp1252.
    # Do this before strip(): mojibake bytes can appear as whitespace controls.
    if any(marker in text for marker in ("\u00d8", "\u00d9", "\u00f0\u0178", "\u00e2\u20ac")):
        for source_encoding in ("latin1", "cp1252"):
            try:
                repaired = text.encode(source_encoding).decode("utf-8")
            except Exception:
                continue
            if repaired and repaired != text:
                text = repaired
                break
    return text.strip()


def _agent_report_label(value: str | None) -> str:
    normalized = _display_agent_name(value)
    if not normalized:
        return "unknown"
    contact = _agent_contact(normalized)
    return _clean_report_text((contact or {}).get("name") or normalized)


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
            tg_raw = ""
            tg_direct_id = ""
            for _k, _v in row.items():
                k_lower = _k.lower()
                if "telegram" not in k_lower:
                    continue
                v = (_v or "").strip()
                if "id" in k_lower or "chat" in k_lower:
                    tg_direct_id = v
                elif not tg_raw:
                    tg_raw = v
            # Accept @username or full t.me/username links
            if tg_raw.startswith("http"):
                tg_raw = tg_raw.rstrip("/").split("/")[-1]
            telegram = tg_raw.lstrip("@").lower() if tg_raw else ""
            telegram_id = re.sub(r"[^\d]", "", tg_direct_id) if tg_direct_id else ""
            if not (name and email):
                continue
            contact = {"name": name, "email": email, "phone": phone, "telegram": telegram, "telegram_id": telegram_id}
            for key in _name_keys(name):
                contacts[key] = contact
    except Exception:
        contacts = _contacts_cache.get("contacts") or {}

    _contacts_cache.update({"loaded_at": now, "contacts": contacts})
    return contacts


def _agent_contact(agent_name: str | None) -> dict | None:
    contacts = _load_agent_contacts()
    keys = _name_keys(agent_name)
    alias = AGENT_CONTACT_ALIASES.get(_normalize_name(agent_name))
    if alias:
        keys.update(_name_keys(alias))
    for key in keys:
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



def _fmt_report_message_time(value: str | None, report_tz: ZoneInfo) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.astimezone(report_tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "-"
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



def _is_banal_report_message(body: str | None) -> bool:
    text = (body or "").strip()
    if not text:
        return True
    normalized = " ".join(text.lower().replace("\u0640", "").split())
    normalized = normalized.strip(" .,!\u061f?\u061b;:()[]{}\"'`~|/\\")
    if normalized in {"[reaction]", "[sticker]", "[image]", "[video]", "reaction", "sticker", "image", "video"}:
        return True
    if re.fullmatch(r"[\d\u0660-\u0669]+", normalized):
        return True
    if len(normalized) <= 4 and not any(ch.isalnum() for ch in normalized):
        return True
    compact = re.sub(r"[\W_]+", " ", normalized, flags=re.UNICODE).strip()
    banal = {
        "ok", "okay", "yes", "no", "done", "thanks", "thank you",
        "\u062a\u0645", "\u062a\u0645\u0627\u0645", "\u0646\u0639\u0645", "\u0627\u064a", "\u0625\u064a",
        "\u0627\u064a\u0648\u0647", "\u0623\u064a\u0648\u0647", "\u0627\u0648\u0643\u064a", "\u0623\u0648\u0643\u064a",
        "\u0634\u0643\u0631\u0627", "\u0634\u0643\u0631\u0627\u064b", "\u0645\u0634\u0643\u0648\u0631",
        "\u0645\u0634\u0643\u0648\u0631\u0647", "\u062a\u0633\u0644\u0645", "\u062d\u0633\u0646\u0627",
    }
    return len(compact) <= 24 and any(compact == item or compact.startswith(item + " ") for item in banal)


def _latest_unanswered_inbound(rows: list[dict], now: datetime) -> dict | None:
    last_inbound = None
    for msg in sorted(rows, key=lambda row: row.get("sent_at") or ""):
        direction = (msg.get("direction") or "").lower()
        if direction == "inbound":
            if _is_banal_report_message(msg.get("body")):
                continue
            last_inbound = msg
        elif direction == "outbound" and last_inbound:
            last_inbound = None
    if not last_inbound:
        return None
    try:
        sent = datetime.fromisoformat(str(last_inbound.get("sent_at")).replace("Z", "+00:00"))
    except Exception:
        return None
    if (now.astimezone(timezone.utc) - sent).total_seconds() < 60 * 60:
        return None
    return last_inbound


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


def _send_whatsapp_meta(phone: str | None, body: str) -> bool:
    digits = re.sub(r"\D+", "", phone or "")
    if not (META_WHATSAPP_PHONE_ID and META_WHATSAPP_TOKEN and digits and body.strip()):
        return False
    response = requests.post(
        f"https://graph.facebook.com/v17.0/{META_WHATSAPP_PHONE_ID}/messages",
        headers={
            "Authorization": f"Bearer {META_WHATSAPP_TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "messaging_product": "whatsapp",
            "to": digits,
            "type": "text",
            "text": {"body": body.strip()},
        },
        timeout=20,
    )
    response.raise_for_status()
    return True


def _send_whatsapp_callbell(phone: str | None, body: str) -> bool:
    digits = re.sub(r"\D+", "", phone or "")
    if not (CALLBELL_API_KEY and digits and body.strip()):
        return False
    payload: dict = {
        "to": f"+{digits}",
        "from": "whatsapp",
        "type": "text",
        "content": {"text": body.strip()},
    }
    if CALLBELL_CHANNEL_UUID:
        payload["channel_uuid"] = CALLBELL_CHANNEL_UUID
    response = requests.post(
        "https://api.callbell.eu/v1/messages/send",
        headers={
            "Authorization": f"Bearer {CALLBELL_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    return True


def _send_telegram(chat_id: str | None, body: str) -> bool:
    if not (TELEGRAM_BOT_TOKEN and chat_id and body.strip()):
        return False
    response = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": body.strip(), "parse_mode": "Markdown"},
        timeout=20,
    )
    response.raise_for_status()
    return True


def _log_notification_delivery(
    *,
    alert_id: str | None,
    lead_id: str | None,
    agent_name: str | None,
    channel: str,
    recipient: str | None,
    status: str,
    subject: str | None = None,
    message_preview: str | None = None,
    provider_response: dict | None = None,
    error_message: str | None = None,
) -> None:
    try:
        row = {
            "alert_id": alert_id or None,
            "lead_id": lead_id or None,
            "agent_name": agent_name or None,
            "channel": channel,
            "recipient": recipient or None,
            "status": status,
            "subject": subject or None,
            "message_preview": (message_preview or "")[:500] or None,
            "provider_response": provider_response,
            "error_message": (error_message or "")[:1000] or None,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        supabase.table("notification_deliveries").insert(row).execute()
    except Exception:
        # Delivery logging must never block the actual alert notification path.
        pass


def _send_telegram_alert(
    chat_id: str | None,
    body: str,
    *,
    alert: dict,
    agent_name: str,
    subject: str,
) -> bool:
    if not (TELEGRAM_BOT_TOKEN and chat_id and body.strip()):
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": body.strip(), "parse_mode": "Markdown"},
            timeout=20,
        )
        response.raise_for_status()
        try:
            provider_response = response.json()
        except Exception:
            provider_response = {"status_code": response.status_code, "text": response.text[:500]}
        _log_notification_delivery(
            alert_id=alert.get("id"),
            lead_id=alert.get("lead_id"),
            agent_name=agent_name,
            channel="telegram",
            recipient=chat_id,
            status="sent",
            subject=subject,
            message_preview=body,
            provider_response=provider_response,
        )
        return True
    except Exception as exc:
        _log_notification_delivery(
            alert_id=alert.get("id"),
            lead_id=alert.get("lead_id"),
            agent_name=agent_name,
            channel="telegram",
            recipient=chat_id,
            status="failed",
            subject=subject,
            message_preview=body,
            error_message=str(exc),
        )
        return False


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
    _now = datetime.now(ZoneInfo(REPORT_TIMEZONE))
    if not (0 <= _now.weekday() <= 4 and 9 <= _now.hour < 19):
        return False

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
    latest_customer_message = (alert.get("last_customer_message") or "").strip()
    latest_customer_time = alert.get("last_customer_message_at") or ""
    if lead_id and lead_id != "test":
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
                latest_customer_message = latest_customer_message or (msg_rows[0].get("body") or "").strip()
                latest_customer_time = latest_customer_time or (msg_rows[0].get("sent_at") or "")
        except Exception:
            pass
    latest_customer_message = latest_customer_message or "No customer message saved."
    report_tz = ZoneInfo(REPORT_TIMEZONE)
    message_time_label = _fmt_report_message_time(latest_customer_time, report_tz)
    alert_time_label = _fmt_report_message_time(alert.get("created_at"), report_tz)
    html_body = f"""
    <h2>New Fiper Alert</h2>
    <p><b>Agent:</b> {html.escape(agent)}</p>
    <p><b>Severity:</b> {html.escape(severity)}</p>
    <p><b>Type:</b> {html.escape(alert_type)}</p>
    <p><b>Lead:</b> {html.escape(lead_label)}</p>
    <p><b>Message time ({html.escape(REPORT_TIMEZONE_LABEL)}):</b> {html.escape(message_time_label)}</p>
    <p><b>Alert time ({html.escape(REPORT_TIMEZONE_LABEL)}):</b> {html.escape(alert_time_label)}</p>
    <p><b>Alert:</b> {html.escape(_clean_alert_message(message) or message)}</p>
    <p><b>Latest customer message:</b></p>
    <blockquote style="margin:0;padding:10px 12px;background:#f3f7ff;border-left:4px solid #0b7cff;white-space:pre-wrap">{html.escape(latest_customer_message)}</blockquote>
    <p><b>Lead ID:</b> {html.escape(str(lead_id))}</p>
    """
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
    telegram_id = _agent_telegram_id(agent)
    tg_body = (
        f"🚨 *Fiper Alert — {severity}*\n\n"
        f"*Agent:* {agent}\n"
        f"*Type:* {alert_type}\n"
        f"*Lead:* {lead_label}\n"
        f"*Alert:* {_clean_alert_message(message) or message}\n\n"
        f"*Last message ({REPORT_TIMEZONE_LABEL}):* {message_time_label}\n"
        f'"{latest_customer_message[:200]}"\n\n'
        "Please contact the lead and update the conversation now."
    )
    subject = f"Fiper Alert - {severity} - {alert_type}"
    email_sent = _send_email(recipient, subject, html_body) if recipient else False
    whatsapp_sent = _send_whatsapp_callbell(whatsapp_phone, wa_body) if whatsapp_phone else False
    if telegram_id:
        telegram_sent = _send_telegram_alert(
            telegram_id, tg_body, alert=alert, agent_name=agent, subject=subject
        )
    else:
        _log_notification_delivery(
            alert_id=alert.get("id"),
            lead_id=alert.get("lead_id"),
            agent_name=agent,
            channel="telegram",
            recipient=None,
            status="skipped",
            subject=subject,
            message_preview=tg_body,
            provider_response={"reason": "no_telegram_recipient"},
            error_message="No Telegram chat ID resolved for agent",
        )
        telegram_sent = False
    return email_sent or whatsapp_sent or telegram_sent


def send_overnight_alert_catchup() -> int:
    """Send Telegram for any open alerts that fired outside working hours.

    Runs at 09:05 UTC+2 Mon-Fri. Finds unresolved alerts from the last 20 hours
    where no 'sent' notification exists, then delivers them now via Telegram only.
    Returns number of notifications sent.
    """
    from datetime import timezone as _tz
    # The reply-reminder engine handles overnight customers itself when live.
    if os.getenv("REMINDER_MODE", "shadow").strip().lower() == "live":
        return 0
    cutoff = (datetime.now(_tz.utc) - timedelta(hours=20)).isoformat()
    try:
        alerts = (
            supabase.table("alerts")
            .select("id,lead_id,agent_name,severity,type,message,created_at")
            .eq("resolved", False)
            .eq("type", "no_reply")  # coaching alerts never ping agents
            .gte("created_at", cutoff)
            .execute()
            .data or []
        )
    except Exception:
        return 0

    sent_count = 0
    for alert in alerts:
        alert_id = alert.get("id")
        if not alert_id:
            continue
        # Skip if a 'sent' notification already exists for this alert
        try:
            existing = (
                supabase.table("notification_deliveries")
                .select("id")
                .eq("alert_id", alert_id)
                .eq("status", "sent")
                .limit(1)
                .execute()
                .data or []
            )
        except Exception:
            continue
        if existing:
            continue

        agent = _display_agent_name(alert.get("agent_name")) or "unknown"
        telegram_id = _agent_telegram_id(agent)
        if not telegram_id:
            continue

        severity = alert.get("severity", "HIGH")
        alert_type = (alert.get("type") or "alert").replace("_", " ").title()
        message = alert.get("message", "")
        created_at = alert.get("created_at", "")
        try:
            from zoneinfo import ZoneInfo as _ZI
            alert_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            alert_local = alert_dt.astimezone(_ZI(REPORT_TIMEZONE))
            alert_time_label = alert_local.strftime("%Y-%m-%d %H:%M")
        except Exception:
            alert_time_label = created_at[:16]

        tg_body = (
            f"🔔 *Overnight Alert — {severity}*\n\n"
            f"*Agent:* {agent}\n"
            f"*Type:* {alert_type}\n"
            f"*Alert:* {_clean_alert_message(message) or message}\n\n"
            f"*Fired at ({REPORT_TIMEZONE_LABEL}):* {alert_time_label}\n"
            "_This alert fired outside working hours and is delivered now._"
        )
        subject = f"Fiper Alert - {severity} - {alert_type} (overnight)"
        ok = _send_telegram_alert(
            telegram_id, tg_body,
            alert=alert, agent_name=agent, subject=subject,
        )
        if ok:
            sent_count += 1

    return sent_count


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
      <b>Urgent:</b> one or more WhatsApp webhook lines appear to be delayed,
      disabled, or missing heartbeat updates. ManyContacts can automatically
      disable forwarding after repeated failed webhook requests.
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
      Please check ManyContacts -> API / Developers now and make sure
      <b>Enable WhatsApp API webhook forwarding</b> is ON and saved.
    </p>
    """
    _LINE_LABELS = {"96897245526": "Oman 🇴🇲", "905318880855": "Turkey 🇹🇷"}
    tg_lines = "\n".join(
        f"• Line *{_LINE_LABELS.get(str(item.get('number') or ''), str(item.get('number') or 'unknown'))}*"
        f" — silent {item.get('lag_min', '?')} min"
        for item in stale_lines
    )
    tg_text = (
        "⚠️ *WhatsApp Webhook Stalled*\n\n"
        f"{tg_lines}\n\n"
        "👉 ManyContacts → API / Developers → re\\-enable webhook"
    )
    return _send_telegram(TELEGRAM_ADMIN_CHAT_ID, tg_text)


def send_supervisor_report(report_label: str = "") -> bool:
    """Daily sales report (13:00 / 19:00). Built in pipeline/reports.py."""
    from pipeline import reports  # lazy: reports imports this module
    return reports.send_daily_report(report_label)


def send_weekly_intelligence_report(report_label: str = "Weekly Intelligence Report") -> bool:
    """Weekly management report (Monday). Built in pipeline/reports.py."""
    from pipeline import reports
    return reports.send_weekly_report(report_label)

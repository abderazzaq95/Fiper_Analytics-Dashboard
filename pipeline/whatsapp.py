"""
ManyContacts API client.
ManyContacts is Fiper's WhatsApp CRM inbox — it proxies the Meta WhatsApp
Business API. Auth header: apikey.

Fiper WhatsApp display number: +96897245526
Secondary Fiper WhatsApp number: +905318880855
ManyContacts account: fadad3c2-617e-43fb-85eb-a53e93f44fc2

Confirmed working endpoints:
  GET /v1/contacts                     — paginated contact list, supports date_from/date_to
  GET /v1/contact/{id}                 — single contact detail
  GET /v1/contact/{id}/messages        — full conversation history (inbound + outbound)
  GET /v1/users                        — agent list

Inbound messages also arrive in real-time via the Meta WhatsApp Cloud API webhook
format at POST /webhook/manycontacts.
"""

import os
import httpx
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

BASE_URL = os.getenv("MC_BASE_URL", "https://api.manycontacts.com/v1")
HEADERS = {"apikey": os.getenv("MC_API_KEY", "")}
_DEFAULT_BUSINESS_NUMBERS = ("96897245526", "905318880855")
_BUSINESS_NUMBERS_RAW = os.getenv("MC_BUSINESS_NUMBERS") or os.getenv("MC_BUSINESS_NUMBER") or ",".join(_DEFAULT_BUSINESS_NUMBERS)


def _normalize_phone(phone: str | None) -> str:
    return "".join(ch for ch in str(phone or "") if ch.isdigit())


BUSINESS_NUMBERS = {
    _normalize_phone(n)
    for n in _BUSINESS_NUMBERS_RAW.split(",")
    if _normalize_phone(n)
}

_WA_LINE_COLUMNS_AVAILABLE: bool | None = None


def is_internal_whatsapp_number(phone: str | None) -> bool:
    """Return True when a phone belongs to Fiper's own WhatsApp lines."""
    return _normalize_phone(phone) in BUSINESS_NUMBERS


def normalize_business_line(value: str | None) -> str | None:
    """Normalize a selected ManyContacts line to a digits-only phone number."""
    normalized = _normalize_phone(value)
    return normalized or None


def can_use_whatsapp_line_columns() -> bool:
    """Detect whether the live Supabase schema already has line columns."""
    global _WA_LINE_COLUMNS_AVAILABLE
    if _WA_LINE_COLUMNS_AVAILABLE is not None:
        return _WA_LINE_COLUMNS_AVAILABLE
    sb_url = os.getenv("SUPABASE_URL", "").rstrip("/")
    sb_key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not sb_url or not sb_key:
        _WA_LINE_COLUMNS_AVAILABLE = False
        return False
    headers = {
        "apikey": sb_key,
        "Authorization": f"Bearer {sb_key}",
        "Content-Type": "application/json",
    }
    try:
        resp = httpx.get(
            f"{sb_url}/rest/v1/leads",
            headers=headers,
            params={"select": "whatsapp_business_number", "limit": "1"},
            timeout=10,
        )
        _WA_LINE_COLUMNS_AVAILABLE = resp.status_code == 200
    except Exception:
        _WA_LINE_COLUMNS_AVAILABLE = False
    return _WA_LINE_COLUMNS_AVAILABLE


def add_whatsapp_line_select(base_fields: str) -> str:
    """Append the WhatsApp line column only when the schema supports it."""
    if not can_use_whatsapp_line_columns():
        return base_fields
    if "whatsapp_business_number" in base_fields:
        return base_fields
    return f"{base_fields},whatsapp_business_number"


def selected_business_lines(raw: str | None) -> set[str] | None:
    """Parse a wa_line query param into a normalized set.

    Returns None when the caller wants the dashboard-wide aggregate view.
    """
    if raw is None:
        return None
    value = str(raw).strip()
    if not value or value.lower() in {"all", "*", "any"}:
        return None
    lines = {
        normalize_business_line(part)
        for part in value.split(",")
        if normalize_business_line(part)
    }
    return lines or None


def row_whatsapp_line(row: dict | None) -> str | None:
    """Best-effort lookup for the WhatsApp business line stored on a row."""
    if not row:
        return None
    for key in ("whatsapp_business_number", "business_number", "line_number", "wa_line"):
        value = row.get(key)
        normalized = normalize_business_line(value)
        if normalized:
            return normalized
    wa_contact_id = str(row.get("wa_contact_id") or "").strip()
    if wa_contact_id.startswith("whatsapp_health_"):
        return normalize_business_line(wa_contact_id.replace("whatsapp_health_", "", 1))
    return None


def matches_business_line(row: dict | None, selected_line: str | None) -> bool:
    """Return True when a row matches the selected ManyContacts line.

    Maqsam rows are always included. WhatsApp rows are filtered only when a
    specific ManyContacts line is selected.
    """
    lines = selected_business_lines(selected_line)
    if not lines:
        return True
    if not row:
        return False
    if str(row.get("channel") or "").lower() == "maqsam":
        return True
    line = row_whatsapp_line(row)
    if not line:
        return False  # Unattributed rows excluded from per-line views; visible in "All"
    return bool(line in lines)

# Module-level agent cache: {user_id: name}
_agent_cache: dict[str, str] = {}


async def fetch_users() -> list[dict]:
    """Return all ManyContacts users (agents)."""
    global _agent_cache
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/users", headers=HEADERS, timeout=15)
        resp.raise_for_status()
        users = resp.json()
        _agent_cache = {u["id"]: u["name"] for u in users if isinstance(u, dict)}
        return users


def resolve_agent_name(user_id: str | None) -> str | None:
    """Map a ManyContacts user_id to a human name using the cached user list."""
    if not user_id:
        return None
    return _agent_cache.get(user_id, user_id)


async def fetch_contacts(date_from: str | None = None, date_to: str | None = None, api_key: str | None = None) -> list[dict]:
    """
    Fetch contacts updated within the given date range.
    date_from / date_to: 'YYYY-MM-DD' strings.
    ManyContacts returns 50 contacts per page. The unpaged request is the first
    slice; page=1 continues after it.
    """
    params: dict = {}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    cutoff = None
    if date_from:
        cutoff = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)

    def _updated_at(contact: dict) -> datetime | None:
        raw = contact.get("updatedAt") or contact.get("updated_at") or contact.get("updated")
        if not raw:
            return None
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None

    all_contacts: list[dict] = []
    seen_ids: set[str] = set()
    async with httpx.AsyncClient(timeout=60) as client:
        # The default request gives the latest page. page=1,2,... gives older
        # pages, so include both shapes and deduplicate by contact id.
        page: int | None = None
        while True:
            req_params = dict(params)
            if page is not None:
                req_params["page"] = page
            req_headers = {"apikey": api_key} if api_key else HEADERS
            resp = await client.get(
                f"{BASE_URL}/contacts",
                headers=req_headers,
                params=req_params,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or not data:
                break

            page_oldest = None
            for contact in data:
                if not isinstance(contact, dict):
                    continue
                updated = _updated_at(contact)
                if updated and (page_oldest is None or updated < page_oldest):
                    page_oldest = updated
                if cutoff and updated and updated < cutoff:
                    continue
                contact_id = str(contact.get("id") or contact.get("number") or "")
                if contact_id and contact_id not in seen_ids:
                    seen_ids.add(contact_id)
                    all_contacts.append(contact)

            if len(data) < 50:
                break
            if cutoff and page_oldest and page_oldest < cutoff:
                break
            page = 1 if page is None else page + 1

    return all_contacts


async def fetch_contact(contact_id: str) -> dict:
    """Fetch a single contact by ManyContacts ID."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{BASE_URL}/contact/{contact_id}",
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()


import logging as _logging
_log = _logging.getLogger("fiper.whatsapp")

async def fetch_contact_messages(contact_id: str, phone: str | None = None) -> list[dict]:
    """
    Fetch full conversation history for a contact.
    Tries multiple URL patterns and logs the actual HTTP responses.
    Returns a list of message dicts; empty list on total failure.
    """
    candidates = [
        f"{BASE_URL}/contact/{contact_id}/messages",
        f"{BASE_URL}/contacts/{contact_id}/messages",
    ]
    if phone:
        candidates += [
            f"{BASE_URL}/contact/{phone}/messages",
            f"{BASE_URL}/contacts/{phone}/messages",
        ]

    async with httpx.AsyncClient(timeout=30) as client:
        for url in candidates:
            try:
                resp = await client.get(url, headers=HEADERS)
                _log.debug(f"[mc] messages {url} → {resp.status_code} ({len(resp.content)}B)")
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and data:
                        return data
                    if isinstance(data, dict):
                        for key in ("messages", "data", "items", "conversations"):
                            if isinstance(data.get(key), list):
                                return data[key]
                elif resp.status_code not in (404, 405, 501):
                    _log.warning(f"[mc] messages {url} → unexpected {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                _log.debug(f"[mc] messages {url} → error: {e}")
    return []


async def probe_contact(contact_id: str) -> dict:
    """Probe a single contact: return detail + messages endpoint raw responses for debugging."""
    async with httpx.AsyncClient(timeout=30) as client:
        result: dict = {"contact_id": contact_id, "urls_tried": []}
        # Contact detail
        try:
            r = await client.get(f"{BASE_URL}/contact/{contact_id}", headers=HEADERS)
            result["contact_status"] = r.status_code
            result["contact_keys"] = list(r.json().keys()) if r.status_code == 200 else r.text[:300]
        except Exception as e:
            result["contact_error"] = str(e)

        # Try all message URL patterns
        for path in (
            f"/contact/{contact_id}/messages",
            f"/contacts/{contact_id}/messages",
            f"/contact/{contact_id}/conversation",
            f"/contact/{contact_id}/chat",
        ):
            url = BASE_URL + path
            try:
                r = await client.get(url, headers=HEADERS)
                entry = {"url": url, "status": r.status_code, "body": r.text[:400]}
                result["urls_tried"].append(entry)
            except Exception as e:
                result["urls_tried"].append({"url": url, "error": str(e)})
        return result

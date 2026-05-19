"""
ManyContacts API client.
ManyContacts is Fiper's WhatsApp CRM inbox — it proxies the Meta WhatsApp
Business API. Auth header: apikey.

Fiper WhatsApp display number: +96897245526
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


async def fetch_contacts(date_from: str | None = None, date_to: str | None = None) -> list[dict]:
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
            resp = await client.get(
                f"{BASE_URL}/contacts",
                headers=HEADERS,
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

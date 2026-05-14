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
    ManyContacts returns all matching contacts in a single JSON array.
    """
    params: dict = {}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(
            f"{BASE_URL}/contacts",
            headers=HEADERS,
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []


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


async def fetch_contact_messages(contact_id: str) -> list[dict]:
    """
    Fetch full conversation history for a contact (both inbound and outbound).
    Endpoint: GET /v1/contacts/{id}/messages  (note: 'contacts' plural)
    Returns a list of message dicts. Empty list on 404 or error.

    Expected response fields per message:
      id        — unique message ID
      type      — "INBOUND" | "OUTBOUND"
      text      — message body text (may also be under 'message' or 'body')
      timestamp — unix timestamp (int or string) or ISO datetime string
      user_id   — agent user_id for outbound messages
    """
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{BASE_URL}/contacts/{contact_id}/messages",
                headers=HEADERS,
            )
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception:
            return []

"""
Maqsam API client.
Auth: HTTP Basic Auth — Base64(ACCESS_KEY:ACCESS_SECRET).
Response envelope: {"result": "success", "message": [...]}
Agent info is embedded per call as agents[0].name — no separate /agents endpoint.
"""

import os
import base64
import httpx
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.maqsam.com/v2"
_key = os.getenv("MAQSAM_ACCESS_KEY", "")
_secret = os.getenv("MAQSAM_ACCESS_SECRET", "")
_token = base64.b64encode(f"{_key}:{_secret}".encode()).decode()
HEADERS = {"Authorization": f"Basic {_token}", "Content-Type": "application/json"}


def _extract_calls(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        return data
    return data.get("message", data.get("data", []))


def extract_agent_name(call: dict) -> str | None:
    agents = call.get("agents", [])
    if agents and isinstance(agents, list):
        return agents[0].get("name")
    return None


async def fetch_calls(date_from: str, date_to: str, page: int = 1, max_pages: int = 10) -> list[dict]:
    """
    Fetch call logs for a date range. date_from/date_to: 'YYYY-MM-DD'.
    Page size is 100 (fixed by API). max_pages caps total fetched per run.
    """
    calls: list[dict] = []
    current_page = page

    async with httpx.AsyncClient(timeout=30) as client:
        while current_page <= page + max_pages - 1:
            resp = await client.get(
                f"{BASE_URL}/calls",
                headers=HEADERS,
                params={"date_from": date_from, "date_to": date_to, "page": current_page},
            )
            resp.raise_for_status()
            batch = _extract_calls(resp.json())
            if not batch:
                break
            calls.extend(batch)
            if len(batch) < 100:
                break
            current_page += 1

    return calls


async def fetch_call(call_id: str) -> dict:
    """Fetch a single call record."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{BASE_URL}/calls/{call_id}", headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        calls = _extract_calls(data)
        return calls[0] if calls else data

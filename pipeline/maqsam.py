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


async def fetch_calls(date_from: str, date_to: str, since_ts: int | None = None) -> list[dict]:
    """
    Fetch call logs newest-first. Stops early once the oldest call on a page
    predates since_ts (unix timestamp). date_from/date_to are sent for completeness
    but the Maqsam API ignores them — filtering is done client-side via since_ts.

    If since_ts is None, falls back to filtering by date_from string comparison.
    """
    from datetime import datetime, timezone
    import time as _time

    # Derive since_ts from date_from when not supplied explicitly
    if since_ts is None:
        try:
            since_dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            since_ts = int(since_dt.timestamp())
        except Exception:
            since_ts = int(_time.time()) - 86400 * 7  # fallback: last 7 days

    calls: list[dict] = []
    page = 1

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(
                f"{BASE_URL}/calls",
                headers=HEADERS,
                params={"page": page},
            )
            resp.raise_for_status()
            batch = _extract_calls(resp.json())
            if not batch:
                break

            # Keep only calls within the time window
            in_window = [c for c in batch if int(c.get("timestamp") or 0) >= since_ts]
            calls.extend(in_window)

            # Stop when the oldest call on this page is before our cutoff
            oldest_ts = min((int(c.get("timestamp") or 0) for c in batch), default=0)
            if oldest_ts < since_ts:
                break
            if len(batch) < 100:
                break
            page += 1

    return calls


async def fetch_call(call_id: str) -> dict:
    """Fetch a single call record."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{BASE_URL}/calls/{call_id}", headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        calls = _extract_calls(data)
        return calls[0] if calls else data

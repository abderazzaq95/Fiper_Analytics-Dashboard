"""Dashboard helpers: teams (from Maqsam) and one customer's conversation."""
from __future__ import annotations

import json
import logging
import os
import re

import requests
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from api import dataset

from pipeline import agent_names
from pipeline.reply_tracker import parse_dt
from pipeline.swr_cache import SWRCache

load_dotenv()
log = logging.getLogger("fiper.workspace")
router = APIRouter()
supabase = dataset.db
_TEAMS = SWRCache("teams", 3600)


def build_teams(agents: list[dict]) -> list[dict]:
    """Maqsam agents [{name, groups, active}] -> [{name, agents: [display names]}]."""
    teams: dict[str, set[str]] = {}
    for agent in agents:
        if agent.get("active") is False:
            continue
        name = agent_names.display(re.sub(r"^[؀-؅؜‎‏‪-‮\s]+", "",
                                          str(agent.get("name") or agent.get("fullName") or "")))
        if not name:
            continue
        for group in agent.get("groups") or []:
            label = str(group.get("name") if isinstance(group, dict) else group).strip()
            if label:
                teams.setdefault(label, set()).add(name)
    return [{"name": team, "agents": sorted(members)} for team, members in sorted(teams.items())]


def _fetch_maqsam_agents() -> list[dict]:
    user = os.getenv("MAQSAM_API_USERNAME") or os.getenv("MAQSAM_ACCESS_KEY") or ""
    password = os.getenv("MAQSAM_API_PASSWORD") or os.getenv("MAQSAM_ACCESS_SECRET") or ""
    if not (user and password):
        return []
    # The QC app uses api.mq.maqsam.com; the call sync here uses api.maqsam.com.
    bases = [os.getenv("MAQSAM_BASE_URL", "https://api.mq.maqsam.com"), "https://api.maqsam.com"]
    last_error = None
    for base in dict.fromkeys(b.rstrip("/") for b in bases):
        try:
            rows = _fetch_agents_from(base, user, password)
            if rows:
                return rows
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            log.warning(f"Maqsam agents not available from {base}: {type(exc).__name__}")
    if last_error:
        raise last_error
    return []


def _fetch_agents_from(base: str, user: str, password: str) -> list[dict]:
    rows: list[dict] = []
    for page in range(1, 11):
        response = requests.get(f"{base}/v1/agents", params={"page": page}, auth=(user, password), timeout=30)
        response.raise_for_status()
        body = response.json()
        batch = body.get("message") if isinstance(body, dict) else body
        batch = batch if isinstance(batch, list) else []
        rows.extend(batch)
        if len(batch) < 100:
            break
    return rows


def _load_teams() -> list[dict]:
    # Optional manual list: {"Team name": ["Agent", ...]}
    manual = os.getenv("AGENT_TEAMS_JSON", "").strip()
    if manual:
        try:
            return build_teams([{"name": agent, "groups": [team]}
                                for team, members in json.loads(manual).items() for agent in members])
        except (ValueError, AttributeError) as exc:
            log.warning(f"AGENT_TEAMS_JSON is invalid: {exc}")
    try:
        return build_teams(_fetch_maqsam_agents())
    except Exception as exc:
        log.warning(f"Maqsam teams unavailable: {exc}")
        return []


@router.get("/api/teams")
def teams():
    return {"teams": _TEAMS.get("all", _load_teams, keep=bool)}


def phone_variants(phone: str) -> list[str]:
    digits = re.sub(r"\D+", "", phone or "")
    return [digits, f"+{digits}"] if len(digits) >= 7 else []


def _clean_body(message: dict) -> str:
    """ManyContacts prefixes agent messages with "*Agent:*"; the chat shows the name separately."""
    body = message.get("body") or ""
    return re.sub(r"^\s*\*[^*\r\n]{1,60}:\*\s*", "", body) if message.get("direction") == "outbound" else body


TEXT_MATCH_SECONDS = 90  # a "sent" event this close to a stored agent text is that text


def _untracked_sends(messages: list[dict], sends: list[dict]) -> list[dict]:
    """WhatsApp "sent" events with no stored agent text: voice notes, images,
    files (ManyContacts does not forward those as messages)."""
    texts = [dt for m in messages if m.get("direction") == "outbound" and (dt := parse_dt(m.get("sent_at")))]
    out = []
    for s in sends:
        dt = parse_dt(s.get("sent_at"))
        if dt and not any(abs((dt - t).total_seconds()) <= TEXT_MATCH_SECONDS for t in texts):
            out.append(s)
    return out


def build_timeline(messages: list[dict], calls: list[dict], limit: int = 200,
                   sends: list[dict] | None = None) -> list[dict]:
    events = [{
        "type": "media", "at": s.get("sent_at"), "direction": "outbound", "category": s.get("category"),
    } for s in _untracked_sends(messages, sends or [])]
    events += [{
        "type": "message", "at": m.get("sent_at"), "direction": m.get("direction"),
        "agent": agent_names.display(m.get("agent_name")) if m.get("agent_name") else None,
        "body": _clean_body(m), "line": m.get("whatsapp_business_number"),
    } for m in messages if m.get("sent_at")]
    events += [{
        "type": "call", "at": c.get("called_at"), "direction": "call",
        "agent": agent_names.display(c.get("agent_name")) if c.get("agent_name") else None,
        "outcome": c.get("outcome"), "duration_seconds": c.get("duration_seconds") or 0,
    } for c in calls if c.get("called_at")]
    return sorted(events, key=lambda e: e["at"])[-limit:]


@router.get("/api/conversation")
def conversation(phone: str = Query(..., min_length=7, max_length=32)):
    """Latest messages and calls with one customer, across duplicate lead rows."""
    variants = phone_variants(phone)
    if not variants:
        raise HTTPException(status_code=400, detail="invalid phone")
    leads = (supabase.table("leads").select("id,name,phone,assigned_agent,status,score,channel")
             .in_("phone", variants).execute().data or [])
    ids = [lead["id"] for lead in leads]
    if not ids:
        return {"phone": variants[1], "name": None, "agent": None, "events": []}
    messages = (supabase.table("messages").select("direction,body,agent_name,sent_at,whatsapp_business_number")
                .in_("lead_id", ids).order("sent_at", desc=True).limit(200).execute().data or [])
    calls = (supabase.table("calls").select("agent_name,outcome,duration_seconds,called_at")
             .in_("lead_id", ids).order("called_at", desc=True).limit(50).execute().data or [])
    try:
        sends = (supabase.table("whatsapp_sends").select("sent_at,category")
                 .eq("phone", variants[0]).order("sent_at", desc=True).limit(200).execute().data or [])
    except Exception:  # migration 005 not run yet
        sends = []
    if messages and len(messages) == 200:  # older history is cut off: keep sends to the same window
        sends = [s for s in sends if (s.get("sent_at") or "") >= messages[-1]["sent_at"]]
    whatsapp = next((lead for lead in leads if lead.get("channel") == "whatsapp"), leads[0])
    return {
        "phone": variants[1],
        "name": next((lead["name"] for lead in leads if lead.get("name")), None),
        "agent": agent_names.display(whatsapp.get("assigned_agent")) if whatsapp.get("assigned_agent") else None,
        "status": whatsapp.get("status"),
        "score": whatsapp.get("score"),
        "events": build_timeline(messages, calls, sends=sends),
    }

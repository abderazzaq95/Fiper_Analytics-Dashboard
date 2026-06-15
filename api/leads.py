from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
BATCH = 100


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    if range_ == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    if range_ in ("week", "7d"):
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    if range_ in ("month", "30d"):
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    start = now - timedelta(days=now.weekday())
    return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


@router.get("/api/leads")
def leads(range: str = Query("week", pattern="^(today|week|month|7d|30d)$")):
    try:
        return _leads_inner(range)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/leads error ({range}): {e}", exc_info=True)
        return {"range": range, "total": 0, "funnel": {}, "score_distribution": {"0-25":0,"26-50":0,"51-75":0,"76-100":0}, "leads": [], "hot_leads": []}


def _leads_inner(range: str):
    since = _since(range)

    # Activity-based filtering: WA leads by last_message_at, Maqsam leads by calls
    wa_leads = (
        supabase.table("leads")
        .select("id,name,phone,channel,status,score,assigned_agent,last_message_at,created_at")
        .eq("channel", "whatsapp")
        .gte("last_message_at", since)
        .order("score", desc=True)
        .execute()
        .data
    ) or []

    # Maqsam: find leads that had a call in the period
    call_rows = (
        supabase.table("calls")
        .select("lead_id")
        .gte("called_at", since)
        .execute()
        .data
    ) or []
    maqsam_ids = list({c["lead_id"] for c in call_rows if c.get("lead_id")})

    maqsam_leads = []
    for i in range(0, len(maqsam_ids), BATCH):
        chunk = (
            supabase.table("leads")
            .select("id,name,phone,channel,status,score,assigned_agent,last_message_at,created_at")
            .in_("id", maqsam_ids[i:i + BATCH])
            .execute()
            .data
        ) or []
        maqsam_leads.extend(chunk)

    # Deduplicate and sort by score
    seen: set = set()
    all_leads = []
    for lead in wa_leads + maqsam_leads:
        if lead["id"] not in seen:
            seen.add(lead["id"])
            all_leads.append(lead)
    all_leads.sort(key=lambda l: l.get("score") or 0, reverse=True)

    # Join AI summaries
    try:
        analyses = (
            supabase.table("ai_analysis")
            .select("lead_id,summary")
            .execute()
            .data
        ) or []
    except Exception:
        analyses = []
    summary_map = {a["lead_id"]: a.get("summary") for a in analyses}
    for lead in all_leads:
        lead["summary"] = summary_map.get(lead["id"])

    status_counts: dict = {}
    score_buckets = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for l in all_leads:
        status = l.get("status") or "new"
        status_counts[status] = status_counts.get(status, 0) + 1
        score = l.get("score")
        if score is None:
            continue
        if score <= 25:
            score_buckets["0-25"] += 1
        elif score <= 50:
            score_buckets["26-50"] += 1
        elif score <= 75:
            score_buckets["51-75"] += 1
        else:
            score_buckets["76-100"] += 1

    # Hot leads: score >= 80 AND status = 'new' (all-time)
    hot_leads_raw = (
        supabase.table("leads")
        .select("id,name,phone,channel,status,score,assigned_agent,last_message_at,created_at")
        .gte("score", 80)
        .eq("status", "new")
        .order("score", desc=True)
        .limit(50)
        .execute()
        .data
    ) or []

    return {
        "range": range,
        "total": len(all_leads),
        "funnel": status_counts,
        "score_distribution": score_buckets,
        "leads": all_leads,
        "hot_leads": hot_leads_raw,
    }

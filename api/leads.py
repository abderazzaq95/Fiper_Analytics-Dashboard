from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
BATCH = 100
PAGE = 1000
EMPTY_SCORE_BUCKETS = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}


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
        return {
            "range": range,
            "total": 0,
            "funnel": {},
            "score_distribution": _score_distribution(),
            "leads": [],
            "hot_leads": _hot_leads(),
        }


def _bucket_scores(rows):
    score_buckets = dict(EMPTY_SCORE_BUCKETS)

    for row in rows:
        score = row.get("score")
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

    return score_buckets


def _merge_by_phone(rows):
    merged = {}
    for row in rows:
        phone = row.get("phone") or row.get("id")
        if not phone:
            continue
        cur = merged.get(phone)
        if not cur:
            merged[phone] = {
                **row,
                "lead_ids": [row.get("id")] if row.get("id") else [],
            }
            continue
        if row.get("id") and row["id"] not in cur["lead_ids"]:
            cur["lead_ids"].append(row["id"])
        # Prefer the most recent activity, but keep the highest score.
        if (row.get("last_message_at") or "") > (cur.get("last_message_at") or ""):
            for key in ("name", "status", "assigned_agent", "channel", "last_message_at", "created_at"):
                if row.get(key) is not None:
                    cur[key] = row.get(key)
        if (row.get("score") or 0) > (cur.get("score") or 0):
            cur["score"] = row.get("score")
    return list(merged.values())


def _score_distribution():
    score_buckets = dict(EMPTY_SCORE_BUCKETS)
    offset = 0

    try:
        while True:
            rows = (
                supabase.table("leads")
                .select("score")
                .not_.is_("score", "null")
                .range(offset, offset + PAGE - 1)
                .execute()
                .data
            ) or []

            page_buckets = _bucket_scores(rows)
            for key, value in page_buckets.items():
                score_buckets[key] += value

            if len(rows) < PAGE:
                break
            offset += PAGE
    except Exception:
        return dict(EMPTY_SCORE_BUCKETS)

    return score_buckets


def _hot_leads():
    try:
        return (
            supabase.table("leads")
            .select("id,name,phone,channel,status,score,assigned_agent,last_message_at,created_at")
            .gte("score", 80)
            .eq("status", "new")
            .order("score", desc=True)
            .limit(50)
            .execute()
            .data
        ) or []
    except Exception:
        return []


def _leads_inner(range_: str):
    since = _since(range_)

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
    all_leads = _merge_by_phone(all_leads)
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
    for l in all_leads:
        status = l.get("status") or "new"
        status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "range": range_,
        "total": len(all_leads),
        "funnel": status_counts,
        "score_distribution": _bucket_scores(all_leads),
        "leads": all_leads,
        "hot_leads": _hot_leads(),
    }

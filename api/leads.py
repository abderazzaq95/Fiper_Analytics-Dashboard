from fastapi import APIRouter, Query
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from pipeline.time_ranges import RANGE_PATTERN
from api import dataset
from pipeline import agent_names
from pipeline.whatsapp import matches_business_line
from pipeline.swr_cache import SWRCache

load_dotenv()
_CACHE = SWRCache("leads", 300)
router = APIRouter()
supabase = dataset.db
BATCH = 100
PAGE = 1000
EMPTY_SCORE_BUCKETS = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}


def _empty(range_: str) -> dict:
    return {"range": range_, "total": 0, "funnel": {}, "score_distribution": dict(EMPTY_SCORE_BUCKETS),
            "avg_score": 0, "geo_prefixes": {}, "leads": [], "hot_leads": []}


@router.get("/api/leads")
def leads(
    range: str = Query("week", pattern=RANGE_PATTERN),
    wa_line: str = Query("all"),
    team: str = Query(""),
):
    try:
        return dataset.view(range, team, wa_line).cached("leads", _leads_inner)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/leads error ({range}): {e}", exc_info=True)
        return _empty(range)


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


def _with_agent_names(rows):
    for row in rows:
        if row.get("assigned_agent"):
            row["assigned_agent"] = agent_names.display(row["assigned_agent"])
    return rows


def _fetch_hot_leads() -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(days=HOT_LEAD_MAX_AGE_DAYS)).isoformat()
    rows = (
        supabase.table("leads")
        .select("id,name,phone,channel,status,score,assigned_agent,last_message_at,created_at,whatsapp_business_number")
        .gte("score", 80)
        .eq("status", "new")
        .gte("last_message_at", since)
        .order("score", desc=True)
        .limit(200)
        .execute()
        .data
    ) or []
    return _with_agent_names(rows)


def _hot_leads(view: dataset.View) -> list[dict]:
    try:
        rows = _CACHE.get("hot", _fetch_hot_leads, ttl=120)
    except Exception:
        return []
    if view.wa_line != "all":
        rows = [row for row in rows if matches_business_line(row, view.wa_line)]
    return [row for row in rows if view.has_agent(row.get("assigned_agent"))][:50]


def _leads_inner(view: dataset.View) -> dict:
    call_leads = [view.leads[i] for i in {c.get("lead_id") for c in view.calls} if i in view.leads]
    unique = {lead["id"]: lead for lead in [*view.activity, *call_leads]}
    all_leads = _with_agent_names(_merge_by_phone(list(unique.values())))
    all_leads.sort(key=lambda lead: lead.get("score") or 0, reverse=True)

    status_counts: dict = {}
    for lead in all_leads:
        status = lead.get("status") or "new"
        status_counts[status] = status_counts.get(status, 0) + 1

    # The page lists only the top leads; everything else is sent as aggregates.
    top = all_leads[:LIST_LIMIT]
    _attach_summaries(top)
    return {
        "range": view.period.range,
        "total": len(all_leads),
        "funnel": status_counts,
        "score_distribution": _bucket_scores(all_leads),
        "avg_score": _avg_score(all_leads),
        "geo_prefixes": _geo_prefixes(all_leads),
        "leads": top,
        "hot_leads": _hot_leads(view),
    }


LIST_LIMIT = 50
HOT_LEAD_MAX_AGE_DAYS = 14  # "immediate attention" must be recent


def _avg_score(rows):
    scores = [r["score"] for r in rows if r.get("score") is not None]
    return round(sum(scores) / len(scores), 1) if scores else 0


def _geo_prefixes(rows):
    """Lead count per first 4 phone digits; the page maps prefixes to countries."""
    counts: dict[str, int] = {}
    for r in rows:
        digits = "".join(ch for ch in str(r.get("phone") or "") if ch.isdigit())
        if digits.startswith("00"):
            digits = digits[2:]
        if digits:
            counts[digits[:4]] = counts.get(digits[:4], 0) + 1
    return counts


def _attach_summaries(rows):
    """Latest AI summary per listed lead. (Before: the whole ai_analysis table was
    read without paging, so Supabase's 1000-row cap dropped most summaries.)"""
    ids = list({i for r in rows for i in (r.get("lead_ids") or [r.get("id")]) if i})
    latest: dict[str, tuple[str, str]] = {}
    try:
        for i in range(0, len(ids), BATCH):
            for a in (supabase.table("ai_analysis").select("lead_id,summary,analyzed_at")
                      .in_("lead_id", ids[i:i + BATCH]).execute().data or []):
                if a.get("summary") and (a.get("analyzed_at") or "") >= latest.get(a["lead_id"], ("", ""))[0]:
                    latest[a["lead_id"]] = (a.get("analyzed_at") or "", a["summary"])
    except Exception:
        pass
    for r in rows:
        best = max((latest[i] for i in (r.get("lead_ids") or [r.get("id")]) if i in latest), default=None)
        r["summary"] = best[1] if best else None

from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import re
from pipeline.whatsapp import add_whatsapp_line_select, matches_business_line, resolve_agent_name

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
BATCH_SIZE = 100

# Server-side cache for /api/agents — keyed by (range, wa_line), TTL 5 minutes.
# Prevents the 45-request aggregation from running on every page load/refresh.
_agents_cache: dict[str, tuple[float, dict]] = {}
_AGENTS_CACHE_TTL = 300  # seconds


def _paginate(build_query) -> list:
    """Exhaust Supabase pagination — calls build_query() fresh each page."""
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


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


_NAME_ALIASES: dict[str, str] = {
    # ManyContacts spelling → canonical (Maqsam spelling wins as it has full name)
    "firas zabalwi":  "Feras Zabalawi",
    "feras zabalwi":  "Feras Zabalawi",
    "alaa al eish":   "Alaa Eddin Alesh",
    "alaa eddin alesh": "Alaa Eddin Alesh",
    "bashir":         "Basher Hallak",
    "basher":         "Basher Hallak",
    "fatima akel":    "Fatma Aqel",
    "fatima aqel":    "Fatma Aqel",
}


def _norm(name: str | None) -> str | None:
    """Normalize agent name: strip + Title Case, then apply alias map.
    Collapses cross-system duplicates (ManyContacts vs Maqsam spellings).
    """
    if not name:
        return None
    key = name.strip().lower()
    if key in _NAME_ALIASES:
        return _NAME_ALIASES[key]
    return name.strip().title()


def _looks_like_uuid(value: str | None) -> bool:
    if not value:
        return False
    return bool(re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", str(value).strip()))


def _agent_label(*values: str | None) -> str | None:
    """Return the first human-readable agent label from a list of candidates."""
    for value in values:
        if not value:
            continue
        if _looks_like_uuid(value):
            resolved = _norm(resolve_agent_name(value))
            if resolved and resolved.lower() not in ("unknown", "team", "n/a") and not _looks_like_uuid(resolved):
                return resolved
            continue
        normalized = _norm(value)
        if not normalized:
            continue
        if normalized.lower() in ("unknown", "team", "n/a"):
            continue
        return normalized
    return None


def _message_agent_name(
    row: dict,
    *,
    lead_agent_map: dict[str, str] | None = None,
    lead_to_agent_fallback: dict[str, str] | None = None,
) -> str | None:
    """Resolve the best agent label for a WhatsApp message row.

    Prefer the explicit message agent name. If that is missing, fall back to the
    lead's assigned agent, then the historical lead?agent fallback built from
    outbound WhatsApp rows and Maqsam calls.
    """
    raw_agent = row.get("agent_name")
    explicit = _norm(raw_agent)
    if explicit and not _looks_like_uuid(raw_agent):
        return explicit
    if raw_agent and _looks_like_uuid(raw_agent):
        resolved = _agent_label(resolve_agent_name(raw_agent))
        if resolved:
            return resolved
    lead_id = row.get("lead_id")
    if lead_id and lead_agent_map:
        lead_agent = _agent_label(lead_agent_map.get(lead_id))
        if lead_agent:
            return lead_agent
    if lead_id and lead_to_agent_fallback:
        fallback = _agent_label(lead_to_agent_fallback.get(lead_id))
        if fallback:
            return fallback
    return None


@router.get("/api/agents")
def agents(
    range: str = Query("week", pattern="^(today|week|month|7d|30d)$"),
    wa_line: str = Query("all"),
    refresh: str = Query("0"),
):
    import logging, time
    log = logging.getLogger("fiper")
    cache_key = f"{range}:{wa_line}"
    now_ts = time.time()

    # Return cached result if still fresh (skip cache when refresh=1)
    if refresh != "1":
        cached = _agents_cache.get(cache_key)
        if cached:
            cached_at, cached_data = cached
            if now_ts - cached_at < _AGENTS_CACHE_TTL:
                return cached_data

    try:
        data = _agents_inner(range, wa_line)
        if not data.get("agents"):
            fallback = _agents_lightweight(range, wa_line)
            if fallback.get("agents"):
                _agents_cache[cache_key] = (now_ts, fallback)
                return fallback
        _agents_cache[cache_key] = (now_ts, data)
        return data
    except Exception as e:
        log.error(f"/api/agents error ({range}): {e}", exc_info=True)
        # Try lightweight fallback before giving up
        try:
            fallback = _agents_lightweight(range, wa_line)
            if fallback.get("agents"):
                _agents_cache[cache_key] = (now_ts, fallback)
                return fallback
        except Exception as fallback_error:
            log.error(
                f"/api/agents lightweight fallback error ({range}): {fallback_error}",
                exc_info=True,
            )
        return {"range": range, "agents": []}


def _agents_lightweight(range: str, wa_line: str = "all"):
    since = _since(range)
    calls = _paginate(
        lambda: supabase.table("calls")
        .select("agent_name,lead_id,duration_seconds,called_at")
        .gte("called_at", since)
    )
    messages = _paginate(
        lambda: supabase.table("messages")
        .select("agent_name,direction,sent_at,lead_id,whatsapp_business_number")
        .gte("sent_at", since)
    )
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        messages = [m for m in messages if matches_business_line(m, wa_line)]
    alerts = (
        supabase.table("alerts")
        .select("agent_name,lead_id,severity,type,message,resolved,created_at")
        .eq("resolved", False)
        .gte("created_at", since)
        .execute().data or []
    )
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        alert_lead_ids = list({a.get("lead_id") for a in alerts if a.get("lead_id")})
        lead_rows = []
        for idx in range(0, len(alert_lead_ids), 100):
            lead_rows.extend(
                supabase.table("leads")
                .select("id,channel,whatsapp_business_number")
                .in_("id", alert_lead_ids[idx:idx + 100])
                .execute().data or []
            )
        lead_map = {r["id"]: r for r in lead_rows}
        alerts = [a for a in alerts if matches_business_line(lead_map.get(a.get("lead_id") or ""), wa_line)]

    active_lead_ids = {
        row.get("lead_id")
        for row in [*calls, *messages, *alerts]
        if row.get("lead_id")
    }
    lead_rows = []
    active_ids = list(active_lead_ids)
    idx = 0
    while idx < len(active_ids):
        lead_rows.extend(
            supabase.table("leads")
            .select("id,assigned_agent,channel,whatsapp_business_number")
            .in_("id", active_ids[idx:idx + 100])
            .execute().data or []
        )
        idx += 100
    lead_agent_map = {r["id"]: _agent_label(r.get("assigned_agent")) for r in lead_rows if r.get("id")}
    lead_to_agent_fallback: dict[str, str] = {}
    for m in messages:
        lid = m.get("lead_id")
        ag = _message_agent_name(m, lead_agent_map=lead_agent_map)
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in calls:
        lid = c.get("lead_id")
        ag = _agent_label(c.get("agent_name"))
        if lid and ag:
            lead_to_agent_fallback.setdefault(lid, ag)

    agents = sorted({
        *(
            _agent_label(c.get("agent_name"))
            for c in calls
            if _agent_label(c.get("agent_name"))
        ),
        *(
            _message_agent_name(
                m,
                lead_agent_map=lead_agent_map,
                lead_to_agent_fallback=lead_to_agent_fallback,
            )
            for m in messages
            if _message_agent_name(
                m,
                lead_agent_map=lead_agent_map,
                lead_to_agent_fallback=lead_to_agent_fallback,
            )
        ),
        *(
            _agent_label(a.get("agent_name"))
            for a in alerts
            if _agent_label(a.get("agent_name"))
        ),
    })

    rows = []
    for agent in agents:
        agent_calls = [c for c in calls if _agent_label(c.get("agent_name")) == agent]
        agent_msgs = [
            m for m in messages
            if _message_agent_name(
                m,
                lead_agent_map=lead_agent_map,
                lead_to_agent_fallback=lead_to_agent_fallback,
            ) == agent
        ]
        agent_alerts = [
            a for a in alerts
            if _agent_label(a.get("agent_name")) == agent
        ]
        completed_calls = [c for c in agent_calls if (c.get("duration_seconds") or 0) > 0]
        lead_ids = {
            item.get("lead_id")
            for item in [*agent_calls, *agent_msgs, *agent_alerts]
            if item.get("lead_id")
        }
        outbound_lw   = [m for m in agent_msgs if m.get("direction") == "outbound"]
        wa_chats_lw   = len({m["lead_id"] for m in outbound_lw if m.get("lead_id")})
        messages_sent_lw = len(outbound_lw)
        rows.append({
            "agent": agent,
            "leads": len(lead_ids),
            "calls_handled": len(completed_calls),
            "wa_chats": wa_chats_lw,
            "messages_sent": messages_sent_lw,
            "avg_call_duration_seconds": round(
                sum(c.get("duration_seconds") or 0 for c in completed_calls) / len(completed_calls)
            ) if completed_calls else 0,
            "avg_response_time_min": 0,
            "avg_treatment_score": 0,
            "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            "open_alerts": len(agent_alerts),
            "alert_details": [
                {
                    "lead_id": a.get("lead_id"),
                    "severity": a.get("severity") or "MED",
                    "type": a.get("type") or "alert",
                    "message": a.get("message") or "",
                    "created_at": a.get("created_at"),
                }
                for a in agent_alerts
            ],
            "quality_trend": [],
        })
    rows.sort(key=lambda x: x["calls_handled"], reverse=True)
    return {"range": range, "agents": rows, "fallback": "lightweight"}


def _fetch_in_parallel(table: str, select: str, id_field: str, ids: list[str], extra_filters=None) -> list[dict]:
    """Fetch rows where id_field IN ids, using parallel threads for batch requests."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    if not ids:
        return []
    chunks = [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]

    def fetch_chunk(chunk):
        q = supabase.table(table).select(select).in_(id_field, chunk)
        if extra_filters:
            for method, args in extra_filters:
                q = getattr(q, method)(*args)
        return q.execute().data or []

    results: list[dict] = []
    # Use up to 6 parallel workers — each Supabase request is ~200-400ms
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fetch_chunk, chunk): chunk for chunk in chunks}
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception:
                pass
    return results


def _agents_inner(range: str, wa_line: str = "all"):
    from concurrent.futures import ThreadPoolExecutor, as_completed as cf_as_completed
    since = _since(range)

    # Fetch messages, calls, and alerts in parallel (3 independent queries)
    def _fetch_messages():
        return _paginate(lambda: supabase.table("messages")
            .select(add_whatsapp_line_select("agent_name,direction,sent_at,lead_id"))
            .gte("sent_at", since))

    def _fetch_calls():
        return _paginate(lambda: supabase.table("calls")
            .select("agent_name,lead_id,duration_seconds,called_at")
            .gte("called_at", since))

    def _fetch_alerts():
        return (
            supabase.table("alerts")
            .select("id,agent_name,lead_id,severity,type,message,resolved,created_at")
            .eq("resolved", False)
            .gte("created_at", since)
            .order("created_at", desc=True)
            .execute()
            .data
        ) or []

    with ThreadPoolExecutor(max_workers=3) as executor:
        f_msgs   = executor.submit(_fetch_messages)
        f_calls  = executor.submit(_fetch_calls)
        f_alerts = executor.submit(_fetch_alerts)
        messages = f_msgs.result()
        calls    = f_calls.result()
        alerts   = f_alerts.result()

    # Collect all active lead IDs
    active_lead_ids = {
        row.get("lead_id")
        for row in [*messages, *calls, *alerts]
        if row.get("lead_id")
    }
    active_ids = list(active_lead_ids)

    # Fetch leads and analyses in parallel using threaded batch requests
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_leads    = executor.submit(_fetch_in_parallel, "leads",
            "id,phone,name,assigned_agent,status,score,channel,whatsapp_business_number",
            "id", active_ids)
        f_analyses = executor.submit(_fetch_in_parallel, "ai_analysis",
            "lead_id,sentiment,treatment_score,source,analyzed_at,outcome",
            "lead_id", active_ids)
        leads    = f_leads.result()
        analyses = f_analyses.result()

    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        messages = [m for m in messages if matches_business_line(m, wa_line)]
        leads = [l for l in leads if matches_business_line(l, wa_line)]
        alert_lead_map = {l["id"]: l for l in leads if l.get("id")}
        alerts = [a for a in alerts if matches_business_line(alert_lead_map.get(a.get("lead_id") or ""), wa_line)]

    # ── Build lookup dicts (all agent keys normalized to Title Case) ──────────

    # leads grouped by assigned_agent (WhatsApp path)
    agent_leads: dict[str, list] = defaultdict(list)
    for l in leads:
        ag = _agent_label(l.get("assigned_agent"))
        if ag:
            agent_leads[ag].append(l)

    # WhatsApp messages grouped by agent
    lead_by_id = {l["id"]: l for l in leads if l.get("id")}
    lead_agent_map = {l["id"]: _agent_label(l.get("assigned_agent")) for l in leads if l.get("id")}

    # Build a historical fallback map from outbound messages and calls so rows
    # without an explicit agent_name still count for the right agent.
    lead_to_agent_fallback: dict[str, str] = {}
    for m in messages:
        lid = m.get("lead_id")
        ag = _message_agent_name(m, lead_agent_map=lead_agent_map)
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in calls:
        lid = c.get("lead_id")
        ag = _agent_label(c.get("agent_name"))
        if lid and ag:
            lead_to_agent_fallback.setdefault(lid, ag)

    agent_msgs: dict[str, list] = defaultdict(list)
    # WA-only dict: attribute by explicit name OR assigned_agent — but NO
    # lead_to_agent_fallback so one agent can't steal another's leads.
    agent_wa_msgs: dict[str, list] = defaultdict(list)
    for m in messages:
        ag = _message_agent_name(
            m,
            lead_agent_map=lead_agent_map,
            lead_to_agent_fallback=lead_to_agent_fallback,
        )
        if ag:
            agent_msgs[ag].append(m)
        wa_ag = _message_agent_name(m, lead_agent_map=lead_agent_map)
        if wa_ag:
            agent_wa_msgs[wa_ag].append(m)

    # Maqsam calls grouped by agent + their lead_ids
    agent_calls: dict[str, list] = defaultdict(list)
    call_lead_ids_by_agent: dict[str, set] = defaultdict(set)
    for c in calls:
        ag = _agent_label(c.get("agent_name"))
        if ag:
            agent_calls[ag].append(c)
            if c.get("lead_id"):
                call_lead_ids_by_agent[ag].add(c["lead_id"])

    # ai_analysis grouped by lead_id
    lead_analysis: dict[str, list] = defaultdict(list)
    for a in analyses:
        lead_analysis[a["lead_id"]].append(a)

    # Alert attribution: normalized agent_name → list of alerts
    lead_by_id = {l["id"]: l for l in leads if l.get("id")}
    lead_agent_map = {l["id"]: _agent_label(l.get("assigned_agent")) for l in leads}

    # All-time lead → agent fallback (uses unfiltered calls so old leads are covered)
    lead_to_agent_fallback: dict[str, str] = {}
    for m in messages:
        lid = m.get("lead_id")
        ag = _agent_label(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in calls:
        lid = c.get("lead_id")
        ag = _agent_label(c.get("agent_name"))
        if lid and ag:
            lead_to_agent_fallback.setdefault(lid, ag)

    agent_alerts: dict[str, list] = defaultdict(list)
    for a in alerts:
        lid = a.get("lead_id")
        # Keep a stable fallback label so open alerts never disappear from the
        # leaderboard just because ManyContacts only sent a UUID.
        raw_alert_value = str(a.get("agent_name") or "").strip() or None
        raw_alert_agent = _agent_label(raw_alert_value)
        if raw_alert_agent and raw_alert_agent.lower() in ("unknown", "team", "n/a"):
            raw_alert_agent = None
        raw_lead_value = None
        if lid and lead_by_id.get(lid):
            raw_lead_value = str(lead_by_id[lid].get("assigned_agent") or "").strip() or None
        lead_agent = _agent_label(raw_lead_value)
        agent = _agent_label(
            raw_alert_agent,
            lead_agent,
            lead_to_agent_fallback.get(lid) if lid else None,
        ) or raw_alert_agent or lead_agent or raw_alert_value or raw_lead_value
        if agent:
            lead = lead_by_id.get(lid, {})
            agent_alerts[agent].append({
                "id": a.get("id"),
                "lead_id": lid,
                "lead_phone": lead.get("phone"),
                "lead_name": lead.get("name"),
                "severity": a.get("severity") or "MED",
                "type": a.get("type") or "alert",
                "message": a.get("message") or "",
                "created_at": a.get("created_at"),
            })

    # All agents: WhatsApp (leads + messages) + Maqsam (calls)
    all_agents = set(agent_leads.keys()) | set(agent_msgs.keys()) | set(agent_calls.keys()) | set(agent_alerts.keys())

    result = []
    for agent in all_agents:
        al = agent_leads.get(agent, [])
        am = agent_msgs.get(agent, [])
        ac = agent_calls.get(agent, [])

        # ── Lead IDs this agent is responsible for ────────────────────────────
        # Union of: leads with assigned_agent + all leads from their calls
        analysis_lead_ids = {l["id"] for l in al} | call_lead_ids_by_agent.get(agent, set())
        total_leads = len(analysis_lead_ids)

        # Fix 1: converted = leads where ai_analysis.outcome == 'converted'
        # leads.status never contains 'converted' — outcome lives in ai_analysis
        converted = sum(
            1 for lid in analysis_lead_ids
            if any(a.get("outcome") == "converted" for a in lead_analysis.get(lid, []))
        )

        completed_calls = [c for c in ac if (c.get("duration_seconds") or 0) > 0]
        calls_answered  = len(completed_calls)
        avg_call_dur    = (
            round(sum(c["duration_seconds"] for c in completed_calls) / calls_answered)
            if calls_answered else 0
        )

        # Response times (WhatsApp inbound → outbound gap)
        by_lead: dict[str, list] = defaultdict(list)
        for m in am:
            by_lead[m["lead_id"]].append(m)
        response_times = []
        for msgs in by_lead.values():
            sorted_msgs = sorted(msgs, key=lambda x: x.get("sent_at") or "")
            last_in = None
            for m in sorted_msgs:
                if m["direction"] == "inbound":
                    last_in = datetime.fromisoformat(m["sent_at"].replace("Z", "+00:00"))
                elif m["direction"] == "outbound" and last_in:
                    out_t = datetime.fromisoformat(m["sent_at"].replace("Z", "+00:00"))
                    gap = (out_t - last_in).total_seconds() / 60
                    if gap >= 0:
                        response_times.append(gap)
                    last_in = None

        # ── Collect ai_analysis for this agent ───────────────────────────────
        sentiments: list[str] = []
        treatment_scores: list[float] = []
        for lid in analysis_lead_ids:
            for a in lead_analysis.get(lid, []):
                if a.get("sentiment"):
                    sentiments.append(a["sentiment"])
                if a.get("treatment_score") is not None:
                    # Exclude maqsam score=0: no-answer call artifacts
                    if not (a.get("source") == "maqsam" and a["treatment_score"] == 0):
                        treatment_scores.append(a["treatment_score"])

        sentiment_summary = {
            "positive": sentiments.count("positive"),
            "neutral":  sentiments.count("neutral"),
            "negative": sentiments.count("negative"),
        }

        # Daily quality trend: last 7 days of treatment scores for this agent
        seven_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        daily_scores: dict[str, list] = defaultdict(list)
        for lid in analysis_lead_ids:
            for a in lead_analysis.get(lid, []):
                ts = a.get("analyzed_at")
                score = a.get("treatment_score")
                if (ts and score is not None and ts >= seven_ago
                        and not (a.get("source") == "maqsam" and score == 0)):
                    daily_scores[ts[:10]].append(score)
        quality_trend = sorted(
            [{"date": day, "avg_score": round(sum(s) / len(s), 1)} for day, s in daily_scores.items()],
            key=lambda x: x["date"],
        )

        # Use WA-attributed messages (explicit name + assigned_agent, no fallback)
        wa_msgs       = agent_wa_msgs.get(agent, [])
        outbound_msgs = [m for m in wa_msgs if m.get("direction") == "outbound"]
        wa_chats      = len({m["lead_id"] for m in outbound_msgs if m.get("lead_id")})
        messages_sent = len(outbound_msgs)

        result.append({
            "agent":                    agent,
            "leads":                    total_leads,
            "calls_handled":            calls_answered,
            "wa_chats":                 wa_chats,
            "messages_sent":            messages_sent,
            "avg_call_duration_seconds": avg_call_dur,
            "avg_response_time_min":    round(sum(response_times) / len(response_times), 1) if response_times else 0,
            "avg_treatment_score":      round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
            "sentiment":                sentiment_summary,
            "open_alerts":              len(agent_alerts.get(agent, [])),
            "alert_details":            agent_alerts.get(agent, []),
            "quality_trend":            quality_trend,
        })

    result.sort(key=lambda x: x["calls_handled"], reverse=True)
    if not result:
        fallback_agents = set()
        fallback_agents.update(_agent_label(c.get("agent_name")) for c in calls if _agent_label(c.get("agent_name")))
        fallback_agents.update(
            _message_agent_name(
                m,
                lead_agent_map=lead_agent_map,
                lead_to_agent_fallback=lead_to_agent_fallback,
            )
            for m in messages
            if _message_agent_name(
                m,
                lead_agent_map=lead_agent_map,
                lead_to_agent_fallback=lead_to_agent_fallback,
            )
        )
        fallback_agents.update(agent_alerts.keys())
        for agent in fallback_agents:
            ac = [c for c in calls if _agent_label(c.get("agent_name")) == agent]
            am = [m for m in messages if _message_agent_name(m, lead_agent_map=lead_agent_map, lead_to_agent_fallback=lead_to_agent_fallback) == agent]
            lead_ids = {
                row.get("lead_id")
                for row in [*ac, *am]
                if row.get("lead_id")
            }
            completed_calls = [c for c in ac if (c.get("duration_seconds") or 0) > 0]
            ob_fb = [m for m in am if m.get("direction") == "outbound"]
            result.append({
                "agent": agent,
                "leads": len(lead_ids),
                "calls_handled": len(completed_calls),
                "wa_chats": len({m["lead_id"] for m in ob_fb if m.get("lead_id")}),
                "messages_sent": len(ob_fb),
                "avg_call_duration_seconds": round(
                    sum(c.get("duration_seconds") or 0 for c in completed_calls) / len(completed_calls)
                ) if completed_calls else 0,
                "avg_response_time_min": 0,
                "avg_treatment_score": 0,
                "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
                "open_alerts": len(agent_alerts.get(agent, [])),
                "alert_details": agent_alerts.get(agent, []),
                "quality_trend": [],
            })
        result.sort(key=lambda x: x["calls_handled"], reverse=True)
    return {"range": range, "agents": result}


@router.get("/api/agents/alerts")
def agent_alerts(
    agent: str = Query(...),
    range: str = Query("week", pattern="^(today|week|month|7d|30d)$"),
):
    """Return open alerts attributed to one normalized agent.

    Uses the same attribution rules as the leaderboard: explicit alert agent,
    then lead assigned_agent, then time-bounded call/message fallback.
    """
    try:
        return _agent_alerts_inner(agent, range)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/agents/alerts error ({agent}): {e}", exc_info=True)
        return {"agent": _norm(agent) or agent, "alerts": []}


def _agent_alerts_inner(agent: str, range_: str):
    target = _norm(agent)
    if not target:
        return {"agent": agent, "alerts": []}

    since = _since(range_)

    # All name spelling variants for DB matching
    target_spellings: set[str] = {target}
    for raw_key, resolved in _NAME_ALIASES.items():
        if resolved == target:
            target_spellings.add(raw_key.strip().title())
    spelling_list = list(target_spellings)

    # Fetch all open alerts (small dataset — ~333 rows)
    all_alerts = (
        supabase.table("alerts")
        .select("id,agent_name,lead_id,severity,type,message,resolved,created_at")
        .eq("resolved", False)
        .gte("created_at", since)
        .order("created_at", desc=True)
        .execute()
        .data
    ) or []

    # Pass 1: alerts with an explicit matching agent_name (fast, no lead lookup needed)
    matched_alerts: list[dict] = []
    need_lead_lookup: list[dict] = []
    for a in all_alerts:
        raw = _agent_label(a.get("agent_name"))
        if raw and raw.lower() in ("unknown", "team", "n/a"):
            raw = None
        if raw == target:
            matched_alerts.append(a)
        elif raw is None:
            need_lead_lookup.append(a)
        # else: belongs to a different agent — skip

    # Pass 2: for alerts without agent_name, fetch only their lead IDs
    if need_lead_lookup:
        lookup_lead_ids = list({a["lead_id"] for a in need_lead_lookup if a.get("lead_id")})
        # Fetch only the leads we actually need
        lookup_leads = _fetch_in_parallel(
            "leads", "id,phone,name,assigned_agent", "id", lookup_lead_ids
        )
        lead_by_id_lk = {l["id"]: l for l in lookup_leads if l.get("id")}
        lead_agent_map_lk = {l["id"]: _agent_label(l.get("assigned_agent")) for l in lookup_leads}
        for a in need_lead_lookup:
            lid = a.get("lead_id")
            if lead_agent_map_lk.get(lid) == target:
                matched_alerts.append(a)
                # Attach lead details
                a["_lead"] = lead_by_id_lk.get(lid, {})

    # Also fetch lead details for directly-matched alerts
    direct_lead_ids = list({a["lead_id"] for a in matched_alerts if a.get("lead_id") and "_lead" not in a})
    if direct_lead_ids:
        direct_leads = _fetch_in_parallel("leads", "id,phone,name,assigned_agent", "id", direct_lead_ids)
        dl_map = {l["id"]: l for l in direct_leads if l.get("id")}
        for a in matched_alerts:
            if "_lead" not in a:
                a["_lead"] = dl_map.get(a.get("lead_id"), {})

    rows = []
    for a in matched_alerts:
        lead = a.pop("_lead", {})
        rows.append({
            "id": a.get("id"),
            "lead_id": a.get("lead_id"),
            "lead_phone": lead.get("phone"),
            "lead_name": lead.get("name"),
            "severity": a.get("severity") or "MED",
            "type": a.get("type") or "alert",
            "message": a.get("message") or "",
            "created_at": a.get("created_at"),
        })

    return {"agent": target, "alerts": rows}


@router.get("/api/agents/detail")
def agent_detail(
    agent: str = Query(...),
    range: str = Query("week", pattern="^(today|week|month|7d|30d)$"),
    wa_line: str = Query("all"),
):
    try:
        return _agent_detail_inner(agent, range, wa_line)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/agents/detail error ({agent}): {e}", exc_info=True)
        return {"agent": agent, "calls": [], "wa_leads": [], "analyses": [], "alerts": []}


def _agent_detail_inner(agent: str, range_: str, wa_line: str):
    from concurrent.futures import ThreadPoolExecutor
    target = _norm(agent)
    if not target:
        return {"agent": agent, "stats": {}, "calls": [], "wa_leads": [], "analyses": [], "alerts": []}

    since = _since(range_)
    msg_select = add_whatsapp_line_select("agent_name,direction,sent_at,lead_id")

    # Build all spelling variants for this agent (ilike is case-insensitive)
    target_spellings: set[str] = {target}
    for raw_key, resolved in _NAME_ALIASES.items():
        if resolved == target:
            target_spellings.add(raw_key.strip().title())
    target_keys_list = list(target_spellings)

    # ── Round 1 (parallel): calls + assigned leads + messages by name ─────────
    def _fetch_calls_nv(nv):
        return _paginate(lambda nv2=nv: supabase.table("calls")
            .select("lead_id,duration_seconds,called_at,agent_name")
            .gte("called_at", since).ilike("agent_name", nv2))

    def _fetch_assigned_nv(nv):
        try:
            return supabase.table("leads")\
                .select("id,phone,name,assigned_agent,status,score,channel,whatsapp_business_number")\
                .ilike("assigned_agent", nv).gte("last_message_at", since)\
                .execute().data or []
        except Exception:
            return []

    def _fetch_msgs_nv(nv):
        return _paginate(lambda nv2=nv: supabase.table("messages")
            .select(msg_select).gte("sent_at", since).ilike("agent_name", nv2))

    raw_calls_parts: list[list] = []
    assigned_parts: list[list] = []
    msgs_name_parts: list[list] = []

    with ThreadPoolExecutor(max_workers=min(len(target_keys_list) * 3, 9)) as ex:
        fc = [ex.submit(_fetch_calls_nv, nv) for nv in target_keys_list]
        fa = [ex.submit(_fetch_assigned_nv, nv) for nv in target_keys_list]
        fm = [ex.submit(_fetch_msgs_nv, nv) for nv in target_keys_list]
        for f in fc: raw_calls_parts.extend(f.result())
        for f in fa: assigned_parts.extend(f.result())
        for f in fm: msgs_name_parts.extend(f.result())

    # Deduplicate calls
    seen_calls: set[tuple] = set()
    raw_calls: list[dict] = []
    for c in raw_calls_parts:
        k = (c.get("lead_id"), c.get("called_at"))
        if k not in seen_calls:
            seen_calls.add(k); raw_calls.append(c)

    # Deduplicate assigned leads
    seen_lids: set[str] = set()
    assigned_lead_rows: list[dict] = []
    for l in assigned_parts:
        if l.get("id") and l["id"] not in seen_lids:
            seen_lids.add(l["id"]); assigned_lead_rows.append(l)

    call_lead_ids: set[str] = {c["lead_id"] for c in raw_calls if c.get("lead_id")}
    assigned_lead_ids: set[str] = {l["id"] for l in assigned_lead_rows}
    msg_lead_ids_direct: set[str] = {m["lead_id"] for m in msgs_name_parts if m.get("lead_id")}

    all_agent_lead_ids = list(call_lead_ids | assigned_lead_ids | msg_lead_ids_direct)
    extra_lead_ids   = [lid for lid in all_agent_lead_ids if lid not in msg_lead_ids_direct]
    remaining_ids    = [lid for lid in all_agent_lead_ids if lid not in assigned_lead_ids]

    # ── Round 2 (parallel): extra messages + lead details + open alerts ────────
    extra_chunks   = [extra_lead_ids[i:i+BATCH_SIZE]  for i in range(0, len(extra_lead_ids),  BATCH_SIZE)]
    remain_chunks  = [remaining_ids[i:i+BATCH_SIZE]   for i in range(0, len(remaining_ids),   BATCH_SIZE)]

    def _fetch_extra_msgs(chunk):
        return supabase.table("messages").select(msg_select)\
            .gte("sent_at", since).in_("lead_id", chunk).execute().data or []

    def _fetch_lead_detail(chunk):
        return supabase.table("leads")\
            .select("id,phone,name,assigned_agent,status,score,channel,whatsapp_business_number")\
            .in_("id", chunk).execute().data or []

    def _fetch_all_alerts():
        query = supabase.table("alerts")\
            .select("id,agent_name,lead_id,severity,type,message,resolved,created_at")\
            .eq("resolved", False).order("created_at", desc=True)
        query = query.gte("created_at", since)
        return query.execute().data or []

    raw_msgs:   list[dict] = list(msgs_name_parts)
    leads_raw:  list[dict] = list(assigned_lead_rows)
    alerts_raw: list[dict] = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        fe = [ex.submit(_fetch_extra_msgs, ch)  for ch in extra_chunks]
        fl = [ex.submit(_fetch_lead_detail, ch) for ch in remain_chunks]
        fa2 = ex.submit(_fetch_all_alerts)
        for f in fe: raw_msgs.extend(f.result())
        for f in fl: leads_raw.extend(f.result())
        alerts_raw = fa2.result()

    lead_by_id    = {l["id"]: l for l in leads_raw if l.get("id")}
    lead_agent_map = {l["id"]: _agent_label(l.get("assigned_agent")) for l in leads_raw if l.get("id")}

    lead_to_agent_fallback: dict[str, str] = {}
    for m in raw_msgs:
        lid = m.get("lead_id"); ag = _norm(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in raw_calls:
        lid = c.get("lead_id"); ag = _agent_label(c.get("agent_name"))
        if lid and ag:
            lead_to_agent_fallback.setdefault(lid, ag)

    agent_calls = sorted(raw_calls, key=lambda x: x.get("called_at") or "", reverse=True)
    agent_msgs  = [m for m in raw_msgs
                   if _message_agent_name(m, lead_agent_map=lead_agent_map,
                                          lead_to_agent_fallback=lead_to_agent_fallback) == target]

    agent_lead_ids: set[str] = (
        {l["id"] for l in leads_raw if _norm(l.get("assigned_agent")) == target}
        | {c["lead_id"] for c in agent_calls if c.get("lead_id")}
        | {m["lead_id"] for m in agent_msgs  if m.get("lead_id")}
    )

    # ── Round 3 (parallel): analyses + alert lead details ─────────────────────
    aid_list = list(agent_lead_ids)
    alert_extra_ids = [a.get("lead_id") for a in alerts_raw
                       if a.get("lead_id") and a.get("lead_id") not in lead_by_id]

    aid_chunks         = [aid_list[i:i+BATCH_SIZE]        for i in range(0, len(aid_list),        BATCH_SIZE)]
    alert_lead_chunks  = [alert_extra_ids[i:i+BATCH_SIZE] for i in range(0, len(alert_extra_ids), BATCH_SIZE)]

    def _fetch_analyses(chunk):
        return supabase.table("ai_analysis")\
            .select("lead_id,sentiment,treatment_score,outcome,summary,risk_flags,analyzed_at,source")\
            .in_("lead_id", chunk).execute().data or []

    def _fetch_alert_leads(chunk):
        return supabase.table("leads")\
            .select("id,phone,name,assigned_agent").in_("id", chunk).execute().data or []

    analyses_raw: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        fan = [ex.submit(_fetch_analyses,    ch) for ch in aid_chunks]
        fal = [ex.submit(_fetch_alert_leads, ch) for ch in alert_lead_chunks]
        for f in fan: analyses_raw.extend(f.result())
        for f in fal:
            for r in f.result():
                if r.get("id"):
                    lead_by_id[r["id"]] = r

    analyses_raw.sort(key=lambda x: x.get("analyzed_at") or "", reverse=True)

    fb_full: dict[str, str] = {}
    for m in raw_msgs:
        lid = m.get("lead_id"); ag = _agent_label(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            fb_full.setdefault(lid, ag)
    for c in raw_calls:
        lid = c.get("lead_id"); ag = _agent_label(c.get("agent_name"))
        if lid and ag:
            fb_full.setdefault(lid, ag)

    alerts_out = []
    for a in alerts_raw:
        lid = a.get("lead_id")
        raw_alert_value = str(a.get("agent_name") or "").strip() or None
        raw_ag = _agent_label(raw_alert_value)
        if raw_ag and raw_ag.lower() in ("unknown", "team", "n/a"):
            raw_ag = None
        raw_lead_value = None
        if lid and lead_by_id.get(lid):
            raw_lead_value = str(lead_by_id[lid].get("assigned_agent") or "").strip() or None
        lead_ag = _agent_label(raw_lead_value)
        attributed = _agent_label(
            raw_ag,
            lead_ag,
            fb_full.get(lid) if lid else None,
        ) or raw_ag or lead_ag or raw_alert_value or raw_lead_value
        if attributed != target:
            continue
        lead = lead_by_id.get(lid, {})
        alerts_out.append({
            "id": a.get("id"), "lead_id": lid,
            "lead_phone": lead.get("phone"), "lead_name": lead.get("name"),
            "severity": a.get("severity") or "MED",
            "type": a.get("type") or "alert",
            "message": a.get("message") or "",
            "created_at": a.get("created_at"),
        })

    # ── Aggregated stats (same range as lists) ────────────────────────────────
    completed_calls = [c for c in agent_calls if (c.get("duration_seconds") or 0) > 0]
    calls_handled = len(completed_calls)
    avg_call_dur = (
        round(sum(c["duration_seconds"] for c in completed_calls) / calls_handled)
        if calls_handled else 0
    )
    outbound_msgs = [m for m in agent_msgs if m.get("direction") == "outbound"]
    wa_chats = len({m["lead_id"] for m in outbound_msgs if m.get("lead_id")})
    messages_sent = len(outbound_msgs)

    # Response times
    by_lead_rt: dict[str, list] = defaultdict(list)
    for m in agent_msgs:
        if m.get("lead_id"):
            by_lead_rt[m["lead_id"]].append(m)
    response_times = []
    for msgs in by_lead_rt.values():
        sorted_msgs = sorted(msgs, key=lambda x: x.get("sent_at") or "")
        last_in = None
        for m in sorted_msgs:
            ts_raw = m.get("sent_at")
            if not ts_raw:
                continue
            try:
                ts_dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if m.get("direction") == "inbound":
                last_in = ts_dt
            elif m.get("direction") == "outbound" and last_in:
                gap = (ts_dt - last_in).total_seconds() / 60
                if gap >= 0:
                    response_times.append(gap)
                last_in = None

    # Sentiment and treatment from analyses
    sentiments: list[str] = []
    treatment_scores: list[float] = []
    daily_scores: dict[str, list] = defaultdict(list)
    seven_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    lead_analysis: dict[str, list] = defaultdict(list)
    for a in analyses_raw:
        if a.get("lead_id"):
            lead_analysis[a["lead_id"]].append(a)
    for lid in agent_lead_ids:
        for a in lead_analysis.get(lid, []):
            if a.get("sentiment"):
                sentiments.append(a["sentiment"])
            if a.get("treatment_score") is not None:
                if not (a.get("source") == "maqsam" and a["treatment_score"] == 0):
                    treatment_scores.append(a["treatment_score"])
            ts_val = a.get("analyzed_at")
            score_val = a.get("treatment_score")
            if ts_val and score_val is not None and ts_val >= seven_ago:
                if not (a.get("source") == "maqsam" and score_val == 0):
                    daily_scores[ts_val[:10]].append(score_val)
    quality_trend = sorted(
        [{"date": d, "avg_score": round(sum(s)/len(s),1)} for d,s in daily_scores.items()],
        key=lambda x: x["date"],
    )

    # ── Build output lists ────────────────────────────────────────────────────
    calls_out = [
        {
            "lead_phone": lead_by_id.get(c.get("lead_id") or "", {}).get("phone"),
            "lead_name":  lead_by_id.get(c.get("lead_id") or "", {}).get("name"),
            "duration_seconds": c.get("duration_seconds") or 0,
            "answered": (c.get("duration_seconds") or 0) > 0,
            "called_at": c.get("called_at"),
        }
        for c in agent_calls
    ]

    by_lead_wa: dict[str, dict] = {}
    for m in agent_msgs:
        lid = m.get("lead_id")
        if not lid:
            continue
        if lid not in by_lead_wa:
            lead = lead_by_id.get(lid, {})
            by_lead_wa[lid] = {
                "lead_phone": lead.get("phone"), "lead_name": lead.get("name"),
                "messages_sent": 0, "messages_received": 0, "last_message_at": None,
            }
        entry = by_lead_wa[lid]
        if m.get("direction") == "outbound":
            entry["messages_sent"] += 1
        else:
            entry["messages_received"] += 1
        ts = m.get("sent_at")
        if ts and (not entry["last_message_at"] or ts > entry["last_message_at"]):
            entry["last_message_at"] = ts
    wa_leads_out = sorted(
        (v for v in by_lead_wa.values() if v["messages_sent"] > 0),
        key=lambda x: x["last_message_at"] or "", reverse=True
    )

    seen: set[str] = set()
    analyses_out = []
    for a in analyses_raw:
        lid = a.get("lead_id")
        if not lid or lid in seen:
            continue
        seen.add(lid)
        lead = lead_by_id.get(lid, {})
        analyses_out.append({
            "lead_phone":  lead.get("phone"),
            "lead_name":   lead.get("name"),
            "sentiment":   a.get("sentiment"),
            "treatment_score": a.get("treatment_score"),
            "outcome":     a.get("outcome"),
            "summary":     a.get("summary"),
            "risk_flags":  a.get("risk_flags") or [],
            "analyzed_at": a.get("analyzed_at"),
            "source":      a.get("source"),
        })
        if len(analyses_out) >= 30:
            break

    return {
        "agent": target,
        "range": range_,
        "stats": {
            "calls_handled":            calls_handled,
            "calls_total":              len(agent_calls),
            "wa_chats":                 wa_chats,
            "messages_sent":            messages_sent,
            "leads":                    len(agent_lead_ids),
            "avg_call_duration_seconds": avg_call_dur,
            "avg_response_time_min":    round(sum(response_times)/len(response_times),1) if response_times else 0,
            "avg_treatment_score":      round(sum(treatment_scores)/len(treatment_scores),1) if treatment_scores else 0,
            "sentiment": {
                "positive": sentiments.count("positive"),
                "neutral":  sentiments.count("neutral"),
                "negative": sentiments.count("negative"),
            },
            "quality_trend": quality_trend,
            "open_alerts": len(alerts_out),
        },
        "calls":    calls_out,
        "wa_leads": wa_leads_out,
        "analyses": analyses_out,
        "alerts":   alerts_out,
    }



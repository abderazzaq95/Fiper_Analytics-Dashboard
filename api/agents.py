from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import re
from pipeline.whatsapp import add_whatsapp_line_select, matches_business_line

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
BATCH_SIZE = 100


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
        normalized = _norm(value)
        if not normalized:
            continue
        if normalized.lower() in ("unknown", "team", "n/a"):
            continue
        if _looks_like_uuid(value):
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
):
    try:
        data = _agents_inner(range, wa_line)
        if not data.get("agents"):
            fallback = _agents_lightweight(range, wa_line)
            if fallback.get("agents"):
                return fallback
        return data
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/agents error ({range}): {e}", exc_info=True)
        try:
            return _agents_lightweight(range, wa_line)
        except Exception as fallback_error:
            logging.getLogger("fiper").error(
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
    lead_agent_map = {r["id"]: _norm(r.get("assigned_agent")) for r in lead_rows if r.get("id")}
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


def _agents_inner(range: str, wa_line: str = "all"):
    since = _since(range)

    messages = _paginate(lambda: supabase.table("messages").select(add_whatsapp_line_select("agent_name,direction,sent_at,lead_id")).gte("sent_at", since))
    calls    = _paginate(lambda: supabase.table("calls").select("agent_name,lead_id,duration_seconds,called_at").gte("called_at", since))
    alerts   = (
        supabase.table("alerts")
        .select("id,agent_name,lead_id,severity,type,message,resolved,created_at")
        .eq("resolved", False)
        .order("created_at", desc=True)
        .execute().data or []
    )
    # All-time call→lead→agent map for alert attribution (unfiltered by date)
    active_lead_ids = {
        row.get("lead_id")
        for row in [*messages, *calls, *alerts]
        if row.get("lead_id")
    }
    active_ids = list(active_lead_ids)
    leads = []
    idx = 0
    while idx < len(active_ids):
        leads.extend(
            supabase.table("leads")
            .select("id,phone,name,assigned_agent,status,score,channel,whatsapp_business_number")
            .in_("id", active_ids[idx:idx + BATCH_SIZE])
            .execute().data or []
        )
        idx += BATCH_SIZE
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        messages = [m for m in messages if matches_business_line(m, wa_line)]
        leads = [l for l in leads if matches_business_line(l, wa_line)]
        alert_lead_map = {l["id"]: l for l in leads if l.get("id")}
        alerts = [a for a in alerts if matches_business_line(alert_lead_map.get(a.get("lead_id") or ""), wa_line)]
    analyses = []
    idx = 0
    while idx < len(active_ids):
        analyses.extend(
            supabase.table("ai_analysis")
            .select("lead_id,sentiment,treatment_score,source,analyzed_at,outcome")
            .in_("lead_id", active_ids[idx:idx + BATCH_SIZE])
            .execute().data or []
        )
        idx += BATCH_SIZE

    # ── Build lookup dicts (all agent keys normalized to Title Case) ──────────

    # leads grouped by assigned_agent (WhatsApp path)
    agent_leads: dict[str, list] = defaultdict(list)
    for l in leads:
        ag = _norm(l.get("assigned_agent"))
        if ag:
            agent_leads[ag].append(l)

    # WhatsApp messages grouped by agent
    lead_by_id = {l["id"]: l for l in leads if l.get("id")}
    lead_agent_map = {l["id"]: _norm(l.get("assigned_agent")) for l in leads if l.get("id")}

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
    for m in messages:
        ag = _message_agent_name(
            m,
            lead_agent_map=lead_agent_map,
            lead_to_agent_fallback=lead_to_agent_fallback,
        )
        if ag:
            agent_msgs[ag].append(m)

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
    lead_agent_map = {l["id"]: _norm(l.get("assigned_agent")) for l in leads}

    # All-time lead → agent fallback (uses unfiltered calls so old leads are covered)
    lead_to_agent_fallback: dict[str, str] = {}
    for m in messages:
        lid = m.get("lead_id")
        ag = _norm(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in calls:
        lid = c.get("lead_id")
        ag = _norm(c.get("agent_name"))
        if lid and ag:
            lead_to_agent_fallback.setdefault(lid, ag)

    agent_alerts: dict[str, list] = defaultdict(list)
    for a in alerts:
        lid = a.get("lead_id")
        # 'unknown' / None agent names are meaningless — treat as missing so fallback fires
        raw_alert_agent = _norm(a.get("agent_name"))
        if raw_alert_agent and raw_alert_agent.lower() in ("unknown", "team", "n/a"):
            raw_alert_agent = None
        agent = _agent_label(
            raw_alert_agent,
            lead_agent_map.get(lid) if lid else None,
            lead_to_agent_fallback.get(lid) if lid else None,
        )
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

        outbound_msgs = [m for m in am if m.get("direction") == "outbound"]
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
def agent_alerts(agent: str = Query(...)):
    """Return open alerts attributed to one normalized agent.

    Uses the same attribution rules as the leaderboard: explicit alert agent,
    then lead assigned_agent, then all-time call/message fallback.
    """
    try:
        return _agent_alerts_inner(agent)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/agents/alerts error ({agent}): {e}", exc_info=True)
        return {"agent": _norm(agent) or agent, "alerts": []}


def _agent_alerts_inner(agent: str):
    target = _norm(agent)
    if not target:
        return {"agent": agent, "alerts": []}

    leads = _paginate(lambda: supabase.table("leads").select("id,phone,name,assigned_agent"))
    alerts = (
        supabase.table("alerts")
        .select("id,agent_name,lead_id,severity,type,message,resolved,created_at")
        .eq("resolved", False)
        .order("created_at", desc=True)
        .execute().data or []
    )
    messages = _paginate(lambda: supabase.table("messages").select("agent_name,direction,lead_id"))
    calls = _paginate(lambda: supabase.table("calls").select("agent_name,lead_id"))

    lead_by_id = {l["id"]: l for l in leads if l.get("id")}
    lead_agent_map = {l["id"]: _norm(l.get("assigned_agent")) for l in leads if l.get("id")}
    lead_to_agent_fallback: dict[str, str] = {}
    for m in messages:
        lid = m.get("lead_id")
        ag = _norm(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in calls:
        lid = c.get("lead_id")
        ag = _norm(c.get("agent_name"))
        if lid and ag:
            lead_to_agent_fallback.setdefault(lid, ag)

    rows = []
    for alert in alerts:
        lid = alert.get("lead_id")
        raw_alert_agent = _norm(alert.get("agent_name"))
        if raw_alert_agent and raw_alert_agent.lower() in ("unknown", "team", "n/a"):
            raw_alert_agent = None
        attributed_agent = _agent_label(
            raw_alert_agent,
            lead_agent_map.get(lid) if lid else None,
            lead_to_agent_fallback.get(lid) if lid else None,
        )
        if attributed_agent != target:
            continue
        lead = lead_by_id.get(lid, {})
        rows.append({
            "id": alert.get("id"),
            "lead_id": lid,
            "lead_phone": lead.get("phone"),
            "lead_name": lead.get("name"),
            "severity": alert.get("severity") or "MED",
            "type": alert.get("type") or "alert",
            "message": alert.get("message") or "",
            "created_at": alert.get("created_at"),
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
    target = _norm(agent)
    if not target:
        return {"agent": agent, "stats": {}, "calls": [], "wa_leads": [], "analyses": [], "alerts": []}

    since = _since(range_)

    # ── Calls for this agent in range (no .order() — avoids pagination issues) ──
    raw_calls = _paginate(
        lambda: supabase.table("calls")
        .select("lead_id,duration_seconds,called_at,agent_name")
        .gte("called_at", since)
    )
    # Calls attributed through explicit name first, then lead-based fallback

    # ── Messages for this agent in range ─────────────────────────────────────
    raw_msgs = _paginate(
        lambda: supabase.table("messages")
        .select(add_whatsapp_line_select("agent_name,direction,sent_at,lead_id"))
        .gte("sent_at", since)
    )

    # ── Collect lead IDs: call leads (unfiltered) + message leads ─────────────
    call_lead_ids: set[str] = {c["lead_id"] for c in agent_calls if c.get("lead_id")}
    msg_lead_ids_all: set[str] = {m["lead_id"] for m in raw_msgs if m.get("lead_id")}
    all_lead_ids = list(call_lead_ids | msg_lead_ids_all)

    # Fetch all lead details; keep Maqsam leads regardless of WA line filter
    leads_raw: list[dict] = []
    idx = 0
    while idx < len(all_lead_ids):
        leads_raw.extend(
            supabase.table("leads")
            .select("id,phone,name,assigned_agent,status,score,channel,whatsapp_business_number")
            .in_("id", all_lead_ids[idx:idx + BATCH_SIZE])
            .execute().data or []
        )
        idx += BATCH_SIZE

    # No WA-line filter here: the agent profile shows ALL the agent's activity
    # across every line. Filtering by line would exclude leads from lead_by_id
    # and break message attribution for WA-only agents.

    lead_by_id = {l["id"]: l for l in leads_raw if l.get("id")}
    lead_agent_map = {l["id"]: _norm(l.get("assigned_agent")) for l in leads_raw if l.get("id")}

    # Build fallback map for message attribution
    lead_to_agent_fallback: dict[str, str] = {}
    for m in raw_msgs:
        lid = m.get("lead_id"); ag = _norm(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in raw_calls:
        lid = c.get("lead_id"); ag = _agent_label(c.get("agent_name"))
        if lid and ag:
            lead_to_agent_fallback.setdefault(lid, ag)

    def _call_agent_name(row: dict) -> str | None:
        raw_ag = row.get("agent_name")
        explicit = _norm(raw_ag)
        if explicit and not _looks_like_uuid(raw_ag):
            return explicit
        lid = row.get("lead_id")
        if lid:
            return lead_agent_map.get(lid) or lead_to_agent_fallback.get(lid)
        return None

    agent_calls = [c for c in raw_calls if _call_agent_name(c) == target]
    agent_calls.sort(key=lambda x: x.get("called_at") or "", reverse=True)

    # Messages attributed to this agent
    agent_msgs = [
        m for m in raw_msgs
        if _message_agent_name(m, lead_agent_map=lead_agent_map, lead_to_agent_fallback=lead_to_agent_fallback) == target
    ]

    # Lead IDs attributed to this agent
    agent_lead_ids: set[str] = (
        {l["id"] for l in leads_raw if _norm(l.get("assigned_agent")) == target}
        | {c["lead_id"] for c in agent_calls if c.get("lead_id")}
        | {m["lead_id"] for m in agent_msgs if m.get("lead_id")}
    )

    # ── AI Analyses ───────────────────────────────────────────────────────────
    analyses_raw: list[dict] = []
    aid_list = list(agent_lead_ids)
    idx = 0
    while idx < len(aid_list):
        analyses_raw.extend(
            supabase.table("ai_analysis")
            .select("lead_id,sentiment,treatment_score,outcome,summary,summary_en,summary_ar,risk_flags,analyzed_at,source")
            .in_("lead_id", aid_list[idx:idx + BATCH_SIZE])
            .execute().data or []
        )
        idx += BATCH_SIZE
    analyses_raw.sort(key=lambda x: x.get("analyzed_at") or "", reverse=True)

    # ── Alerts ────────────────────────────────────────────────────────────────
    alerts_raw = (
        supabase.table("alerts")
        .select("id,agent_name,lead_id,severity,type,message,resolved,created_at")
        .eq("resolved", False)
        .order("created_at", desc=True)
        .execute().data or []
    )
    # Need full lead table for alert attribution
    all_leads_fa = _paginate(lambda: supabase.table("leads").select("id,phone,name,assigned_agent"))
    lbi_full = {l["id"]: l for l in all_leads_fa if l.get("id")}
    lag_full = {l["id"]: _norm(l.get("assigned_agent")) for l in all_leads_fa if l.get("id")}
    fb_full: dict[str, str] = {}
    for m in raw_msgs:
        lid = m.get("lead_id"); ag = _norm(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            fb_full.setdefault(lid, ag)
    for c in raw_calls:
        lid = c.get("lead_id"); ag = _agent_label(c.get("agent_name"))
        if lid and ag:
            fb_full.setdefault(lid, ag)

    alerts_out = []
    for a in alerts_raw:
        lid = a.get("lead_id")
        raw_ag = _norm(a.get("agent_name"))
        if raw_ag and raw_ag.lower() in ("unknown", "team", "n/a"):
            raw_ag = None
        attributed = _agent_label(
            raw_ag,
            lag_full.get(lid) if lid else None,
            fb_full.get(lid) if lid else None,
        )
        if attributed != target:
            continue
        lead = lbi_full.get(lid, {})
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
        for c in agent_calls[:50]
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
    wa_leads_out = sorted(by_lead_wa.values(), key=lambda x: x["last_message_at"] or "", reverse=True)[:50]

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
            "summary_en":  a.get("summary_en") or a.get("summary"),
            "summary_ar":  a.get("summary_ar") or a.get("summary"),
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



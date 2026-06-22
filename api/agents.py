from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pipeline.whatsapp import matches_business_line

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
        .select("agent_name,direction,sent_at,lead_id")
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
                .select("id,channel")
                .in_("id", alert_lead_ids[idx:idx + 100])
                .execute().data or []
            )
        lead_map = {r["id"]: r for r in lead_rows}
        alerts = [a for a in alerts if matches_business_line(lead_map.get(a.get("lead_id") or ""), wa_line)]

    agents = sorted({
        *(_norm(c.get("agent_name")) for c in calls if _norm(c.get("agent_name"))),
        *(_norm(m.get("agent_name")) for m in messages if _norm(m.get("agent_name"))),
        *(_norm(a.get("agent_name")) for a in alerts if _norm(a.get("agent_name"))),
    })

    rows = []
    for agent in agents:
        agent_calls = [c for c in calls if _norm(c.get("agent_name")) == agent]
        agent_msgs = [m for m in messages if _norm(m.get("agent_name")) == agent]
        agent_alerts = [
            a for a in alerts
            if _norm(a.get("agent_name")) == agent
            and (a.get("agent_name") or "").lower() not in ("team", "unknown")
        ]
        completed_calls = [c for c in agent_calls if (c.get("duration_seconds") or 0) > 0]
        lead_ids = {
            item.get("lead_id")
            for item in [*agent_calls, *agent_msgs, *agent_alerts]
            if item.get("lead_id")
        }
        rows.append({
            "agent": agent,
            "leads": len(lead_ids),
            "calls_handled": len(completed_calls),
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

    messages = _paginate(lambda: supabase.table("messages").select("agent_name,direction,sent_at,lead_id").gte("sent_at", since))
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
            .select("id,phone,name,assigned_agent,status,score,channel")
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
    agent_msgs: dict[str, list] = defaultdict(list)
    for m in messages:
        ag = _norm(m.get("agent_name"))
        if ag:
            agent_msgs[ag].append(m)

    # Maqsam calls grouped by agent + their lead_ids
    agent_calls: dict[str, list] = defaultdict(list)
    call_lead_ids_by_agent: dict[str, set] = defaultdict(set)
    for c in calls:
        ag = _norm(c.get("agent_name"))
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
        agent = (
            raw_alert_agent
            or (lead_agent_map.get(lid) if lid else None)
            or (lead_to_agent_fallback.get(lid) if lid else None)
        )
        if agent and agent.lower() not in ("team", "unknown"):
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

        result.append({
            "agent":                    agent,
            "leads":                    total_leads,
            "calls_handled":            calls_answered,
            "avg_call_duration_seconds": avg_call_dur,
            "avg_response_time_min":    round(sum(response_times) / len(response_times), 1) if response_times else 0,
            "avg_treatment_score":      round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
            "sentiment":                sentiment_summary,
            "open_alerts":              len(agent_alerts.get(agent, [])),
            "alert_details":            agent_alerts.get(agent, []),
            "quality_trend":            quality_trend,
            # Conversion data intentionally omitted — requires CRM integration
        })

    result.sort(key=lambda x: x["calls_handled"], reverse=True)
    if not result:
        fallback_agents = set()
        fallback_agents.update(_norm(c.get("agent_name")) for c in calls if _norm(c.get("agent_name")))
        fallback_agents.update(_norm(m.get("agent_name")) for m in messages if _norm(m.get("agent_name")))
        fallback_agents.update(agent_alerts.keys())
        for agent in fallback_agents:
            ac = [c for c in calls if _norm(c.get("agent_name")) == agent]
            am = [m for m in messages if _norm(m.get("agent_name")) == agent]
            lead_ids = {
                row.get("lead_id")
                for row in [*ac, *am]
                if row.get("lead_id")
            }
            completed_calls = [c for c in ac if (c.get("duration_seconds") or 0) > 0]
            result.append({
                "agent": agent,
                "leads": len(lead_ids),
                "calls_handled": len(completed_calls),
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
        attributed_agent = (
            raw_alert_agent
            or (lead_agent_map.get(lid) if lid else None)
            or (lead_to_agent_fallback.get(lid) if lid else None)
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

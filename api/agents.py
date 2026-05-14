from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import defaultdict

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))


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
    deltas = {"7d": timedelta(days=7), "30d": timedelta(days=30)}
    return (now - deltas.get(range_, timedelta(days=7))).isoformat()


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
def agents(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    leads    = _paginate(lambda: supabase.table("leads").select("id,assigned_agent,status,score"))
    messages = _paginate(lambda: supabase.table("messages").select("agent_name,direction,sent_at,lead_id").gte("sent_at", since))
    calls    = _paginate(lambda: supabase.table("calls").select("agent_name,lead_id,duration_seconds,called_at").gte("called_at", since))
    analyses = _paginate(lambda: supabase.table("ai_analysis").select("lead_id,sentiment,treatment_score,source,analyzed_at,outcome"))
    alerts   = supabase.table("alerts").select("agent_name,lead_id,severity,resolved").eq("resolved", False).execute().data or []
    # All-time call→lead→agent map for alert attribution (unfiltered by date)
    all_calls_for_alerts = _paginate(lambda: supabase.table("calls").select("agent_name,lead_id"))

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
    lead_agent_map = {l["id"]: _norm(l.get("assigned_agent")) for l in leads}

    # All-time lead → agent fallback (uses unfiltered calls so old leads are covered)
    lead_to_agent_fallback: dict[str, str] = {}
    for m in messages:
        lid = m.get("lead_id")
        ag = _norm(m.get("agent_name"))
        if lid and m.get("direction") == "outbound" and ag:
            lead_to_agent_fallback.setdefault(lid, ag)
    for c in all_calls_for_alerts:
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
            agent_alerts[agent].append(a)

    # All agents: WhatsApp (leads + messages) + Maqsam (calls)
    all_agents = set(agent_leads.keys()) | set(agent_msgs.keys()) | set(agent_calls.keys())

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

        outbound_msgs  = [m for m in am if m["direction"] == "outbound"]
        calls_answered = len([c for c in ac if (c.get("duration_seconds") or 0) > 0])

        # Fix 3: msgs_sent — WhatsApp outbound when available, else calls_handled
        # Outbound messages are 0 for Maqsam agents (phone-only); show calls instead
        messages_sent = len(outbound_msgs) if outbound_msgs else calls_answered

        # Response times (WhatsApp inbound → outbound gap)
        by_lead: dict[str, list] = defaultdict(list)
        for m in am:
            by_lead[m["lead_id"]].append(m)
        response_times = []
        for msgs in by_lead.values():
            sorted_msgs = sorted(msgs, key=lambda x: x["sent_at"])
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
            "agent":                  agent,
            "leads":                  total_leads,
            "converted":              converted,
            # Fix 2: CVR denominator = total_leads (all leads touched, not just assigned)
            "conversion_rate":        round(converted / total_leads * 100, 1) if total_leads else 0,
            "messages_sent":          messages_sent,
            "calls_handled":          calls_answered,
            "avg_response_time_min":  round(sum(response_times) / len(response_times), 1) if response_times else 0,
            "avg_treatment_score":    round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
            "sentiment":              sentiment_summary,
            "open_alerts":            len(agent_alerts.get(agent, [])),
            "quality_trend":          quality_trend,
        })

    result.sort(key=lambda x: x["converted"], reverse=True)
    return {"range": range, "agents": result}

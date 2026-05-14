from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import defaultdict

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    deltas = {"today": timedelta(days=1), "7d": timedelta(days=7), "30d": timedelta(days=30)}
    return (now - deltas.get(range_, timedelta(days=7))).isoformat()


def _norm(name: str | None) -> str | None:
    """Normalize agent name: strip whitespace + Title Case.
    Collapses duplicates caused by casing differences between ManyContacts
    and Maqsam (e.g. 'Ali rizk' == 'Ali Rizk', 'Ayman delbani' == 'Ayman Delbani').
    """
    return name.strip().title() if name else None


@router.get("/api/agents")
def agents(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    leads    = supabase.table("leads").select("id,assigned_agent,status,score").execute().data or []
    messages = supabase.table("messages").select("agent_name,direction,sent_at,lead_id").gte("sent_at", since).execute().data or []
    calls    = supabase.table("calls").select("agent_name,lead_id,duration_seconds,called_at").gte("called_at", since).execute().data or []
    analyses = supabase.table("ai_analysis").select("lead_id,sentiment,treatment_score,source").execute().data or []
    alerts   = supabase.table("alerts").select("agent_name,lead_id,severity,resolved").eq("resolved", False).execute().data or []

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

    # Fallback: find agent from outbound messages or calls when lead has no assigned_agent
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
        agent = (
            _norm(a.get("agent_name"))
            or (lead_agent_map.get(lid) if lid else None)
            or (lead_to_agent_fallback.get(lid) if lid else None)
        )
        # Skip team-level alerts (weak_engagement) — not attributed to a specific agent
        if agent and agent.lower() != "team":
            agent_alerts[agent].append(a)

    # All agents: WhatsApp (leads + messages) + Maqsam (calls)
    all_agents = set(agent_leads.keys()) | set(agent_msgs.keys()) | set(agent_calls.keys())

    result = []
    for agent in all_agents:
        al = agent_leads.get(agent, [])
        am = agent_msgs.get(agent, [])
        ac = agent_calls.get(agent, [])

        converted = sum(1 for l in al if l.get("status") == "converted")
        outbound_msgs = [m for m in am if m["direction"] == "outbound"]

        # Response times (WhatsApp)
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
        # Lead IDs via: assigned_agent on leads + calls handled in this period
        analysis_lead_ids = {l["id"] for l in al} | call_lead_ids_by_agent.get(agent, set())

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

        result.append({
            "agent":                  agent,
            "leads":                  len(al),
            "converted":              converted,
            "conversion_rate":        round(converted / len(al) * 100, 1) if al else 0,
            "messages_sent":          len(outbound_msgs),
            "calls_handled":          len([c for c in ac if (c.get("duration_seconds") or 0) > 0]),
            "avg_response_time_min":  round(sum(response_times) / len(response_times), 1) if response_times else 0,
            "avg_treatment_score":    round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
            "sentiment":              sentiment_summary,
            "open_alerts":            len(agent_alerts.get(agent, [])),
        })

    result.sort(key=lambda x: x["converted"], reverse=True)
    return {"range": range, "agents": result}

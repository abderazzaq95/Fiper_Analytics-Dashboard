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


@router.get("/api/agents")
def agents(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    leads = supabase.table("leads").select("id,assigned_agent,status,score").execute().data
    messages = supabase.table("messages").select("agent_name,direction,sent_at,lead_id").gte("sent_at", since).execute().data
    analyses = supabase.table("ai_analysis").select("lead_id,sentiment,treatment_score").execute().data
    alerts = supabase.table("alerts").select("agent_name,severity,resolved").eq("resolved", False).execute().data

    agent_leads: dict[str, list] = defaultdict(list)
    for l in leads:
        if l.get("assigned_agent"):
            agent_leads[l["assigned_agent"]].append(l)

    agent_msgs: dict[str, list] = defaultdict(list)
    for m in messages:
        if m.get("agent_name"):
            agent_msgs[m["agent_name"]].append(m)

    lead_analysis: dict[str, list] = defaultdict(list)
    for a in analyses:
        lead_analysis[a["lead_id"]].append(a)

    lead_agent_map = {l["id"]: l.get("assigned_agent") for l in leads}
    agent_alerts: dict[str, list] = defaultdict(list)
    for a in alerts:
        agent = a.get("agent_name")
        if not agent and a.get("lead_id"):
            agent = lead_agent_map.get(a["lead_id"])
        if agent:
            agent_alerts[agent].append(a)

    result = []
    all_agents = set(agent_leads.keys()) | set(agent_msgs.keys())

    for agent in all_agents:
        al = agent_leads.get(agent, [])
        am = agent_msgs.get(agent, [])
        converted = sum(1 for l in al if l.get("status") == "converted")

        inbound_msgs = [m for m in am if m["direction"] == "inbound"]
        outbound_msgs = [m for m in am if m["direction"] == "outbound"]

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

        agent_lead_ids = {l["id"] for l in al}
        sentiments = []
        treatment_scores = []
        for lid in agent_lead_ids:
            for a in lead_analysis.get(lid, []):
                if a.get("sentiment"):
                    sentiments.append(a["sentiment"])
                if a.get("treatment_score") is not None:
                    treatment_scores.append(a["treatment_score"])

        sentiment_summary = {
            "positive": sentiments.count("positive"),
            "neutral": sentiments.count("neutral"),
            "negative": sentiments.count("negative"),
        }

        result.append({
            "agent": agent,
            "leads": len(al),
            "converted": converted,
            "conversion_rate": round(converted / len(al) * 100, 1) if al else 0,
            "messages_sent": len(outbound_msgs),
            "avg_response_time_min": round(sum(response_times) / len(response_times), 1) if response_times else 0,
            "avg_treatment_score": round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
            "sentiment": sentiment_summary,
            "open_alerts": len(agent_alerts.get(agent, [])),
        })

    result.sort(key=lambda x: x["converted"], reverse=True)
    return {"range": range, "agents": result}

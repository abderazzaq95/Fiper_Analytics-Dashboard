from fastapi import APIRouter, Query, HTTPException
from supabase import create_client
from collections import defaultdict
import os
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

_OUTCOME_COLOR = {
    "completed": "green",
    "no_answer": "orange",
    "busy":      "orange",
    "abandoned": "orange",
    "failed":    "red",
    "in_progress": "blue",
}
_SEV_COLOR = {"HIGH": "red", "MED": "orange", "LOW": "blue"}


def _dur(seconds: int | None) -> str:
    if not seconds:
        return "0s"
    if seconds < 60:
        return f"{seconds}s"
    return f"{seconds // 60}m {seconds % 60}s"


@router.get("/api/leads/journey")
def leads_journey(limit: int = Query(10, ge=1, le=50)):
    # ── Top leads by score ────────────────────────────────────────────────────
    leads = (
        supabase.table("leads")
        .select("id,phone,name,score,status,assigned_agent,channel,last_message_at")
        .gt("score", 0)
        .order("score", desc=True)
        .limit(limit)
        .execute()
        .data or []
    )

    if not leads:
        return {"leads": []}

    lead_ids  = [l["id"] for l in leads]
    lead_phone = {l["id"]: l.get("phone") for l in leads}

    # ── Calls (direct lead_id match) ──────────────────────────────────────────
    calls = (
        supabase.table("calls")
        .select("lead_id,agent_name,duration_seconds,outcome,called_at,maqsam_sentiment,summary_en")
        .in_("lead_id", lead_ids)
        .order("called_at")
        .execute()
        .data or []
    )

    # ── Messages: cross-channel phone lookup ──────────────────────────────────
    # Top leads are often Maqsam-channel (wa_contact_id=phone). Their WA
    # messages live under a different lead_id (the WhatsApp lead for the same
    # phone). We find all lead_ids that share a phone with our top leads, then
    # query messages across the full set, mapping results back by phone.
    phones = list({p for p in lead_phone.values() if p})
    cross_leads: list[dict] = []
    if phones:
        cross_leads = (
            supabase.table("leads")
            .select("id,phone")
            .in_("phone", phones)
            .execute()
            .data or []
        )
    msg_lead_phone  = {cl["id"]: cl.get("phone") for cl in cross_leads}
    all_msg_lead_ids = [cl["id"] for cl in cross_leads if cl.get("id")]

    messages: list[dict] = []
    if all_msg_lead_ids:
        messages = (
            supabase.table("messages")
            .select("lead_id,direction,body,sent_at,agent_name")
            .in_("lead_id", all_msg_lead_ids)
            .order("sent_at")
            .execute()
            .data or []
        )

    # ── AI analysis + alerts (direct lead_id match) ───────────────────────────
    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,treatment_score,sentiment,topics,outcome,risk_flags,analyzed_at,source")
        .in_("lead_id", lead_ids)
        .order("analyzed_at")
        .execute()
        .data or []
    )
    alerts = (
        supabase.table("alerts")
        .select("lead_id,severity,type,message,created_at,resolved")
        .in_("lead_id", lead_ids)
        .order("created_at")
        .execute()
        .data or []
    )

    # ── Group by lead_id ──────────────────────────────────────────────────────
    calls_by  = defaultdict(list)
    msgs_by   = defaultdict(list)
    ana_by    = defaultdict(list)
    alerts_by = defaultdict(list)

    for c in calls:
        if c.get("lead_id"):
            calls_by[c["lead_id"]].append(c)

    # Cross-channel: map each message back to the top lead via shared phone
    for m in messages:
        msg_lid = m.get("lead_id")
        if not msg_lid:
            continue
        phone = msg_lead_phone.get(msg_lid)
        if not phone:
            continue
        for top_lid, top_phone in lead_phone.items():
            if top_phone == phone:
                msgs_by[top_lid].append(m)

    for a in analyses:
        if a.get("lead_id"):
            ana_by[a["lead_id"]].append(a)
    for al in alerts:
        if al.get("lead_id"):
            alerts_by[al["lead_id"]].append(al)

    # ── Build timeline per lead ───────────────────────────────────────────────
    result = []
    for lead in leads:
        lid   = lead["id"]
        phone = lead.get("phone") or "—"
        timeline = []

        # All calls in chronological order
        for i, c in enumerate(calls_by[lid]):
            outcome = (c.get("outcome") or "unknown").lower()
            dur     = c.get("duration_seconds") or 0
            timeline.append({
                "type":      "call",
                "date":      c.get("called_at"),
                "color":     _OUTCOME_COLOR.get(outcome, "gray"),
                "title":     "First Call" if i == 0 else f"Call #{i + 1}",
                "agent":     c.get("agent_name") or "—",
                "duration":  _dur(dur),
                "outcome":   outcome,
                "summary":   c.get("summary_en") or "",
                "sentiment": c.get("maqsam_sentiment") or "",
            })

        # WhatsApp messages — first inbound + first outbound
        all_msgs = msgs_by[lid]
        inbound  = [m for m in all_msgs if m.get("direction") == "inbound"]
        outbound = [m for m in all_msgs if m.get("direction") == "outbound"]

        if inbound:
            m = inbound[0]
            timeline.append({
                "type":      "whatsapp",
                "date":      m.get("sent_at"),
                "color":     "blue",
                "title":     "First WhatsApp Message",
                "direction": "inbound",
                "preview":   m.get("body") or "",
                "total":     len(all_msgs),
            })
        if outbound:
            m = outbound[0]
            timeline.append({
                "type":      "whatsapp",
                "date":      m.get("sent_at"),
                "color":     "blue",
                "title":     "First Agent Reply",
                "direction": "outbound",
                "agent":     m.get("agent_name") or "—",
                "preview":   m.get("body") or "",
            })

        # AI analysis events
        for a in ana_by[lid]:
            ts    = a.get("treatment_score") or 0
            color = "green" if ts >= 70 else "orange" if ts >= 40 else "red"
            timeline.append({
                "type":       "ai",
                "date":       a.get("analyzed_at"),
                "color":      color,
                "title":      "AI Analysis",
                "score":      ts,
                "sentiment":  a.get("sentiment") or "neutral",
                "topics":     a.get("topics") or [],
                "outcome":    a.get("outcome") or "",
                "risk_flags": a.get("risk_flags") or [],
                "source":     a.get("source") or "",
            })

        # Alerts
        for al in alerts_by[lid]:
            sev = al.get("severity") or "MED"
            timeline.append({
                "type":     "alert",
                "date":     al.get("created_at"),
                "color":    _SEV_COLOR.get(sev, "orange"),
                "title":    (al.get("type") or "alert").replace("_", " ").title(),
                "severity": sev,
                "message":  al.get("message") or "",
                "resolved": al.get("resolved") or False,
            })

        # Account status marker (always last)
        is_converted = lead.get("status") == "converted"
        timeline.append({
            "type":   "account",
            "date":   None,
            "color":  "green" if is_converted else "gray",
            "title":  "Account Opened",
            "status": lead.get("status") or "pending",
        })

        timeline.sort(key=lambda x: (x.get("date") is None, x.get("date") or ""))

        total_calls     = len(calls_by[lid])
        answered_calls  = sum(1 for c in calls_by[lid] if (c.get("outcome") or "") == "completed")
        latest_analysis = ana_by[lid][-1] if ana_by[lid] else None

        result.append({
            "id":             lid,
            "phone":          phone,
            "name":           lead.get("name") or "",
            "score":          lead.get("score") or 0,
            "status":         lead.get("status") or "new",
            "assigned_agent": lead.get("assigned_agent") or "—",
            "channel":        lead.get("channel") or "—",
            "total_calls":    total_calls,
            "answered_calls": answered_calls,
            "total_messages": len(all_msgs),
            "last_sentiment": latest_analysis.get("sentiment") if latest_analysis else None,
            "open_alerts":    sum(1 for al in alerts_by[lid] if not al.get("resolved")),
            "account_opened": None,
            "timeline":       timeline,
        })

    return {"leads": result}


@router.post("/api/leads/{lead_id}/convert")
def convert_lead(lead_id: str):
    result = (
        supabase.table("leads")
        .update({"status": "converted"})
        .eq("id", lead_id)
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Lead not found")
    return {"success": True, "lead_id": lead_id, "status": "converted"}

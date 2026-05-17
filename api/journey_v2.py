from fastapi import APIRouter, Query
from supabase import create_client
from collections import defaultdict
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

_CC_PREFIXES = sorted([
    ('9665', 'SA'), ('968', 'OM'), ('966', 'SA'), ('971', 'AE'),
    ('965', 'KW'), ('964', 'IQ'), ('962', 'JO'), ('212', 'MA'),
    ('249', 'SD'), ('20',  'EG'), ('90',  'TR'), ('44',  'GB'),
    ('7',   'RU'), ('1',   'US'),
], key=lambda x: -len(x[0]))

_OUTCOME_COLOR = {
    'completed': 'green', 'no_answer': 'orange', 'busy': 'orange',
    'abandoned': 'orange', 'failed': 'red', 'in_progress': 'blue',
}
_SEV_COLOR = {'HIGH': 'red', 'MED': 'orange', 'LOW': 'blue'}
_STATUS_RANK = {'converted': 5, 'callback': 4, 'engaged': 3, 'new': 2, 'lost': 1}


def _detect_cc(phone: str) -> str:
    if not phone:
        return 'OTHER'
    p = phone.replace('+', '').replace(' ', '').replace('-', '')
    if p.startswith('00'):
        p = p[2:]
    for prefix, code in _CC_PREFIXES:
        if p.startswith(prefix):
            return code
    return 'OTHER'


def _paginate(build_query) -> list:
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _dur(seconds) -> str:
    if not seconds:
        return '0s'
    s = int(seconds)
    if s < 60:
        return f'{s}s'
    return f'{s // 60}m {s % 60}s'


def _merge_by_phone(raw_leads: list[dict]) -> list[dict]:
    """Group leads sharing the same phone into one merged record."""
    phone_groups: dict[str, list] = defaultdict(list)
    no_phone: list[dict] = []
    for lead in raw_leads:
        phone = lead.get("phone")
        if phone:
            phone_groups[phone].append(lead)
        else:
            no_phone.append(lead)

    merged: list[dict] = []
    for phone, group in phone_groups.items():
        if len(group) == 1:
            lead = dict(group[0])
            lead["lead_ids"] = [lead["id"]]
            lead["channels"] = [lead.get("channel") or ""]
            merged.append(lead)
            continue

        # Multiple leads share this phone → merge into one card
        best_score = max(l.get("score") or 0 for l in group)
        best_status = max(
            (l.get("status") or "new" for l in group),
            key=lambda s: _STATUS_RANK.get(s, 0),
        )
        channels = sorted({l.get("channel") for l in group if l.get("channel")})
        last_msg = max((l.get("last_message_at") or "" for l in group)) or None
        name = next((l.get("name") for l in group if l.get("name")), None)
        agent = next((l.get("assigned_agent") for l in group if l.get("assigned_agent")), None)
        # Prefer maqsam lead as primary (has calls); else first in group
        primary = next((l for l in group if l.get("channel") == "maqsam"), group[0])

        merged.append({
            "id": primary["id"],
            "lead_ids": [l["id"] for l in group],
            "phone": phone,
            "name": name,
            "score": best_score,
            "status": best_status,
            "assigned_agent": agent,
            "channel": "+".join(channels),
            "channels": channels,
            "last_message_at": last_msg,
        })

    # Leads with no phone are kept as-is (single-channel, no merge possible)
    for lead in no_phone:
        lead = dict(lead)
        lead["lead_ids"] = [lead["id"]]
        lead["channels"] = [lead.get("channel") or ""]
        merged.append(lead)

    return merged


@router.get("/api/leads/journey/v2")
def leads_journey_v2(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    phone_search: str = Query(""),
    country: str = Query(""),
    min_score: int = Query(0, ge=0, le=100),
    max_score: int = Query(100, ge=0, le=100),
    channel: str = Query(""),
    outcome: str = Query(""),
    agent: str = Query(""),
    high_risk_only: bool = Query(False),
    sort_by: str = Query("score"),
    lang: str = Query("en"),
):
    try:
        return _inner(page, limit, phone_search, country,
                      min_score, max_score, channel, outcome,
                      agent, high_risk_only, sort_by, lang)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/leads/journey/v2 error: {e}", exc_info=True)
        return {"leads": [], "total": 0, "page": page, "pages": 0, "meta": {"agents": []}}


def _inner(page, limit, phone_search, country, min_score, max_score,
           channel, outcome, agent, high_risk_only, sort_by, lang="en"):
    since_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    # ── 1. Collect active lead IDs from last 7 days ───────────────────────────
    call_rows = _paginate(
        lambda: supabase.table("calls").select("lead_id,called_at").gte("called_at", since_7d)
    )
    call_lead_ids: set[str] = set()
    call_counts: dict[str, int] = defaultdict(int)
    for c in call_rows:
        lid = c.get("lead_id")
        if lid:
            call_lead_ids.add(lid)
            call_counts[lid] += 1

    msg_lead_rows = (
        supabase.table("leads").select("id")
        .gte("last_message_at", since_7d)
        .execute().data or []
    )
    msg_lead_ids = {r["id"] for r in msg_lead_rows}
    all_active_ids = list(call_lead_ids | msg_lead_ids)

    if not all_active_ids:
        return {"leads": [], "total": 0, "page": page, "pages": 1, "meta": {"agents": []}}

    # ── 2. Fetch lead details ─────────────────────────────────────────────────
    raw_leads: list[dict] = []
    for batch in _chunks(all_active_ids, 500):
        raw_leads += (
            supabase.table("leads")
            .select("id,phone,name,score,status,assigned_agent,channel,last_message_at")
            .in_("id", batch)
            .execute().data or []
        )

    # ── 3. Merge leads sharing the same phone into one card ───────────────────
    all_leads = _merge_by_phone(raw_leads)

    # Build reverse map: every raw lead_id → its merged primary lead_id
    raw_to_primary: dict[str, str] = {}
    for merged in all_leads:
        for sid in merged["lead_ids"]:
            raw_to_primary[sid] = merged["id"]

    # ── 4. Fetch ai_analysis for all raw lead IDs ─────────────────────────────
    raw_lead_ids = [l["id"] for l in raw_leads]
    analyses_raw: list[dict] = []
    for batch in _chunks(raw_lead_ids, 500):
        analyses_raw += (
            supabase.table("ai_analysis")
            .select("lead_id,outcome,risk_flags,sentiment,treatment_score,topics,summary,analyzed_at,source")
            .in_("lead_id", batch)
            .execute().data or []
        )

    # Map analyses to merged primary IDs
    analysis_by_lead: dict[str, list] = defaultdict(list)
    for a in analyses_raw:
        raw_lid = a.get("lead_id")
        if raw_lid:
            primary = raw_to_primary.get(raw_lid, raw_lid)
            analysis_by_lead[primary].append(a)

    # ── 5. Apply filters ──────────────────────────────────────────────────────
    filtered = list(all_leads)

    if phone_search:
        filtered = [l for l in filtered if phone_search in (l.get("phone") or "")]

    if country:
        _countries = {c.strip() for c in country.split(',') if c.strip()}
        filtered = [l for l in filtered if _detect_cc(l.get("phone")) in _countries]

    if min_score > 0 or max_score < 100:
        filtered = [l for l in filtered if min_score <= (l.get("score") or 0) <= max_score]

    if channel == "both":
        filtered = [l for l in filtered
                    if "maqsam" in l.get("channels", []) and "whatsapp" in l.get("channels", [])]
    elif channel:
        filtered = [l for l in filtered if channel in l.get("channels", [])]

    if outcome:
        _outcomes = {o.strip() for o in outcome.split(',') if o.strip()}
        filtered = [
            l for l in filtered
            if any(a.get("outcome") in _outcomes for a in analysis_by_lead.get(l["id"], []))
        ]

    if agent:
        _agents = {a.strip() for a in agent.split(',') if a.strip()}
        def _agent_match(lead):
            la = lead.get("assigned_agent")
            return ("__unassigned__" in _agents and not la) or (la in _agents)
        filtered = [l for l in filtered if _agent_match(l)]

    if high_risk_only:
        filtered = [
            l for l in filtered
            if any(bool(a.get("risk_flags")) for a in analysis_by_lead.get(l["id"], []))
        ]

    # ── 6. Sort ───────────────────────────────────────────────────────────────
    if sort_by == "score":
        filtered.sort(key=lambda l: l.get("score") or 0, reverse=True)
    elif sort_by == "recent":
        filtered.sort(key=lambda l: l.get("last_message_at") or "", reverse=True)
    elif sort_by == "waiting":
        filtered.sort(key=lambda l: l.get("last_message_at") or "")
    elif sort_by == "calls":
        filtered.sort(
            key=lambda l: sum(call_counts.get(sid, 0) for sid in l["lead_ids"]),
            reverse=True,
        )

    # ── 7. Paginate ───────────────────────────────────────────────────────────
    total = len(filtered)
    pages = max(1, (total + limit - 1) // limit)
    page = min(max(page, 1), pages)
    page_leads = filtered[(page - 1) * limit: page * limit]

    all_agents = sorted({l["assigned_agent"] for l in all_leads if l.get("assigned_agent")})

    if not page_leads:
        return {"leads": [], "total": total, "page": page, "pages": pages,
                "meta": {"agents": all_agents}}

    # ── 8. Build timelines for this page ──────────────────────────────────────
    # Expand each merged lead to all its raw lead_ids
    sub_to_primary: dict[str, str] = {}
    all_page_sub_ids: list[str] = []
    for lead in page_leads:
        lid = lead["id"]
        for sid in lead["lead_ids"]:
            sub_to_primary[sid] = lid
            all_page_sub_ids.append(sid)
    all_page_sub_ids = list(set(all_page_sub_ids))
    page_primary_set = {l["id"] for l in page_leads}
    page_phones = list({l.get("phone") for l in page_leads if l.get("phone")})

    # Fetch calls for all sub-lead-ids (covers maqsam calls on any lead in the merge)
    page_calls = (
        supabase.table("calls")
        .select("lead_id,agent_name,duration_seconds,outcome,called_at,maqsam_sentiment,summary_en,summary_ar,transcript")
        .in_("lead_id", all_page_sub_ids)
        .order("called_at")
        .execute().data or []
    )

    # Cross-channel messages via phone lookup (covers whatsapp lead messages)
    cross_leads: list[dict] = []
    if page_phones:
        cross_leads = (
            supabase.table("leads").select("id,phone")
            .in_("phone", page_phones)
            .execute().data or []
        )
    cross_phone = {cl["id"]: cl.get("phone") for cl in cross_leads}
    cross_ids = [cl["id"] for cl in cross_leads if cl.get("id")]

    page_messages: list[dict] = []
    if cross_ids:
        page_messages = (
            supabase.table("messages")
            .select("lead_id,direction,body,sent_at,agent_name")
            .in_("lead_id", cross_ids)
            .order("sent_at")
            .execute().data or []
        )

    page_alerts = (
        supabase.table("alerts")
        .select("lead_id,severity,type,message,created_at,resolved")
        .in_("lead_id", all_page_sub_ids)
        .order("created_at")
        .execute().data or []
    )

    # Group everything by primary (merged) lead_id
    calls_by: dict[str, list] = defaultdict(list)
    msgs_by: dict[str, list] = defaultdict(list)
    alerts_by: dict[str, list] = defaultdict(list)
    page_ana_by: dict[str, list] = defaultdict(list)

    for c in page_calls:
        raw_lid = c.get("lead_id")
        if not raw_lid:
            continue
        primary = sub_to_primary.get(raw_lid, raw_lid)
        if primary in page_primary_set:
            calls_by[primary].append(c)

    lead_phone_map = {l["id"]: l.get("phone") for l in page_leads}
    for m in page_messages:
        msg_lid = m.get("lead_id")
        if not msg_lid:
            continue
        phone = cross_phone.get(msg_lid)
        if not phone:
            continue
        for top_lid, top_phone in lead_phone_map.items():
            if top_phone == phone:
                msgs_by[top_lid].append(m)

    for al in page_alerts:
        raw_lid = al.get("lead_id")
        if not raw_lid:
            continue
        primary = sub_to_primary.get(raw_lid, raw_lid)
        if primary in page_primary_set:
            alerts_by[primary].append(al)

    # ai_analysis: re-use already-fetched analyses_raw, map via sub_to_primary
    for a in analyses_raw:
        raw_lid = a.get("lead_id")
        if not raw_lid:
            continue
        primary = sub_to_primary.get(raw_lid)
        if primary and primary in page_primary_set:
            page_ana_by[primary].append(a)

    # ── 9. Assemble result ────────────────────────────────────────────────────
    result = []
    for lead in page_leads:
        lid = lead["id"]
        timeline = []

        for i, c in enumerate(calls_by[lid]):
            oc = (c.get("outcome") or "unknown").lower()
            # Pick Maqsam summary in the right language; fall back to the other language
            if lang == "ar":
                call_summary = c.get("summary_ar") or c.get("summary_en") or ""
            else:
                call_summary = c.get("summary_en") or c.get("summary_ar") or ""
            timeline.append({
                "type": "call",
                "date": c.get("called_at"),
                "color": _OUTCOME_COLOR.get(oc, "gray"),
                "title": "First Call" if i == 0 else f"Call #{i + 1}",
                "agent": c.get("agent_name") or "—",
                "duration": _dur(c.get("duration_seconds")),
                "outcome": oc,
                "summary": call_summary,
                "sentiment": c.get("maqsam_sentiment") or "",
            })

        all_msgs = msgs_by[lid]
        inbound = [m for m in all_msgs if m.get("direction") == "inbound"]
        outbound = [m for m in all_msgs if m.get("direction") == "outbound"]
        if inbound:
            m = inbound[0]
            timeline.append({
                "type": "whatsapp", "date": m.get("sent_at"), "color": "blue",
                "title": "First WhatsApp Message", "direction": "inbound",
                "preview": m.get("body") or "", "total": len(all_msgs),
            })
        if outbound:
            m = outbound[0]
            timeline.append({
                "type": "whatsapp", "date": m.get("sent_at"), "color": "blue",
                "title": "First Agent Reply", "direction": "outbound",
                "agent": m.get("agent_name") or "—", "preview": m.get("body") or "",
            })

        lead_calls = calls_by[lid]
        _all_short_no_transcript = bool(lead_calls) and all(
            (c.get("duration_seconds") or 0) < 30 and not c.get("transcript")
            for c in lead_calls
        )

        for a in page_ana_by[lid]:
            ts = a.get("treatment_score") or 0
            color = "green" if ts >= 70 else "orange" if ts >= 40 else "red"
            event: dict = {
                "type": "ai", "date": a.get("analyzed_at"), "color": color,
                "title": "AI Analysis", "score": ts,
                "sentiment": a.get("sentiment") or "neutral",
                "topics": a.get("topics") or [],
                "outcome": a.get("outcome") or "",
                "risk_flags": a.get("risk_flags") or [],
                "summary": a.get("summary") or "",
                "source": a.get("source") or "",
            }
            if _all_short_no_transcript:
                event["unreliable"] = True
            timeline.append(event)

        for al in alerts_by[lid]:
            sev = al.get("severity") or "MED"
            timeline.append({
                "type": "alert", "date": al.get("created_at"),
                "color": _SEV_COLOR.get(sev, "orange"),
                "title": (al.get("type") or "alert").replace("_", " ").title(),
                "severity": sev, "message": al.get("message") or "",
                "resolved": al.get("resolved") or False,
            })

        is_conv = lead.get("status") == "converted"
        timeline.append({
            "type": "account", "date": None,
            "color": "green" if is_conv else "gray",
            "title": "Account Opened", "status": lead.get("status") or "pending",
        })

        timeline.sort(key=lambda x: (x.get("date") is None, x.get("date") or ""))

        total_calls = len(calls_by[lid])
        answered = sum(1 for c in calls_by[lid] if (c.get("outcome") or "") == "completed")
        latest_a = page_ana_by[lid][-1] if page_ana_by[lid] else None

        result.append({
            "id": lid,
            "lead_ids": lead["lead_ids"],
            "phone": lead.get("phone") or "—",
            "name": lead.get("name") or "",
            "score": lead.get("score") or 0,
            "status": lead.get("status") or "new",
            "assigned_agent": lead.get("assigned_agent") or "—",
            "channel": lead.get("channel") or "—",
            "channels": lead.get("channels") or [],
            "total_calls": total_calls,
            "answered_calls": answered,
            "total_messages": len(all_msgs),
            "last_sentiment": latest_a.get("sentiment") if latest_a else None,
            "open_alerts": sum(1 for al in alerts_by[lid] if not al.get("resolved")),
            "timeline": timeline,
        })

    return {
        "leads": result,
        "total": total,
        "page": page,
        "pages": pages,
        "meta": {"agents": all_agents},
    }

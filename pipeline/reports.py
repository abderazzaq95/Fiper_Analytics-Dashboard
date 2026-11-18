"""Daily and weekly management reports (email).

Focus: what helps the team work better —
  * how fast customers get a first answer (working time, like the reminders),
  * who is waiting right now,
  * per-agent WhatsApp + call performance against the team,
  * the reminder system (reminders, escalations, follow-ups),
  * weekly: trends vs the previous week, busiest hours, customer topics.

Calculations are pure functions (unit-tested); loading/sending wraps them.
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from pipeline import email_notifications as notify
from pipeline import reminders
from pipeline import report_templates as RT
from pipeline.reply_tracker import Schedule, median, parse_dt, wait_episodes

log = logging.getLogger("fiper.reports")
supabase = notify.supabase

SLA_MIN = 15  # "answered within 15 working minutes"


# ── Agent identity (one person, one name across ManyContacts + Maqsam) ───────

_identity_cache: dict[str, tuple[str, str] | None] = {}


def agent_identity(name: str | None) -> tuple[str, str] | None:
    """-> (key, display name) or None for empty/unknown names."""
    raw = (name or "").strip()
    if raw in _identity_cache:
        return _identity_cache[raw]
    result = None
    clean = reminders._clean_agent(raw)
    if clean:
        canon = reminders._canonical_agent(clean) or clean
        key = notify._normalize_name(canon)
        display = canon.title() if canon == canon.lower() else canon
        result = (key, display) if key else None
    _identity_cache[raw] = result
    return result


# ── Pure calculations ────────────────────────────────────────────────────────

@dataclass
class AgentStats:
    name: str
    episodes: int = 0
    answered: int = 0
    within_sla: int = 0
    sla_base: int = 0
    reply_minutes: list = field(default_factory=list)
    chats: set = field(default_factory=set)
    messages_sent: int = 0
    waiting_now: int = 0
    calls: int = 0
    completed: int = 0
    no_answer: int = 0
    busy: int = 0
    talk_seconds: int = 0
    escalated: int = 0
    treatment: list = field(default_factory=list)

    @property
    def median_reply(self):
        return median(self.reply_minutes)

    @property
    def sla_pct(self):
        return round(self.within_sla / self.sla_base * 100) if self.sla_base else None

    @property
    def pickup_pct(self):
        base = self.completed + self.no_answer + self.busy
        return round(self.completed / base * 100) if base else None


def responsiveness(convs: dict, calls_by_phone: dict, start: datetime, end: datetime,
                   schedule: Schedule, owner_of, agents: dict) -> dict:
    """First-reply metrics for wait episodes that STARTED in [start, end).

    - reply time = working minutes from the customer's message to the first
      agent WhatsApp reply or Maqsam call;
    - SLA = answered within SLA_MIN working minutes. An unanswered episode
      younger than the SLA is not counted yet (it is not late yet).
    `agents` is filled in place: key -> AgentStats.
    """
    customers: set = set()
    replies: list[float] = []
    episodes = answered = within = sla_base = 0
    by_hour: dict[int, list] = defaultdict(list)
    by_day: dict[str, dict] = defaultdict(lambda: {"customers": set(), "replies": [], "within": 0, "base": 0})
    for phone, conv in convs.items():
        for ep in wait_episodes(conv["messages"], calls_by_phone.get(phone)):
            if not (start <= ep.start < end):
                continue
            done = ep.end is not None and ep.end <= end
            minutes = schedule.working_minutes_between(ep.start, ep.end if done else end)
            ident = agent_identity(ep.agent) if (done and ep.agent) else None
            ident = ident or owner_of(conv)
            stats = agents.setdefault(ident[0], AgentStats(ident[1])) if ident else None

            customers.add(phone)
            episodes += 1
            local = ep.start.astimezone(schedule.tz)
            day = by_day[local.strftime("%Y-%m-%d")]
            day["customers"].add(phone)
            counted = done or minutes >= SLA_MIN
            if done:
                answered += 1
                replies.append(minutes)
                by_hour[local.hour].append(minutes)
                day["replies"].append(minutes)
            else:
                by_hour[local.hour].append(None)
            if counted:
                sla_base += 1
                day["base"] += 1
                if done and minutes <= SLA_MIN:
                    within += 1
                    day["within"] += 1
            if stats:
                stats.episodes += 1
                if counted:
                    stats.sla_base += 1
                    if done and minutes <= SLA_MIN:
                        stats.within_sla += 1
                if done:
                    stats.answered += 1
                    stats.reply_minutes.append(minutes)
                    stats.chats.add(phone)
    return {
        "customers": len(customers),
        "episodes": episodes,
        "answered": answered,
        "median_reply": median(replies),
        "sla_pct": round(within / sla_base * 100) if sla_base else None,
        "by_hour": dict(by_hour),
        "by_day": {k: {"customers": len(v["customers"]), "median_reply": median(v["replies"]),
                       "sla_pct": round(v["within"] / v["base"] * 100) if v["base"] else None}
                   for k, v in by_day.items()},
    }


def call_metrics(rows: list[dict], agents: dict, schedule: Schedule | None = None) -> dict:
    """Maqsam call stats. Pickup = completed / (completed + no_answer + busy),
    the same definition as the Maqsam dashboard (failed calls excluded)."""
    totals = Counter()
    talk = 0
    by_day: dict[str, Counter] = defaultdict(Counter)
    for c in rows:
        outcome = (c.get("outcome") or "").lower()
        dur = int(c.get("duration_seconds") or 0)
        totals["calls"] += 1
        totals[outcome] += 1
        if outcome == "completed":
            talk += dur
        if schedule:
            dt = parse_dt(c.get("called_at"))
            if dt:
                d = by_day[dt.astimezone(schedule.tz).strftime("%Y-%m-%d")]
                d["calls"] += 1
                d[outcome] += 1
        ident = agent_identity(c.get("agent_name"))
        if not ident:
            continue
        s = agents.setdefault(ident[0], AgentStats(ident[1]))
        s.calls += 1
        if outcome == "completed":
            s.completed += 1
            s.talk_seconds += dur
        elif outcome == "no_answer":
            s.no_answer += 1
        elif outcome == "busy":
            s.busy += 1
    base = totals["completed"] + totals["no_answer"] + totals["busy"]
    return {
        "calls": totals["calls"],
        "completed": totals["completed"],
        "pickup_pct": round(totals["completed"] / base * 100) if base else None,
        "talk_seconds": talk,
        "avg_call_seconds": round(talk / totals["completed"]) if totals["completed"] else None,
        "by_day": {k: {"calls": v["calls"],
                       "pickup_pct": round(v["completed"] / (v["completed"] + v["no_answer"] + v["busy"]) * 100)
                       if (v["completed"] + v["no_answer"] + v["busy"]) else None}
                   for k, v in by_day.items()},
    }


def reminder_metrics(rows: list[dict], start: datetime, end: datetime) -> dict:
    def within(v):
        dt = parse_dt(v)
        return bool(dt and start <= dt < end)
    reminded = [r for r in rows if within(r.get("last_reminded_at")) or (within(r.get("created_at")) and (r.get("reminder_count") or 0) > 0)]
    answered_after = [r for r in rows if r.get("status") == "answered" and within(r.get("resolved_at")) and (r.get("reminder_count") or 0) > 0]
    escalated = [r for r in rows if within(r.get("escalated_at"))]
    followups = [r for r in rows if within(r.get("last_escalated_at")) and (r.get("escalation_count") or 0) > 1]
    expired = [r for r in rows if r.get("status") == "expired" and within(r.get("resolved_at"))]
    return {
        "reminded_customers": len(reminded),
        "answered_after_reminder": len(answered_after),
        "escalated": len(escalated),
        "followups": len(followups),
        "expired_unanswered": len(expired),
    }


# ── Data loading ─────────────────────────────────────────────────────────────

def _paginate(build):
    return notify._paginate(build)


def _load(start: datetime, end: datetime) -> dict:
    ctx_start = start - timedelta(hours=24)  # context so waits that began just before are understood
    messages = _paginate(
        lambda: supabase.table("messages")
        .select("lead_id,direction,body,sent_at,agent_name,whatsapp_business_number")
        .gte("sent_at", ctx_start.isoformat()).lt("sent_at", end.isoformat())
        .order("sent_at", desc=False)
    )
    lead_ids = list({m["lead_id"] for m in messages if m.get("lead_id")})
    leads: dict[str, dict] = {}
    for chunk in reminders._chunks(lead_ids, 150):
        for r in (supabase.table("leads")
                  .select("id,phone,wa_contact_id,name,assigned_agent,channel,updated_at")
                  .in_("id", chunk).execute().data or []):
            leads[r["id"]] = r
    convs: dict[str, dict] = {}
    for m in messages:
        lead = leads.get(m.get("lead_id"))
        if not lead:
            continue
        key = reminders.phone_key(lead.get("phone")) or reminders.phone_key(lead.get("wa_contact_id")) or f"lead:{lead['id']}"
        conv = convs.setdefault(key, {"phone": key, "messages": [], "leads": {}})
        conv["messages"].append(m)
        conv["leads"][lead["id"]] = lead
    calls_by_phone = reminders._load_call_times(sorted(convs), ctx_start)
    call_rows = _paginate(
        lambda: supabase.table("calls")
        .select("agent_name,outcome,duration_seconds,called_at")
        .gte("called_at", start.isoformat()).lt("called_at", end.isoformat())
    )
    new_leads = _paginate(
        lambda: supabase.table("leads").select("channel")
        .gte("created_at", start.isoformat()).lt("created_at", end.isoformat())
    )
    return {"messages": messages, "convs": convs, "calls_by_phone": calls_by_phone,
            "call_rows": call_rows, "new_leads": new_leads}


def _pending_rows(since: datetime) -> list[dict]:
    try:
        return _paginate(lambda: supabase.table("pending_replies").select("*").gte("updated_at", since.isoformat()))
    except Exception as exc:
        log.warning(f"[reports] pending_replies unavailable: {exc}")
        return []


def _owner_of(conv: dict):
    return agent_identity(reminders._pick_agent(conv, None))


def _period_metrics(data: dict, start: datetime, end: datetime) -> tuple[dict, dict]:
    schedule = reminders.SCHEDULE
    agents: dict[str, AgentStats] = {}
    resp = responsiveness(data["convs"], data["calls_by_phone"], start, end, schedule, _owner_of, agents)
    calls = call_metrics([c for c in data["call_rows"] if start <= (parse_dt(c.get("called_at")) or start) < end],
                         agents, schedule)
    for m in data["messages"]:
        dt = parse_dt(m.get("sent_at"))
        if (m.get("direction") or "").lower() == "outbound" and dt and start <= dt < end:
            ident = agent_identity(m.get("agent_name"))
            if ident:
                agents.setdefault(ident[0], AgentStats(ident[1])).messages_sent += 1
    leads = Counter((r.get("channel") or "other") for r in data["new_leads"])
    return {"resp": resp, "calls": calls, "new_leads": sum(leads.values()), "new_leads_by_channel": dict(leads)}, agents


def _waiting_now(agents: dict | None = None) -> list[dict]:
    now = datetime.now(timezone.utc)
    try:
        rows = _paginate(lambda: supabase.table("pending_replies").select("*").eq("status", "open").order("waiting_since"))
    except Exception:
        return []
    out = []
    for r in rows:
        elapsed = reminders.SCHEDULE.working_minutes_between(parse_dt(r["waiting_since"]), now)
        ident = agent_identity(r.get("agent_name"))
        if agents is not None and ident:
            agents.setdefault(ident[0], AgentStats(ident[1])).waiting_now += 1
        out.append({
            "name": r.get("customer_name"), "phone": r.get("phone"), "line": r.get("business_line"),
            "agent": ident[1] if ident else None, "elapsed_min": elapsed,
            "since": parse_dt(r.get("waiting_since")), "body": r.get("last_inbound_body"),
            "reminders": r.get("reminder_count") or 0,
            "escalations": r.get("escalation_count") or (1 if r.get("escalated_at") else 0),
        })
    out.sort(key=lambda w: -w["elapsed_min"])
    return out


# ── Daily ────────────────────────────────────────────────────────────────────

def build_daily(now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    tz = reminders.SCHEDULE.tz
    start = now.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    data = _load(start, now)
    metrics, agents = _period_metrics(data, start, now)
    waiting = _waiting_now(agents)
    pend = _pending_rows(start)
    metrics["reminders"] = reminder_metrics(pend, start, now)
    for r in pend:
        dt = parse_dt(r.get("escalated_at"))
        ident = agent_identity(r.get("agent_name"))
        if ident and dt and start <= dt < now:
            agents.setdefault(ident[0], AgentStats(ident[1])).escalated += 1
    return {"now": now, "start": start, "metrics": metrics, "agents": agents, "waiting": waiting}


def send_daily_report(report_label: str = "") -> bool:
    recipients = notify._supervisor_recipients()
    if not recipients:
        return False
    data = build_daily()
    subject, html_body = RT.daily_report(data, reminders.SCHEDULE.tz, reminders.TZ_LABEL, report_label)
    return notify._send_email(recipients, subject, html_body)


# ── Weekly ───────────────────────────────────────────────────────────────────

def build_weekly(now: datetime | None = None) -> dict:
    """Last full week Monday 00:00 -> Monday 00:00 (local), vs the week before."""
    now = now or datetime.now(timezone.utc)
    tz = reminders.SCHEDULE.tz
    local = now.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    this_monday = local - timedelta(days=local.weekday())
    end = this_monday.astimezone(timezone.utc)
    start = (this_monday - timedelta(days=7)).astimezone(timezone.utc)
    prev_start = (this_monday - timedelta(days=14)).astimezone(timezone.utc)

    data = _load(prev_start, end)
    metrics, agents = _period_metrics(data, start, end)
    prev_metrics, prev_agents = _period_metrics(data, prev_start, start)
    pend = _pending_rows(prev_start)
    metrics["reminders"] = reminder_metrics(pend, start, end)
    for r in pend:
        dt = parse_dt(r.get("escalated_at"))
        ident = agent_identity(r.get("agent_name"))
        if ident and dt and start <= dt < end:
            agents.setdefault(ident[0], AgentStats(ident[1])).escalated += 1

    # Customer messages arriving outside working hours (staffing insight).
    after_hours = total_in = 0
    for m in data["messages"]:
        dt = parse_dt(m.get("sent_at"))
        if (m.get("direction") or "").lower() == "inbound" and dt and start <= dt < end:
            total_in += 1
            if not reminders.SCHEDULE.is_working_time(dt):
                after_hours += 1
    metrics["after_hours_pct"] = round(after_hours / total_in * 100) if total_in else None

    # AI quality + topics.
    analyses = _paginate(
        lambda: supabase.table("ai_analysis").select("lead_id,topics,sentiment,treatment_score,source")
        .gte("analyzed_at", start.isoformat()).lt("analyzed_at", end.isoformat())
    )
    lead_agent: dict[str, str] = {}
    ids = list({a["lead_id"] for a in analyses if a.get("lead_id")})
    for chunk in reminders._chunks(ids, 150):
        for r in supabase.table("leads").select("id,assigned_agent").in_("id", chunk).execute().data or []:
            lead_agent[r["id"]] = r.get("assigned_agent")
    topics = Counter(t for a in analyses for t in (a.get("topics") or []) if t and t not in {"greetings", "no_answer"})
    sentiment = Counter((a.get("sentiment") or "").lower() for a in analyses if a.get("sentiment"))
    for a in analyses:
        score = a.get("treatment_score")
        ident = agent_identity(lead_agent.get(a.get("lead_id")))
        if ident and score is not None and not (a.get("source") == "maqsam" and score == 0):
            agents.setdefault(ident[0], AgentStats(ident[1])).treatment.append(score)
    metrics["topics"] = topics.most_common(8)
    metrics["topics_total"] = sum(topics.values())
    metrics["sentiment"] = dict(sentiment)
    metrics["analyses"] = len(analyses)

    return {"now": now, "start": start, "end": end, "metrics": metrics, "prev": prev_metrics,
            "agents": agents, "prev_agents": prev_agents, "insights": weekly_insights(metrics, prev_metrics, agents)}


def weekly_insights(m: dict, p: dict, agents: dict) -> list[tuple[str, str]]:
    """Plain, data-driven observations -> [(arabic, english)]."""
    out: list[tuple[str, str]] = []
    team_med = m["resp"]["median_reply"]
    active = [a for a in agents.values() if a.answered >= 10]
    if active:
        best = min(active, key=lambda a: a.median_reply)
        out.append((f"أسرع رد هذا الأسبوع: {best.name} (الوسيط {RT.mins_ar(best.median_reply)}).",
                    f"Fastest replies: {best.name} (median {RT.mins_en(best.median_reply)})."))
        if team_med is not None:
            slow = [a for a in active if a.median_reply is not None and a.median_reply > max(2 * team_med, 30)]
            for a in sorted(slow, key=lambda a: -a.median_reply)[:3]:
                out.append((f"{a.name}: وسيط الرد {RT.mins_ar(a.median_reply)} مقابل {RT.mins_ar(team_med)} للفريق — يحتاج متابعة.",
                            f"{a.name}: median reply {RT.mins_en(a.median_reply)} vs team {RT.mins_en(team_med)} — needs coaching."))
    if m["resp"]["sla_pct"] is not None and p["resp"]["sla_pct"] is not None:
        d = m["resp"]["sla_pct"] - p["resp"]["sla_pct"]
        if abs(d) >= 5:
            up = d > 0
            out.append((f"نسبة الرد خلال 15 دقيقة {'تحسنت' if up else 'انخفضت'} بمقدار {abs(d)} نقطة ({m['resp']['sla_pct']}%).",
                        f"Answered within 15 min {'improved' if up else 'dropped'} by {abs(d)} pts ({m['resp']['sla_pct']}%)."))
    by_hour = m["resp"]["by_hour"]
    if by_hour:
        busiest = max(by_hour.items(), key=lambda kv: len(kv[1]))
        out.append((f"أكثر ساعة ازدحاماً: {busiest[0]:02d}:00 ({len(busiest[1])} محادثة) — تأكد من وجود موظفين كافين.",
                    f"Busiest hour: {busiest[0]:02d}:00 ({len(busiest[1])} customer waits) — make sure it is staffed."))
    if m.get("after_hours_pct"):
        out.append((f"{m['after_hours_pct']}% من رسائل العملاء تصل خارج أوقات العمل.",
                    f"{m['after_hours_pct']}% of customer messages arrive outside working hours."))
    rem = m.get("reminders") or {}
    if rem.get("expired_unanswered"):
        out.append((f"{rem['expired_unanswered']} عميل بقي دون رد حتى انتهاء المتابعة.",
                    f"{rem['expired_unanswered']} customer(s) were never answered."))
    return out


def send_weekly_report(report_label: str = "") -> bool:
    recipients = notify._supervisor_recipients()
    if not recipients:
        return False
    data = build_weekly()
    subject, html_body = RT.weekly_report(data, reminders.SCHEDULE.tz, reminders.TZ_LABEL, report_label)
    return notify._send_email(recipients, subject, html_body)

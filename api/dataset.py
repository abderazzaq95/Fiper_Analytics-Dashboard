"""Shared in-memory data for dashboard periods.

Each period (today, yesterday, week, month, custom) is loaded once from Supabase
and kept in memory. Every dashboard section is computed from it, so switching
date, team or WhatsApp line needs no database work. A background job keeps the
common periods fresh; the previous version keeps serving during a refresh.
"""
from __future__ import annotations

import logging
import os
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Callable

from dotenv import load_dotenv
from supabase import ClientOptions, create_client

from pipeline import agent_names
from pipeline.reply_tracker import agent_send_times, parse_dt, phone_key
from pipeline.time_ranges import is_closed, period_bounds
from pipeline.whatsapp import add_whatsapp_line_select, matches_business_line, resolve_agent_name

load_dotenv()
log = logging.getLogger("fiper.dataset")

PAGE = 1000
WARM_RANGES = ("today", "yesterday", "week", "month")
TEAM_READY_RANGES = ("today", "yesterday", "week")
MAX_PERIODS = 12
MAX_CONNECTIONS = 8
REQUEST_TIMEOUT_SECONDS = 20
MAX_VIEWS = 30
MAX_MEMO = 200
CONTEXT = timedelta(hours=24)  # earlier messages needed to understand reply times

CALL_COLUMNS = "id,lead_id,agent_name,duration_seconds,outcome,called_at"
MESSAGE_COLUMNS = "id,lead_id,direction,body,agent_name,sent_at"
LEAD_COLUMNS = "id,phone,wa_contact_id,name,status,score,assigned_agent,channel,last_message_at,created_at,updated_at"
ANALYSIS_COLUMNS = "id,lead_id,sentiment,topics,treatment_score,risk_flags,analyzed_at,source,summary,outcome"
UUID_RE = re.compile(r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")


# How old a period may get before it is reloaded (seconds). While someone uses
# the dashboard the data stays fresh; when nobody has opened it for
# IDLE_AFTER_SECONDS, the background job reloads much less often to spare the
# database (Supabase disk IO budget). The first request after a quiet time
# still answers instantly and starts a fresh load in the background.
ACTIVE_TTL = {"today": 120, "week": 600, "7d": 600, "month": 1800}
IDLE_TTL = {"today": 900, "week": 3600, "7d": 3600, "month": 4 * 3600}
OTHER_OPEN_TTL = 300
CLOSED_TTL, CLOSED_IDLE_TTL = 3600, 6 * 3600
IDLE_AFTER_SECONDS = int(os.getenv("DASHBOARD_IDLE_MINUTES", "15")) * 60
_last_used = 0.0


def mark_used() -> None:
    global _last_used
    _last_used = time.time()


def is_idle() -> bool:
    return time.time() - _last_used > IDLE_AFTER_SECONDS


def ttl_seconds(range_: str, idle: bool = False) -> int:
    if is_closed(range_):
        return CLOSED_IDLE_TTL if idle else CLOSED_TTL
    return (IDLE_TTL if idle else ACTIVE_TTL).get(range_, OTHER_OPEN_TTL * (6 if idle else 1))


_clients: queue.Queue = queue.Queue()
_client_count = 0
_client_lock = threading.Lock()


def _take_client():
    """One client per running request: a shared HTTP/2 connection stalls under threads."""
    global _client_count
    try:
        return _clients.get_nowait()
    except queue.Empty:
        pass
    with _client_lock:
        create = _client_count < MAX_CONNECTIONS
        if create:
            _client_count += 1
    return _new_client() if create else _clients.get()


def _new_client():
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"),
                         options=ClientOptions(postgrest_client_timeout=REQUEST_TIMEOUT_SECONDS))


class ThreadClient:
    """Supabase access with one connection per thread (safe for parallel requests)."""

    def __init__(self):
        self._local = threading.local()

    def table(self, name: str):
        if not hasattr(self._local, "client"):
            self._local.client = _new_client()
        return self._local.client.table(name)


db = ThreadClient()


def _request(call: Callable):
    """Run one database request; short network hiccups must not lose a refresh."""
    for attempt in range(3):
        client = _take_client()
        try:
            return call(client)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))
        finally:
            _clients.put(client)


def _rows(table: str, columns: str, apply: Callable, order: str) -> list[dict]:
    """All matching rows; pages after the first are fetched in parallel."""
    def page(client, start: int, count: str | None = None):
        select = client.table(table).select(columns, count=count) if count else client.table(table).select(columns)
        return apply(select).order(order).order("id").range(start, start + PAGE - 1).execute()

    first = _request(lambda client: page(client, 0, "exact"))
    rows = list(first.data or [])
    if len(rows) < PAGE:
        return rows
    if first.count is None:
        offset = PAGE
        while True:
            start = offset
            batch = _request(lambda client: page(client, start)).data or []
            rows.extend(batch)
            if len(batch) < PAGE:
                break
            offset += PAGE
    else:
        with ThreadPoolExecutor(max_workers=4) as pool:
            for result in pool.map(lambda start: _request(lambda client: page(client, start)),
                                   range(PAGE, first.count, PAGE)):
                rows.extend(result.data or [])
    return list({row["id"]: row for row in rows}.values())


@lru_cache(maxsize=5000)
def agent_key(name: str | None) -> str:
    value = (name or "").strip()
    if UUID_RE.fullmatch(value):
        value = (resolve_agent_name(value) or "").strip()
        if UUID_RE.fullmatch(value):
            return ""
    if not value or value.lower() in {"unknown", "team", "n/a", "none"}:
        return ""
    return agent_names.key(value)


class Period:
    def __init__(self, range_: str, *, calls, messages, context_messages, context_calls,
                 activity, alerts, analyses, leads, start, end, sends=None):
        self.range = range_
        self.start, self.end = start, end
        self.calls, self.messages, self.activity = calls, messages, activity
        self.context_messages, self.context_calls = context_messages, context_calls
        self.sends = sends or []  # WhatsApp "sent" events: agent voice notes / media count as replies
        self.alerts, self.analyses, self.leads = alerts, analyses, leads
        self.loaded_at = time.time()
        self.views: dict[tuple[str, str], View] = {}
        self._views_lock = threading.Lock()

        self.lead_ids_by_phone: dict[str, set[str]] = {}
        for lead_id, lead in leads.items():
            key = phone_key(lead.get("phone"))
            if key:
                self.lead_ids_by_phone.setdefault(key, set()).add(lead_id)
        self.latest_message: dict[str, dict] = {}
        self.latest_inbound_by_phone: dict[str, dict] = {}
        for message in sorted(messages, key=lambda m: m.get("sent_at") or ""):
            lead_id = message.get("lead_id")
            if not lead_id:
                continue
            self.latest_message[lead_id] = message
            key = phone_key((leads.get(lead_id) or {}).get("phone"))
            if key and message.get("direction") == "inbound":
                self.latest_inbound_by_phone[key] = message

    @property
    def stale(self) -> bool:
        return time.time() - self.loaded_at >= ttl_seconds(self.range)

    def stale_for(self, idle: bool) -> bool:
        return time.time() - self.loaded_at >= ttl_seconds(self.range, idle)

    def view(self, team: str = "", wa_line: str = "all") -> "View":
        members = team_members(team)
        key = (team if members is not None else "", _line(wa_line))
        with self._views_lock:
            view = self.views.get(key)
            if view is None:
                if len(self.views) >= MAX_VIEWS:
                    self.views.pop(next(iter(self.views)))
                view = self.views[key] = View(self, key[0], key[1], members)
            return view


def _line(wa_line: str | None) -> str:
    value = (wa_line or "all").strip()
    return "all" if value.lower() in {"all", "*", "any", ""} else value


class View:
    """One period narrowed to a team and/or WhatsApp line."""

    def __init__(self, period: Period, team: str, wa_line: str, members: set[str] | None):
        self.period, self.team, self.wa_line, self.members = period, team, wa_line, members
        self._memo: dict = {}
        self._locks: dict = {}
        self._lock = threading.Lock()
        leads = period.leads
        line_on = wa_line != "all"

        calls = period.calls
        messages = [m for m in period.messages if matches_business_line(m, wa_line)] if line_on else period.messages
        activity = [l for l in period.activity if matches_business_line(l, wa_line)] if line_on else period.activity
        alerts = period.alerts
        analyses = period.analyses
        visible = leads
        if line_on:
            visible = {i: l for i, l in leads.items() if matches_business_line(l, wa_line)}
            alerts = [a for a in alerts if a.get("lead_id") in visible]
            analyses = [a for a in analyses if a.get("lead_id") in visible]

        self.owned: set[str] | None = None
        if members is not None:
            owned = {i for i, l in visible.items() if agent_key(l.get("assigned_agent")) in members}
            calls = [c for c in calls if agent_key(c.get("agent_name")) in members]
            messages = [m for m in messages if self._team_message(m, owned)]
            touched = owned | {c["lead_id"] for c in calls if c.get("lead_id")} | {
                m["lead_id"] for m in messages if m.get("lead_id")}
            activity = [l for l in activity if l.get("id") in owned]
            alerts = [a for a in alerts
                      if (agent_key(a.get("agent_name")) or agent_key((leads.get(a.get("lead_id")) or {}).get("assigned_agent"))) in members]
            analyses = [a for a in analyses if a.get("lead_id") in touched]
            visible = {i: visible[i] for i in touched if i in visible}
            self.owned = owned
        self.calls, self.messages, self.activity = calls, messages, activity
        self.alerts, self.analyses, self.leads = alerts, analyses, visible

    def _team_message(self, message: dict, owned: set[str]) -> bool:
        return message.get("lead_id") in owned or agent_key(message.get("agent_name")) in self.members

    def has_agent(self, name: str | None) -> bool:
        return self.members is None or agent_key(name) in self.members

    def cached(self, key, compute: Callable[["View"], object]):
        if key in self._memo:
            return self._memo[key]
        with self._lock:
            lock = self._locks.setdefault(key, threading.Lock())
        with lock:
            if key not in self._memo:
                if len(self._memo) >= MAX_MEMO:
                    self._memo.clear()
                self._memo[key] = compute(self)
            return self._memo[key]

    def reply_stats(self) -> dict:
        return self.cached("reply_stats", _compute_reply_stats)


def _compute_reply_stats(view: View) -> dict:
    """First-reply figures, defined exactly like the email reports."""
    from pipeline import reminders, reports

    period = view.period
    context = period.context_messages
    if view.wa_line != "all":
        context = [m for m in context if matches_business_line(m, view.wa_line)]
    if view.owned is not None:
        context = [m for m in context if view._team_message(m, view.owned)]
    conversations: dict[str, dict] = {}
    for message in [*context, *view.messages]:
        lead = period.leads.get(message.get("lead_id"))
        if not lead:
            continue
        key = phone_key(lead.get("phone")) or phone_key(lead.get("wa_contact_id")) or f"lead:{lead['id']}"
        conversation = conversations.setdefault(key, {"phone": key, "messages": [], "leads": {}})
        conversation["messages"].append(message)
        conversation["leads"][lead["id"]] = lead
    calls_by_phone: dict[str, list] = {}
    for call in [*period.context_calls, *period.calls]:
        key = phone_key((period.leads.get(call.get("lead_id")) or {}).get("phone"))
        when = parse_dt(call.get("called_at"))
        if key and when:
            calls_by_phone.setdefault(key, []).append(when)
    sends_by_phone: dict[str, list] = {}
    for send in period.sends:
        key = phone_key(send.get("phone"))
        if key in conversations:
            sends_by_phone.setdefault(key, []).append(send)
    for key, sends in sends_by_phone.items():
        times = agent_send_times(conversations[key]["messages"], sends)
        calls_by_phone[key] = [*calls_by_phone.get(key, []), *times]
    end = min(period.end or datetime.now(timezone.utc), datetime.now(timezone.utc))
    agents: dict = {}
    team = reports.responsiveness(conversations, calls_by_phone, period.start, end,
                                  reminders.SCHEDULE, reports._owner_of, agents)
    return {
        "team": {k: team[k] for k in ("customers", "episodes", "answered", "median_reply", "sla_pct")},
        "agents": {key: {"median_reply": a.median_reply, "sla_pct": a.sla_pct, "answered": a.answered,
                         "episodes": a.episodes} for key, a in agents.items()},
    }


def team_members(team: str | None) -> set[str] | None:
    """Agent keys of a team. None = no filter (empty or unknown team)."""
    if not team:
        return None
    from api import workspace
    for item in workspace.teams()["teams"]:
        if item["name"] == team:
            return {agent_names.key(name) for name in item["agents"]}
    return None


def _optional_rows(table: str, columns: str, apply: Callable, order: str) -> list[dict]:
    """Like _rows, but a missing table (migration not run yet) gives no rows."""
    try:
        return _rows(table, columns, apply, order)
    except Exception as exc:
        log.warning(f"[dataset] {table} not loaded: {str(exc)[:160]}")
        return []


def load_period(range_: str) -> Period:
    start, end = period_bounds(range_)
    upper = end or datetime.now(timezone.utc) + timedelta(days=1)
    since, until = start.isoformat(), upper.isoformat()
    context_since = (start - CONTEXT).isoformat()
    message_columns = add_whatsapp_line_select(MESSAGE_COLUMNS)
    lead_columns = add_whatsapp_line_select(LEAD_COLUMNS)

    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = {
            "calls": pool.submit(_rows, "calls", CALL_COLUMNS,
                                 lambda q: q.gte("called_at", since).lt("called_at", until), "called_at"),
            "messages": pool.submit(_rows, "messages", message_columns,
                                    lambda q: q.gte("sent_at", since).lt("sent_at", until), "sent_at"),
            "context_messages": pool.submit(_rows, "messages", message_columns,
                                            lambda q: q.gte("sent_at", context_since).lt("sent_at", since), "sent_at"),
            "context_calls": pool.submit(_rows, "calls", "id,lead_id,called_at",
                                         lambda q: q.gte("called_at", context_since).lt("called_at", since), "called_at"),
            "activity": pool.submit(_rows, "leads", lead_columns,
                                    lambda q: q.eq("channel", "whatsapp").gte("last_message_at", since), "last_message_at"),
            "alerts": pool.submit(_rows, "alerts", "*",
                                  lambda q: q.gte("created_at", since).lt("created_at", until), "created_at"),
            "analyses": pool.submit(_rows, "ai_analysis", ANALYSIS_COLUMNS,
                                    lambda q: q.gte("analyzed_at", since).lt("analyzed_at", until), "analyzed_at"),
            "sends": pool.submit(_optional_rows, "whatsapp_sends", "id,phone,sent_at,category",
                                 lambda q: q.gte("sent_at", context_since).lt("sent_at", until), "sent_at"),
        }
        data = {name: job.result() for name, job in jobs.items()}

    if is_closed(range_):
        # last_message_at only keeps the latest activity: use the period's messages.
        active = {m.get("lead_id") for m in data["messages"]}
        data["activity"] = [lead for lead in data["activity"] if lead["id"] in active]
    leads = {lead["id"]: lead for lead in data["activity"]}
    wanted = {row.get("lead_id") for name in ("calls", "messages", "context_messages", "context_calls", "alerts", "analyses")
              for row in data[name]}
    missing = [lead_id for lead_id in wanted if lead_id and lead_id not in leads]
    chunks = [missing[i:i + 150] for i in range(0, len(missing), 150)]
    def fetch_leads(ids: list[str]) -> list[dict]:
        return _request(lambda client: client.table("leads").select(lead_columns).in_("id", ids).execute()).data or []

    with ThreadPoolExecutor(max_workers=6) as pool:
        for rows in pool.map(fetch_leads, chunks):
            leads.update({lead["id"]: lead for lead in rows})
    return Period(range_, start=start, end=end, leads=leads, **data)


_periods: dict[str, Period] = {}
_period_locks: dict[str, threading.Lock] = {}
_refreshing: set[str] = set()
_state_lock = threading.Lock()


def _publish(period: Period) -> None:
    with _state_lock:
        _periods[period.range] = period
        extra = [key for key in _periods if key not in WARM_RANGES]
        while len(_periods) > MAX_PERIODS and extra:
            oldest = min(extra, key=lambda key: _periods[key].loaded_at)
            extra.remove(oldest)
            _periods.pop(oldest, None)


def _current(range_: str) -> Period | None:
    period = _periods.get(range_)
    # A new day/week/month starts a different period.
    return period if period and period.start == period_bounds(range_)[0] else None


def get_period(range_: str) -> Period:
    mark_used()  # a person is looking at the dashboard
    period = _current(range_)
    if period:
        if period.stale:
            refresh_in_background(range_)
        return period
    with _state_lock:
        lock = _period_locks.setdefault(range_, threading.Lock())
    with lock:
        period = _current(range_)
        if period is None:
            period = load_period(range_)
            _publish(period)
        return period


def view(range_: str, team: str = "", wa_line: str = "all") -> View:
    return get_period(range_).view(team, wa_line)


def only_active(rows: list[dict], range_: str) -> list[dict]:
    """For past periods keep leads with messages in that period."""
    if not is_closed(range_):
        return rows
    active = {m.get("lead_id") for m in get_period(range_).messages}
    return [row for row in rows if row.get("id") in active]


def precompute(target: View) -> None:
    from api import agents, channels, leads, overview, quality
    for name, compute in (("overview", overview._overview_inner), ("channels", channels._channels_inner),
                          ("leads", leads._leads_inner), ("quality", quality._quality_inner),
                          ("agents", agents._agents_inner)):
        try:
            target.cached(name, compute)
        except Exception as exc:
            log.warning(f"precompute {name}/{target.period.range} failed: {exc}")


def refresh(range_: str) -> Period:
    """Load a new version and prepare it before users can see it."""
    period = load_period(range_)
    precompute(period.view())
    _publish(period)
    if range_ in TEAM_READY_RANGES:
        _prepare_teams(period)
    return period


def _prepare_teams(period: Period) -> None:
    from api import workspace
    try:
        names = [team["name"] for team in workspace.teams()["teams"]]
    except Exception as exc:
        log.warning(f"teams unavailable: {exc}")
        return
    for name in names:
        precompute(period.view(name))


def refresh_in_background(range_: str) -> None:
    with _state_lock:
        if range_ in _refreshing:
            return
        _refreshing.add(range_)

    def work():
        try:
            refresh(range_)
        except Exception as exc:
            log.warning(f"refresh {range_} failed: {exc}")
        finally:
            with _state_lock:
                _refreshing.discard(range_)

    threading.Thread(target=work, name=f"dataset-{range_}", daemon=True).start()


def warm() -> None:
    """Scheduled: keep the common periods ready (slowly when nobody is using the dashboard)."""
    idle = is_idle()
    for range_ in WARM_RANGES:
        period = _current(range_)
        if period is None or period.stale_for(idle):
            try:
                refresh(range_)
            except Exception as exc:
                log.warning(f"warm {range_} failed: {exc}")

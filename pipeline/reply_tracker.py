"""Pure logic for reply reminders (no database or network access).

Kept separate from pipeline/reminders.py so it can be unit-tested without
Supabase credentials.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo

# ── Banal customer messages ───────────────────────────────────────────────────
# A customer saying "ok" / "thanks" / sending a sticker does not need an answer.

_BANAL_MESSAGES = {
    "ok", "okay", "yes", "no", "thanks", "thank you", "done",
    "تمام",  # تمام
    "تم",  # تم
    "نعم",  # نعم
    "اي",  # اي
    "إي",  # إي
    "ايوه",  # ايوه
    "أيوه",  # أيوه
    "اوكي",  # اوكي
    "أوكي",  # أوكي
    "اوك",  # اوك
    "أوك",  # أوك
    "شكرا",  # شكرا
    "شكراً",  # شكراً
    "مشكور",  # مشكور
    "مشكوره",  # مشكوره
    "حسنا",  # حسنا
}
_BANAL_PREFIXES = (
    "ok", "okay", "thanks", "thank you", "done", "yes", "no",
    "تم", "تمام", "نعم", "اي", "إي",
    "ايوه", "أيوه", "اوكي", "أوكي",
    "شكرا", "شكراً", "شكرا استاذ",
    "مشكور", "مشكوره", "تسلم", "حسنا",
)
_MEDIA_PLACEHOLDERS = {"[reaction]", "[sticker]", "[image]", "[video]", "reaction", "sticker", "image", "video"}


def is_banal_message(body: str | None) -> bool:
    text = (body or "").strip()
    if not text:
        return True
    normalized = " ".join(text.lower().replace("ـ", "").split())
    normalized = normalized.strip(" .,!؟?؛;:()[]{}\"'`~|/\\")
    if normalized in _MEDIA_PLACEHOLDERS or normalized in _BANAL_MESSAGES:
        return True
    if re.fullmatch(r"[\d٠-٩]+", normalized):
        return True
    if len(normalized) <= 4 and not any(ch.isalnum() for ch in normalized):
        return True
    compact = re.sub(r"[\W_]+", " ", normalized, flags=re.UNICODE).strip()
    return len(compact) <= 24 and any(compact == p or compact.startswith(p + " ") for p in _BANAL_PREFIXES)


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def phone_key(phone: str | None) -> str | None:
    digits = re.sub(r"\D+", "", phone or "")
    return digits if len(digits) >= 7 else None


_DAY_NAMES = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def parse_work_days(value: str | None) -> frozenset[int]:
    """'mon-fri' or 'sun,mon,tue,wed,thu' -> weekday numbers (Mon=0)."""
    days: set[int] = set()
    for part in (value or "mon-fri").lower().replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            start, end = _DAY_NAMES[a[:3]], _DAY_NAMES[b[:3]]
            d = start
            while True:
                days.add(d)
                if d == end:
                    break
                d = (d + 1) % 7
        else:
            days.add(_DAY_NAMES[part[:3]])
    return frozenset(days)


# ── Working-hours clock ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class Schedule:
    tz: tzinfo
    work_days: frozenset[int]
    start_hour: int
    end_hour: int  # exclusive; 24 allowed

    def _window(self, day: date) -> tuple[datetime, datetime] | None:
        if day.weekday() not in self.work_days:
            return None
        start = datetime.combine(day, time(self.start_hour), self.tz)
        end = datetime.combine(day, time(0), self.tz) + timedelta(hours=self.end_hour)
        return start, end

    def is_working_time(self, moment: datetime) -> bool:
        local = moment.astimezone(self.tz)
        window = self._window(local.date())
        return bool(window and window[0] <= local < window[1])

    def working_minutes_between(self, start: datetime, end: datetime) -> float:
        """Minutes of working time between two instants (0 if end <= start)."""
        if end <= start:
            return 0.0
        s_local, e_local = start.astimezone(self.tz), end.astimezone(self.tz)
        total = 0.0
        day = s_local.date()
        # Waits are capped at a few days upstream; the bound is a safety net.
        for _ in range(400):
            if day > e_local.date():
                break
            window = self._window(day)
            if window:
                a, b = max(window[0], s_local), min(window[1], e_local)
                if b > a:
                    total += (b - a).total_seconds() / 60
            day += timedelta(days=1)
        return total


# ── Is the customer waiting? ──────────────────────────────────────────────────

@dataclass
class WaitState:
    waiting_since: datetime | None   # first unanswered meaningful inbound
    last_inbound: dict | None        # latest unanswered meaningful inbound message
    last_agent_contact_at: datetime | None
    last_agent_name: str | None


def compute_wait_state(messages: list[dict], call_times: list[datetime] | None = None) -> WaitState:
    """Walk one customer's timeline (messages from all their lead rows + calls).

    Any agent WhatsApp message or any Maqsam call with the customer's number
    counts as agent contact and closes the wait. Banal customer messages
    ("ok", "thanks", stickers) never open a wait.
    """
    events: list[tuple[datetime, str, dict | None]] = []
    for m in messages:
        dt = parse_dt(m.get("sent_at"))
        direction = (m.get("direction") or "").lower()
        if dt and direction in ("inbound", "outbound"):
            events.append((dt, direction, m))
    for ct in call_times or []:
        if ct:
            events.append((ct, "call", None))
    # On equal timestamps process agent contact last so it wins.
    order = {"inbound": 0, "outbound": 1, "call": 1}
    events.sort(key=lambda e: (e[0], order[e[1]]))

    waiting_since = None
    last_inbound = None
    last_contact = None
    last_agent = None
    for dt, kind, msg in events:
        if kind == "inbound":
            if is_banal_message(msg.get("body")):
                continue
            if waiting_since is None:
                waiting_since = dt
            last_inbound = msg
        else:
            waiting_since = None
            last_inbound = None
            last_contact = dt
            if kind == "outbound" and msg and msg.get("agent_name"):
                last_agent = msg.get("agent_name")
    return WaitState(waiting_since, last_inbound, last_contact, last_agent)


# ── Which reminder step is due? ───────────────────────────────────────────────

@dataclass(frozen=True)
class Ladder:
    first_min: float = 15
    second_min: float = 45
    escalate_min: float = 120
    min_gap_min: float = 10  # never two agent reminders closer than this (wall clock)


def due_actions(
    *,
    elapsed_working_min: float,
    reminder_count: int,
    last_reminded_at: datetime | None,
    escalated: bool,
    has_agent: bool,
    now: datetime,
    ladder: Ladder,
) -> set[str]:
    """Return {"remind"} / {"escalate"} / both / empty for one waiting customer."""
    actions: set[str] = set()
    if not has_agent:
        # Nobody owns the chat: go straight to the supervisor at the first step.
        if not escalated and elapsed_working_min >= ladder.first_min:
            actions.add("escalate")
        return actions

    if not escalated and elapsed_working_min >= ladder.escalate_min:
        actions.add("escalate")
        # The agent must still hear about it once, even if we only noticed late.
        if reminder_count == 0:
            actions.add("remind")
        return actions
    if escalated:
        return actions  # supervisor owns it now; no more agent pings

    gap_ok = last_reminded_at is None or (now - last_reminded_at) >= timedelta(minutes=ladder.min_gap_min)
    if reminder_count == 0 and elapsed_working_min >= ladder.first_min:
        actions.add("remind")
    elif reminder_count == 1 and elapsed_working_min >= ladder.second_min and gap_ok:
        actions.add("remind")
    return actions


def fmt_minutes(minutes: float) -> str:
    m = int(round(minutes))
    if m < 60:
        return f"{m} min"
    h, rem = divmod(m, 60)
    return f"{h}h {rem}m" if rem else f"{h}h"

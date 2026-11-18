from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


APP_TIMEZONE = os.getenv("APP_TIMEZONE") or os.getenv("REPORT_TIMEZONE") or "Etc/GMT-2"
APP_TIMEZONE_LABEL = os.getenv("APP_TIMEZONE_LABEL") or os.getenv("REPORT_TIMEZONE_LABEL") or "UTC+2"

# today | yesterday | week | month | 7d | 30d | custom:YYYY-MM-DD:YYYY-MM-DD
RANGE_PATTERN = r"^(today|yesterday|week|month|7d|30d|custom:\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2})$"
MAX_CUSTOM_DAYS = 366


def app_timezone() -> ZoneInfo:
    return ZoneInfo(APP_TIMEZONE)


def _midnight(value: datetime) -> datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def period_bounds(range_: str, now: datetime | None = None) -> tuple[datetime, datetime | None]:
    """(start, end) in UTC. end is exclusive; None means "up to now"."""
    tz = app_timezone()
    local = (now or datetime.now(timezone.utc)).astimezone(tz)
    today = _midnight(local)
    end = None
    custom = re.fullmatch(r"custom:(\d{4}-\d{2}-\d{2}):(\d{4}-\d{2}-\d{2})", range_ or "")
    if range_ == "today":
        start = today
    elif range_ == "yesterday":
        start, end = today - timedelta(days=1), today
    elif range_ in ("month", "30d"):
        start = today.replace(day=1)
    elif custom:
        try:
            first = datetime.strptime(custom.group(1), "%Y-%m-%d").replace(tzinfo=tz)
            last = datetime.strptime(custom.group(2), "%Y-%m-%d").replace(tzinfo=tz)
        except ValueError:
            first = last = today
        first, last = min(first, last), max(first, last)
        first = max(first, last - timedelta(days=MAX_CUSTOM_DAYS - 1))
        start, end = first, last + timedelta(days=1)
    else:  # week / 7d / anything unknown
        start = today - timedelta(days=today.weekday())
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc) if end else None


def period_start_utc(range_: str) -> datetime:
    """Return selected dashboard period start as UTC, using the app timezone."""
    return period_bounds(range_)[0]


def since_iso(range_: str) -> str:
    return period_start_utc(range_).isoformat()


def until_iso(range_: str) -> str:
    """Exclusive upper bound. Open periods end safely in the future."""
    end = period_bounds(range_)[1]
    return (end or datetime.now(timezone.utc) + timedelta(days=1)).isoformat()


def is_closed(range_: str, now: datetime | None = None) -> bool:
    """True when the whole period lies in the past."""
    end = period_bounds(range_, now)[1]
    return bool(end and end <= (now or datetime.now(timezone.utc)))

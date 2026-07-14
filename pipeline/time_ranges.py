from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


APP_TIMEZONE = os.getenv("APP_TIMEZONE") or os.getenv("REPORT_TIMEZONE") or "Etc/GMT-2"
APP_TIMEZONE_LABEL = os.getenv("APP_TIMEZONE_LABEL") or os.getenv("REPORT_TIMEZONE_LABEL") or "UTC+2"


def app_timezone() -> ZoneInfo:
    return ZoneInfo(APP_TIMEZONE)


def period_start_utc(range_: str) -> datetime:
    """Return selected dashboard period start as UTC, using the app timezone."""
    now = datetime.now(app_timezone())
    if range_ == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif range_ in ("week", "7d"):
        start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday())
    elif range_ in ("month", "30d"):
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday())
    return start.astimezone(timezone.utc)


def since_iso(range_: str) -> str:
    return period_start_utc(range_).isoformat()

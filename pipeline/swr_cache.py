"""Stale-while-revalidate cache for slow dashboard endpoints.

- Fresh entry (younger than ttl): returned directly.
- Stale entry: returned directly, and ONE background thread rebuilds it.
- No entry: computed in the request (first load after a restart).

So after the first load nobody waits for a multi-second (or multi-minute)
aggregation; numbers are at most one refresh behind.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable

log = logging.getLogger("fiper.cache")
MAX_ENTRIES = 200


class SWRCache:
    def __init__(self, name: str, ttl: float):
        self.name = name
        self.ttl = ttl
        self._data: dict[str, tuple[float, object]] = {}
        self._refreshing: set[str] = set()
        self._lock = threading.Lock()

    def get(self, key: str, compute: Callable[[], object], *, ttl: float | None = None,
            keep: Callable[[object], bool] = lambda d: True):
        ttl = self.ttl if ttl is None else ttl
        entry = self._data.get(key)
        now = time.time()
        if entry and now - entry[0] < ttl:
            return entry[1]
        if entry:
            self._refresh(key, compute, keep)
            return entry[1]
        value = compute()
        if keep(value):
            self._store(key, value)
        return value

    def _store(self, key: str, value) -> None:
        self._data[key] = (time.time(), value)
        while len(self._data) > MAX_ENTRIES:  # custom date ranges create many keys
            self._data.pop(min(self._data, key=lambda k: self._data[k][0]), None)

    def _refresh(self, key: str, compute, keep) -> None:
        with self._lock:
            if key in self._refreshing:
                return
            self._refreshing.add(key)

        def work():
            try:
                value = compute()
                if keep(value):
                    self._store(key, value)
            except Exception as exc:
                log.warning(f"[{self.name}] background refresh failed ({key}): {exc}")
            finally:
                with self._lock:
                    self._refreshing.discard(key)

        threading.Thread(target=work, name=f"{self.name}-{key}", daemon=True).start()

    def clear(self) -> None:
        self._data.clear()


def ttl_for(range_: str) -> float:
    """Shorter for 'today' (changes fast), longer for month."""
    return {"today": 120, "week": 300, "7d": 300}.get(range_, 600)

"""Server-side login for the dashboard (one shared account).

- Credentials only in Render env vars: DASHBOARD_USER (default "fiper") and
  DASHBOARD_PASSWORD. Without DASHBOARD_PASSWORD nobody can log in (fail closed).
- Session = signed, HttpOnly cookie (HMAC-SHA256). Changing DASHBOARD_PASSWORD
  logs everyone out. Lifetime: DASHBOARD_SESSION_DAYS (default 7).
- Every request is checked by `auth_middleware` except the paths in PUBLIC_*:
  the page shell, health checks, inbound webhooks, the QC chat lookup (used by
  the Fiper Quality Control app, returns no message content) and endpoints that
  check EMAIL_DEBUG_TOKEN themselves.
- Failed logins are rate-limited per IP.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from collections import defaultdict, deque

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

log = logging.getLogger("fiper.auth")
router = APIRouter()

COOKIE = "fiper_session"
PUBLIC_EXACT = {"/", "/health", "/ping", "/api/login", "/api/logout", "/api/qc/chat-exists", "/telegram-webhook"}
PUBLIC_PREFIXES = (
    "/static/",            # page assets + email logo (emails load it without a session)
    "/webhook/",           # ManyContacts / Meta / n8n inbound webhooks
    "/api/debug/email/",   # protected by EMAIL_DEBUG_TOKEN in the handler
    "/api/reminders/",     # protected by EMAIL_DEBUG_TOKEN in the handler
    "/api/debug/agent-telegram-status",  # EMAIL_DEBUG_TOKEN
)

MAX_FAILS = 8          # per IP ...
FAIL_WINDOW = 15 * 60  # ... within 15 minutes
_fails: dict[str, deque] = defaultdict(deque)


def _user() -> str:
    return os.getenv("DASHBOARD_USER", "fiper").strip()


def _password() -> str:
    return os.getenv("DASHBOARD_PASSWORD", "")


def _secret() -> bytes:
    explicit = os.getenv("SESSION_SECRET", "")
    base = explicit or (_password() + "|" + os.getenv("SUPABASE_SERVICE_KEY", ""))
    return hashlib.sha256(("fiper-dashboard-session|" + base).encode()).digest()


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def make_token(user: str, now: float | None = None) -> str:
    days = float(os.getenv("DASHBOARD_SESSION_DAYS", "7"))
    issued = time.time() if now is None else now
    payload = _b64(json.dumps({"u": user, "exp": int(issued + days * 86400)}).encode())
    sig = _b64(hmac.new(_secret(), payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{sig}"


def read_token(token: str | None, now: float | None = None) -> str | None:
    """-> user name if the token is valid and not expired."""
    if not token or "." not in token or not _password():
        return None
    payload, sig = token.rsplit(".", 1)
    expected = _b64(hmac.new(_secret(), payload.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        data = json.loads(_unb64(payload))
    except Exception:
        return None
    if int(data.get("exp", 0)) < (time.time() if now is None else now):
        return None
    return data.get("u")


def is_public(path: str) -> bool:
    return path in PUBLIC_EXACT or path.startswith(PUBLIC_PREFIXES)


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    return (fwd.split(",")[0].strip() if fwd else "") or (request.client.host if request.client else "?")


def _is_https(request: Request) -> bool:
    return request.url.scheme == "https" or request.headers.get("x-forwarded-proto", "") == "https"


async def auth_middleware(request: Request, call_next):
    if request.method == "OPTIONS" or is_public(request.url.path):
        return await call_next(request)
    if read_token(request.cookies.get(COOKIE)):
        return await call_next(request)
    return JSONResponse({"detail": "login required"}, status_code=401)


@router.post("/api/login")
async def login(request: Request):
    ip = _client_ip(request)
    now = time.time()
    fails = _fails[ip]
    while fails and now - fails[0] > FAIL_WINDOW:
        fails.popleft()
    if len(fails) >= MAX_FAILS:
        raise HTTPException(status_code=429, detail="Too many attempts. Try again in 15 minutes.")
    try:
        body = await request.json()
    except Exception:
        body = {}
    user = str(body.get("username") or "").strip()
    password = str(body.get("password") or "")
    expected = _password()
    ok = bool(expected) and hmac.compare_digest(user.lower(), _user().lower()) and hmac.compare_digest(
        password.encode(), expected.encode())
    if not ok:
        fails.append(now)
        if not expected:
            log.error("[auth] DASHBOARD_PASSWORD is not set - nobody can log in")
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    fails.clear()
    response = JSONResponse({"ok": True, "user": _user()})
    response.set_cookie(
        COOKIE, make_token(_user()), max_age=int(float(os.getenv("DASHBOARD_SESSION_DAYS", "7")) * 86400),
        httponly=True, secure=_is_https(request), samesite="lax", path="/",
    )
    return response


@router.post("/api/logout")
def logout():
    response = JSONResponse({"ok": True})
    response.delete_cookie(COOKIE, path="/")
    return response


@router.get("/api/me")
def me(request: Request):
    user = read_token(request.cookies.get(COOKIE))
    if not user:
        raise HTTPException(status_code=401, detail="login required")
    return {"user": user}

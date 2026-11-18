"""Run: python -m unittest discover -s tests -v   (offline; no real credentials)"""
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")
os.environ["DASHBOARD_USER"] = "fiper"
os.environ["DASHBOARD_PASSWORD"] = "correct horse battery"

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from api import auth  # noqa: E402


class AuthTest(unittest.TestCase):
    def setUp(self):
        auth._fails.clear()
        self.client = TestClient(main.app)  # no lifespan -> scheduler never starts

    def login(self, user="fiper", password="correct horse battery"):
        return self.client.post("/api/login", json={"username": user, "password": password})

    def test_data_requires_login(self):
        for path in ["/api/overview?range=today", "/api/leads", "/api/leads/journey/v2/export", "/api/debug/webhooks",
                     "/api/waiting", "/api/status", "/docs", "/openapi.json"]:
            self.assertEqual(self.client.get(path).status_code, 401, path)
        for path in ["/api/run-pipeline", "/api/trigger-ai-analysis", "/api/agents/performance"]:
            self.assertEqual(self.client.post(path).status_code, 401, path)

    def test_public_paths_stay_open(self):
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/health").status_code, 200)
        self.assertEqual(self.client.get("/static/email/fiper-logo-white.png").status_code, 200)
        self.assertNotEqual(self.client.get("/webhook/manycontacts").status_code, 401)
        self.assertNotEqual(self.client.post("/api/qc/chat-exists", json={}).status_code, 401)
        # token-protected endpoints answer 403 (bad token), not 401
        self.assertEqual(self.client.get("/api/reminders/status").status_code, 403)

    def test_page_contains_no_credentials(self):
        html = self.client.get("/").text
        self.assertNotIn("Fiper2026", html)
        self.assertNotIn("correct horse battery", html)

    def test_login_flow(self):
        self.assertEqual(self.login(password="wrong").status_code, 401)
        self.assertEqual(self.client.get("/api/me").status_code, 401)
        r = self.login(user="FIPER")  # user name is case-insensitive
        self.assertEqual(r.status_code, 200)
        cookie = r.headers.get("set-cookie", "")
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=lax", cookie)
        self.assertEqual(self.client.get("/api/me").json()["user"], "fiper")
        self.client.post("/api/logout")
        self.assertEqual(self.client.get("/api/me").status_code, 401)

    def test_tampered_or_expired_token_rejected(self):
        token = auth.make_token("fiper")
        self.assertEqual(auth.read_token(token), "fiper")
        self.assertIsNone(auth.read_token(token[:-2] + "xx"))
        self.assertIsNone(auth.read_token(auth.make_token("fiper", now=0)))  # expired long ago
        self.client.cookies.set(auth.COOKIE, token[:-2] + "xx")
        self.assertEqual(self.client.get("/api/me").status_code, 401)

    def test_rate_limit(self):
        for _ in range(auth.MAX_FAILS):
            self.login(password="nope")
        self.assertEqual(self.login().status_code, 429)  # even the right password is blocked for a while

    def test_no_password_configured_means_nobody_gets_in(self):
        saved = os.environ.pop("DASHBOARD_PASSWORD")
        try:
            self.assertEqual(self.login(password="").status_code, 401)
            self.assertIsNone(auth.read_token(auth.make_token("fiper")))
        finally:
            os.environ["DASHBOARD_PASSWORD"] = saved


if __name__ == "__main__":
    unittest.main()

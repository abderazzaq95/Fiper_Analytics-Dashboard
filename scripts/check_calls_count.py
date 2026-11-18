import asyncio, os, base64
import httpx
from dotenv import load_dotenv
load_dotenv()

SB = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_KEY")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Prefer": "count=exact"}

MQ_KEY = os.getenv("MAQSAM_ACCESS_KEY")
MQ_SEC = os.getenv("MAQSAM_ACCESS_SECRET")
tok = base64.b64encode(f"{MQ_KEY}:{MQ_SEC}".encode()).decode()
MH = {"Authorization": f"Basic {tok}", "Content-Type": "application/json"}

async def run():
    async with httpx.AsyncClient(timeout=30) as c:
        # Count calls in DB
        r = await c.get(f"{SB}/rest/v1/calls", headers=H,
                        params={"select": "id", "limit": 1})
        print(f"DB calls count: {r.headers.get('content-range', 'unknown')}")

        # Count messages by direction
        for direction in ("inbound", "outbound"):
            r2 = await c.get(f"{SB}/rest/v1/messages", headers=H,
                             params={"direction": f"eq.{direction}", "select": "id", "limit": 1})
            print(f"DB messages {direction}: {r2.headers.get('content-range', 'unknown')}")

        # Messages with agent_name set
        r3 = await c.get(f"{SB}/rest/v1/messages", headers=H,
                         params={"agent_name": "not.is.null", "select": "id,direction,agent_name,body,sent_at",
                                 "limit": 10, "order": "sent_at.desc"})
        rows = r3.json()
        print(f"\nLast 10 messages with agent_name set:")
        for row in rows:
            print(f"  dir={row['direction']}  agent={row['agent_name']}  body={str(row.get('body',''))[:50]}  at={row.get('sent_at','')[:16]}")

        # Check Maqsam first page to confirm pagination works
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        d_from = (now - timedelta(days=90)).strftime("%Y-%m-%d")
        d_to = now.strftime("%Y-%m-%d")
        r4 = await c.get("https://api.maqsam.com/v2/calls", headers=MH,
                         params={"date_from": d_from, "date_to": d_to, "page": 1})
        batch = r4.json()
        if isinstance(batch, dict):
            batch = batch.get("message", batch.get("data", []))
        print(f"\nMaqsam page 1 returns: {len(batch)} calls")

asyncio.run(run())

import asyncio
import os
import base64
from datetime import datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

SB = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_KEY")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}

MQ_KEY = os.getenv("MAQSAM_ACCESS_KEY")
MQ_SEC = os.getenv("MAQSAM_ACCESS_SECRET")
tok = base64.b64encode(f"{MQ_KEY}:{MQ_SEC}".encode()).decode()
MH = {"Authorization": f"Basic {tok}", "Content-Type": "application/json"}


async def run():
    async with httpx.AsyncClient(timeout=30) as c:

        # 1. Open alerts
        r = await c.get(f"{SB}/rest/v1/alerts", headers=H,
                        params={"resolved": "eq.false", "select": "id,agent_name,severity,type,lead_id"})
        alerts = r.json()
        print("=== OPEN ALERTS ===")
        for a in alerts:
            print(f"  id={str(a['id'])[:8]}  agent_name={a['agent_name']!r}  "
                  f"severity={a['severity']}  type={a['type']}  "
                  f"lead_id={str(a.get('lead_id',''))[:8]}")

        # 2. Distinct assigned_agent from leads
        r2 = await c.get(f"{SB}/rest/v1/leads", headers=H,
                         params={"assigned_agent": "not.is.null", "select": "assigned_agent", "limit": 500})
        agents_in_leads = sorted(set(x["assigned_agent"] for x in r2.json() if x.get("assigned_agent")))
        print(f"\n=== DISTINCT assigned_agent IN LEADS ({len(agents_in_leads)}) ===")
        for a in agents_in_leads:
            print(f"  {a!r}")

        # 3. Distinct agent_name from calls
        r3 = await c.get(f"{SB}/rest/v1/calls", headers=H,
                         params={"agent_name": "not.is.null", "select": "agent_name", "limit": 2000})
        agents_in_calls = sorted(set(x["agent_name"] for x in r3.json() if x.get("agent_name")))
        print(f"\n=== DISTINCT agent_name IN CALLS ({len(agents_in_calls)}) ===")
        for a in agents_in_calls:
            print(f"  {a!r}")

        # 4. Distinct agent_name from messages
        r4 = await c.get(f"{SB}/rest/v1/messages", headers=H,
                         params={"agent_name": "not.is.null", "select": "agent_name", "limit": 2000})
        agents_in_msgs = sorted(set(x["agent_name"] for x in r4.json() if x.get("agent_name")))
        print(f"\n=== DISTINCT agent_name IN MESSAGES ({len(agents_in_msgs)}) ===")
        for a in agents_in_msgs:
            print(f"  {a!r}")

        # 5. Alert lead_ids → look up assigned_agent for each
        alert_lead_ids = [a["lead_id"] for a in alerts if a.get("lead_id")]
        if alert_lead_ids:
            print(f"\n=== LEADS FOR ALERT lead_ids ===")
            for lid in alert_lead_ids:
                rl = await c.get(f"{SB}/rest/v1/leads", headers=H,
                                 params={"id": f"eq.{lid}", "select": "id,assigned_agent,phone,channel"})
                rows = rl.json()
                if rows:
                    row = rows[0]
                    print(f"  lead_id={str(lid)[:8]}  assigned_agent={row.get('assigned_agent')!r}  "
                          f"phone={row.get('phone')}  channel={row.get('channel')}")
                    # Also check messages for this lead
                    rm = await c.get(f"{SB}/rest/v1/messages", headers=H,
                                     params={"lead_id": f"eq.{lid}", "direction": "eq.outbound",
                                             "select": "agent_name", "limit": 10})
                    msg_agents = sorted(set(x["agent_name"] for x in rm.json() if x.get("agent_name")))
                    print(f"    outbound message agents: {msg_agents}")
                    # And calls
                    rc = await c.get(f"{SB}/rest/v1/calls", headers=H,
                                     params={"lead_id": f"eq.{lid}", "select": "agent_name", "limit": 10})
                    call_agents = sorted(set(x["agent_name"] for x in rc.json() if x.get("agent_name")))
                    print(f"    call agents: {call_agents}")

        # 6. Maqsam call count (90 days, paginate all)
        now = datetime.now(timezone.utc)
        d_from = (now - timedelta(days=90)).strftime("%Y-%m-%d")
        d_to = now.strftime("%Y-%m-%d")
        print(f"\n=== MAQSAM CALL COUNT (90 days: {d_from} to {d_to}) ===")
        total = 0
        for pg in range(1, 100):
            r5 = await c.get("https://api.maqsam.com/v2/calls", headers=MH,
                             params={"date_from": d_from, "date_to": d_to, "page": pg})
            data = r5.json()
            batch = data if isinstance(data, list) else data.get("message", data.get("data", []))
            print(f"  page {pg:2d}: {len(batch)} calls")
            total += len(batch)
            if len(batch) < 100:
                break
        print(f"  TOTAL: {total} calls over 90 days")


if __name__ == "__main__":
    asyncio.run(run())

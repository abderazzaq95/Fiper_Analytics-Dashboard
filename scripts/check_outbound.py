"""
Investigate outbound message tracking.
Checks: what events ManyContacts webhook actually sends us.
"""
import asyncio, os, base64, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import httpx
from dotenv import load_dotenv
load_dotenv()

SB = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_KEY")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Prefer": "count=exact"}

MQ_KEY = os.getenv("MAQSAM_ACCESS_KEY")
MQ_SEC = os.getenv("MAQSAM_ACCESS_SECRET")
tok = base64.b64encode(f"{MQ_KEY}:{MQ_SEC}".encode()).decode()
MQ_H = {"apikey": MQ_KEY}

async def run():
    async with httpx.AsyncClient(timeout=20) as c:
        # 1. All messages — directions and agent_name
        r = await c.get(f"{SB}/rest/v1/messages", headers=H,
                        params={"select": "direction,agent_name,body,sent_at,wa_message_id",
                                "order": "sent_at.desc", "limit": 20})
        rows = r.json()
        total = r.headers.get("content-range", "?")
        print(f"=== All messages (total: {total}) — last 20 ===")
        for row in rows:
            print(f"  dir={row['direction']:8s} agent={str(row.get('agent_name',''))[:20]:20s} "
                  f"msg_id={str(row.get('wa_message_id',''))[:20]}  body={str(row.get('body',''))[:40]}")

        # 2. Check ManyContacts webhook config endpoint (if available)
        print("\n=== ManyContacts webhook config ===")
        try:
            r2 = await c.get("https://api.manycontacts.com/v1/webhooks",
                             headers={"apikey": MQ_KEY}, timeout=10)
            print(f"  status={r2.status_code}  body={r2.text[:300]}")
        except Exception as e:
            print(f"  error: {e}")

        # 3. Check ManyContacts API for outbound event types
        print("\n=== ManyContacts contact messages for a known contact ===")
        # Pick first known contact from DB
        rc = await c.get(f"{SB}/rest/v1/leads", headers={**H, "Prefer": ""},
                         params={"channel": "eq.whatsapp", "select": "wa_contact_id,name",
                                 "limit": 5, "order": "last_message_at.desc"})
        leads = rc.json()
        for lead in leads[:3]:
            cid = lead.get("wa_contact_id")
            print(f"\n  Contact: {lead.get('name')} ({cid})")
            try:
                rm = await c.get(f"https://api.manycontacts.com/v1/contact/{cid}/messages",
                                 headers={"apikey": MQ_KEY}, timeout=10)
                print(f"    /messages status={rm.status_code}  body={rm.text[:200]}")
            except Exception as e:
                print(f"    /messages error: {e}")

            # Also try /v1/contact/{id}/chats
            try:
                rc2 = await c.get(f"https://api.manycontacts.com/v1/contact/{cid}/chats",
                                  headers={"apikey": MQ_KEY}, timeout=10)
                print(f"    /chats    status={rc2.status_code}  body={rc2.text[:200]}")
            except Exception as e:
                print(f"    /chats error: {e}")

            # Try /v1/contact/{id}/notes
            try:
                rn = await c.get(f"https://api.manycontacts.com/v1/contact/{cid}/notes",
                                  headers={"apikey": MQ_KEY}, timeout=10)
                print(f"    /notes    status={rn.status_code}  body={rn.text[:200]}")
            except Exception as e:
                print(f"    /notes error: {e}")

        # 4. Discover what ManyContacts API root exposes
        print("\n=== ManyContacts /v1 root ===")
        try:
            rroot = await c.get("https://api.manycontacts.com/v1",
                                headers={"apikey": MQ_KEY}, timeout=10)
            print(f"  status={rroot.status_code}  body={rroot.text[:500]}")
        except Exception as e:
            print(f"  error: {e}")

asyncio.run(run())

import asyncio
import sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from pipeline import whatsapp, maqsam


async def test():
    print("--- ManyContacts users ---")
    users = await whatsapp.fetch_users()
    print(f"  {len(users)} agents loaded")
    for u in users[:5]:
        print(f"  {u['name']} (id: {u['id'][:8]}...)")

    print()
    print("--- ManyContacts contacts (last 2 days) ---")
    contacts = await whatsapp.fetch_contacts("2026-05-10", "2026-05-12")
    print(f"  {len(contacts)} contacts returned")
    for c in contacts[:3]:
        agent = whatsapp.resolve_agent_name(c.get("last_user_id"))
        print(f"  {c['name']} | {c['number']} | agent: {agent}")

    print()
    print("--- Maqsam calls (May 2026) ---")
    calls = await maqsam.fetch_calls("2026-05-01", "2026-05-12")
    print(f"  {len(calls)} calls returned")
    for c in calls[:3]:
        agent = maqsam.extract_agent_name(c)
        callee = c.get("calleeNumber", "?")
        dur = c.get("duration")
        state = c.get("state")
        print(f"  agent: {agent} | callee: {callee} | dur: {dur}s | state: {state}")


asyncio.run(test())

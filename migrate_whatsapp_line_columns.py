"""
Add WhatsApp line metadata columns used by the dashboard to split the two
ManyContacts numbers.
Tries the Supabase Management API first; if that fails, prints SQL for the
Dashboard SQL editor.
"""

import os
import sys
import httpx
from dotenv import load_dotenv

load_dotenv()

SB_URL = os.getenv("SUPABASE_URL", "")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
PROJECT_REF = SB_URL.replace("https://", "").split(".")[0]

SQL = """
ALTER TABLE leads ADD COLUMN IF NOT EXISTS whatsapp_business_number TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS whatsapp_business_number TEXT;
CREATE INDEX IF NOT EXISTS idx_messages_wa_line ON messages(whatsapp_business_number);
CREATE INDEX IF NOT EXISTS idx_leads_wa_line ON leads(whatsapp_business_number);
"""


def try_management_api():
    url = f"https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query"
    headers = {
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    stmts = [s.strip() for s in SQL.strip().split(";") if s.strip()]
    errors = []
    for stmt in stmts:
        try:
            r = httpx.post(url, headers=headers, json={"query": stmt + ";"}, timeout=15)
            if r.status_code not in (200, 201):
                errors.append(f"{stmt[:60]}: HTTP {r.status_code} ? {r.text[:200]}")
        except Exception as e:
            errors.append(str(e))
    return errors


def try_rpc_exec(stmt: str) -> bool:
    url = f"{SB_URL}/rest/v1/rpc/exec_sql"
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    try:
        r = httpx.post(url, headers=headers, json={"sql": stmt}, timeout=15)
        return r.status_code in (200, 201, 204)
    except Exception:
        return False


if __name__ == "__main__":
    print(f"Project ref: {PROJECT_REF}")
    print("Trying Supabase Management API...")
    errors = try_management_api()

    if not errors:
        print("\nAll WhatsApp line columns added successfully via Management API.")
        sys.exit(0)

    print(f"\nManagement API failed ({len(errors)} errors). Trying RPC fallback...")
    stmts = [s.strip() for s in SQL.strip().split(";") if s.strip()]
    rpc_ok = all(try_rpc_exec(s + ";") for s in stmts)

    if rpc_ok:
        print("All WhatsApp line columns added via RPC.")
        sys.exit(0)

    print("\n" + "=" * 60)
    print("AUTO-MIGRATION FAILED - run this SQL in Supabase Dashboard:")
    print("Dashboard -> SQL Editor -> New Query -> paste -> Run")
    print("=" * 60)
    print(SQL)
    print("=" * 60)
    print("\nSteps:")
    print("  1. Go to https://supabase.com/dashboard/project/" + PROJECT_REF + "/sql/new")
    print("  2. Paste the SQL above")
    print("  3. Click Run")
    print("  4. Then refresh the dashboard")

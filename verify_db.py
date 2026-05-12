"""Run after executing schema.sql in Supabase to confirm all tables exist."""
import sys
sys.path.insert(0, ".")
from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()

client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

TABLES = ["leads", "messages", "calls", "ai_analysis", "alerts"]
all_ok = True

for table in TABLES:
    try:
        result = client.table(table).select("id").limit(1).execute()
        print(f"  {table:20s} OK")
    except Exception as e:
        print(f"  {table:20s} MISSING — {e}")
        all_ok = False

print()
print("All tables ready." if all_ok else "Some tables are missing — re-run schema.sql.")

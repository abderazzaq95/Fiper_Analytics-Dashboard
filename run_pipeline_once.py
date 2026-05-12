"""One-shot pipeline run — syncs ManyContacts + Maqsam into Supabase."""
import asyncio
import sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

from main import run_pipeline

async def main():
    print("Running pipeline...")
    await run_pipeline()
    print("Done.")

asyncio.run(main())

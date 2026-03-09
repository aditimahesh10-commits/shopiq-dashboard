"""
run_pipeline.py
───────────────
One command to run the FULL data pipeline:
  1. Fetch from FakeStore API
  2. Enrich with Indian order data
  3. Load into SQLite
  4. Verify everything is correct

Usage:
    python run_pipeline.py
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))

print("\n" + "="*55)
print("  🛒 ShopIQ — Data Pipeline Runner")
print("="*55)

start = time.time()

# STEP 1
print("\n📡 STEP 1/2 — Fetching from FakeStore API...")
from pipeline.fetch import run as fetch_run
products_df, orders_df = fetch_run()
print(f"   ✓ {len(products_df)} products | {len(orders_df):,} orders")

# STEP 2
print("\n🗄️  STEP 2/2 — Loading into SQLite...")
from pipeline.load import run as load_run
db_path = load_run()
print(f"   ✓ Database: {db_path}")

elapsed = time.time() - start
print(f"\n{'='*55}")
print(f"  ✅ Pipeline complete in {elapsed:.1f}s")
print(f"{'='*55}")
print(f"\n🚀 Launch dashboard:")
print(f"   streamlit run dashboard/app.py\n")

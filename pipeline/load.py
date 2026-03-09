"""
pipeline/load.py
────────────────
Step 2 of the data pipeline.
Takes raw CSVs from fetch.py and loads them into
a structured SQLite database with proper schema,
indexes, and views — exactly like a real data warehouse.

Tables:
  dim_products   → product master data (from FakeStore API)
  dim_customers  → customer dimension
  fact_orders    → all order transactions (main fact table)

Views (pre-built SQL queries):
  vw_monthly_revenue    → monthly GMV aggregation
  vw_category_summary   → category-level KPIs
  vw_customer_rfm       → RFM scores per customer
  vw_city_performance   → geo analytics
  vw_ab_test_results    → A/B group comparison
"""

import sqlite3
import pandas as pd
import os
import logging
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [LOAD] %(message)s')
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH  = os.path.join(DATA_DIR, 'shopiq.db')


def get_connection() -> sqlite3.Connection:
    """Return a SQLite connection with row factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # faster writes
    conn.execute("PRAGMA foreign_keys=OFF")   # disable FK during bulk load
    return conn


def create_schema(conn: sqlite3.Connection):
    """Create all tables, indexes and views."""
    log.info("Creating schema...")
    conn.executescript("""
    -- ── DIMENSION: PRODUCTS ──────────────────────────────────
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id       INTEGER PRIMARY KEY,
        product_name     TEXT    NOT NULL,
        category         TEXT,
        category_mapped  TEXT,
        price_usd        REAL,
        price_inr        REAL,
        rating_score     REAL,
        rating_count     INTEGER,
        description      TEXT,
        loaded_at        TEXT    DEFAULT (datetime('now'))
    );

    -- ── DIMENSION: CUSTOMERS ─────────────────────────────────
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id      INTEGER PRIMARY KEY,
        age_group        TEXT,
        city             TEXT,
        state            TEXT,
        zone             TEXT,
        tier             TEXT,
        first_order_date TEXT,
        last_order_date  TEXT,
        total_orders     INTEGER DEFAULT 0,
        total_spend      REAL    DEFAULT 0,
        loaded_at        TEXT    DEFAULT (datetime('now'))
    );

    -- ── FACT: ORDERS ──────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id         TEXT    PRIMARY KEY,
        customer_id      INTEGER,
        product_id       INTEGER,
        product_name     TEXT,
        category         TEXT,
        date             TEXT    NOT NULL,
        month            TEXT,
        month_name       TEXT,
        week             INTEGER,
        year             INTEGER,
        dow              TEXT,
        city             TEXT,
        state            TEXT,
        zone             TEXT,
        tier             TEXT,
        quantity         INTEGER DEFAULT 1,
        unit_price       REAL,
        discount_pct     REAL    DEFAULT 0,
        amount           INTEGER NOT NULL,
        payment_method   TEXT,
        status           TEXT,
        age_group        TEXT,
        traffic_source   TEXT,
        delivery_days    INTEGER,
        is_returned      INTEGER DEFAULT 0,
        ab_group         TEXT,
        loaded_at        TEXT    DEFAULT (datetime('now')),
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_id)  REFERENCES dim_products(product_id)
    );

    -- ── INDEXES ───────────────────────────────────────────────
    CREATE INDEX IF NOT EXISTS idx_orders_date     ON fact_orders(date);
    CREATE INDEX IF NOT EXISTS idx_orders_customer ON fact_orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_category ON fact_orders(category);
    CREATE INDEX IF NOT EXISTS idx_orders_city     ON fact_orders(city);
    CREATE INDEX IF NOT EXISTS idx_orders_status   ON fact_orders(status);
    CREATE INDEX IF NOT EXISTS idx_orders_month    ON fact_orders(month);
    CREATE INDEX IF NOT EXISTS idx_orders_ab       ON fact_orders(ab_group);

    -- ── VIEW: MONTHLY REVENUE ─────────────────────────────────
    CREATE VIEW IF NOT EXISTS vw_monthly_revenue AS
    SELECT
        month,
        month_name,
        COUNT(order_id)              AS total_orders,
        SUM(amount)                  AS total_revenue,
        ROUND(AVG(amount), 2)        AS avg_order_value,
        SUM(is_returned)             AS total_returns,
        ROUND(AVG(is_returned)*100, 2) AS return_rate_pct,
        COUNT(DISTINCT customer_id)  AS unique_customers
    FROM fact_orders
    GROUP BY month
    ORDER BY month;

    -- ── VIEW: CATEGORY SUMMARY ────────────────────────────────
    CREATE VIEW IF NOT EXISTS vw_category_summary AS
    SELECT
        category,
        COUNT(order_id)               AS total_orders,
        SUM(amount)                   AS total_revenue,
        ROUND(AVG(amount), 2)         AS avg_order_value,
        ROUND(AVG(discount_pct), 2)   AS avg_discount_pct,
        ROUND(AVG(is_returned)*100,2) AS return_rate_pct,
        COUNT(DISTINCT customer_id)   AS unique_customers,
        COUNT(DISTINCT product_id)    AS unique_products
    FROM fact_orders
    GROUP BY category
    ORDER BY total_revenue DESC;

    -- ── VIEW: CUSTOMER RFM ────────────────────────────────────
    CREATE VIEW IF NOT EXISTS vw_customer_rfm AS
    SELECT
        customer_id,
        COUNT(order_id)              AS frequency,
        SUM(amount)                  AS monetary,
        MAX(date)                    AS last_order_date,
        MIN(date)                    AS first_order_date,
        ROUND(AVG(amount), 2)        AS avg_order_value,
        COUNT(DISTINCT category)     AS categories_bought,
        SUM(is_returned)             AS total_returns
    FROM fact_orders
    GROUP BY customer_id;

    -- ── VIEW: CITY PERFORMANCE ────────────────────────────────
    CREATE VIEW IF NOT EXISTS vw_city_performance AS
    SELECT
        city, state, zone, tier,
        COUNT(order_id)              AS total_orders,
        SUM(amount)                  AS total_revenue,
        ROUND(AVG(amount), 2)        AS avg_order_value,
        ROUND(AVG(delivery_days),2)  AS avg_delivery_days,
        ROUND(AVG(is_returned)*100,2)AS return_rate_pct,
        COUNT(DISTINCT customer_id)  AS unique_customers
    FROM fact_orders
    GROUP BY city
    ORDER BY total_revenue DESC;

    -- ── VIEW: A/B TEST RESULTS ────────────────────────────────
    CREATE VIEW IF NOT EXISTS vw_ab_test_results AS
    SELECT
        ab_group,
        COUNT(order_id)               AS total_orders,
        SUM(amount)                   AS total_revenue,
        ROUND(AVG(amount), 2)         AS avg_order_value,
        ROUND(AVG(is_returned)*100,2) AS return_rate_pct,
        COUNT(DISTINCT customer_id)   AS unique_customers,
        ROUND(AVG(discount_pct), 2)   AS avg_discount_pct,
        ROUND(AVG(delivery_days), 2)  AS avg_delivery_days
    FROM fact_orders
    GROUP BY ab_group;

    -- ── VIEW: PAYMENT SUMMARY ─────────────────────────────────
    CREATE VIEW IF NOT EXISTS vw_payment_summary AS
    SELECT
        payment_method,
        COUNT(order_id)         AS total_orders,
        SUM(amount)             AS total_revenue,
        ROUND(AVG(amount), 2)   AS avg_order_value,
        ROUND(COUNT(order_id) * 100.0 / (SELECT COUNT(*) FROM fact_orders), 2) AS pct_of_orders
    FROM fact_orders
    GROUP BY payment_method
    ORDER BY total_orders DESC;

    -- ── VIEW: WEEKLY TREND ────────────────────────────────────
    CREATE VIEW IF NOT EXISTS vw_weekly_trend AS
    SELECT
        year, week,
        COUNT(order_id)    AS total_orders,
        SUM(amount)        AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_order_value
    FROM fact_orders
    GROUP BY year, week
    ORDER BY year, week;
    """)
    conn.commit()
    log.info("Schema created ✓")


def load_products(conn: sqlite3.Connection, df: pd.DataFrame):
    """Upsert products into dim_products."""
    log.info(f"Loading {len(df)} products...")
    conn.execute("DELETE FROM dim_products")
    df[[
        'product_id','product_name','category','category_mapped',
        'price_usd','price_inr','rating_score','rating_count','description'
    ]].to_sql('dim_products', conn, if_exists='append', index=False)
    log.info(f"✓ dim_products: {len(df)} rows")


def load_orders(conn: sqlite3.Connection, df: pd.DataFrame):
    """Load all orders into fact_orders."""
    log.info(f"Loading {len(df):,} orders...")
    conn.execute("DELETE FROM fact_orders")
    df['date'] = df['date'].astype(str)

    cols = [
        'order_id','customer_id','product_id','product_name','category',
        'date','month','month_name','week','year','dow',
        'city','state','zone','tier','quantity','unit_price',
        'discount_pct','amount','payment_method','status',
        'age_group','traffic_source','delivery_days','is_returned','ab_group'
    ]
    df[cols].to_sql('fact_orders', conn, if_exists='append', index=False)
    log.info(f"✓ fact_orders: {len(df):,} rows")


def load_customers(conn: sqlite3.Connection, df: pd.DataFrame):
    """Build and load customer dimension from orders."""
    log.info("Building customer dimension...")
    cust = df.groupby('customer_id').agg(
        age_group        = ('age_group', 'first'),
        city             = ('city', 'first'),
        state            = ('state', 'first'),
        zone             = ('zone', 'first'),
        tier             = ('tier', 'first'),
        first_order_date = ('date', 'min'),
        last_order_date  = ('date', 'max'),
        total_orders     = ('order_id', 'count'),
        total_spend      = ('amount', 'sum'),
    ).reset_index()
    cust['first_order_date'] = cust['first_order_date'].astype(str)
    cust['last_order_date']  = cust['last_order_date'].astype(str)

    conn.execute("DELETE FROM dim_customers")
    cust.to_sql('dim_customers', conn, if_exists='append', index=False)
    log.info(f"✓ dim_customers: {len(cust):,} rows")


def verify(conn: sqlite3.Connection):
    """Run quick sanity checks on the loaded data."""
    log.info("Running verification queries...")
    checks = {
        "Total orders":    "SELECT COUNT(*) FROM fact_orders",
        "Total revenue":   "SELECT ROUND(SUM(amount)/1e7, 2) || ' Cr' FROM fact_orders",
        "Unique customers":"SELECT COUNT(DISTINCT customer_id) FROM fact_orders",
        "Categories":      "SELECT COUNT(DISTINCT category) FROM fact_orders",
        "Date range":      "SELECT MIN(date) || ' → ' || MAX(date) FROM fact_orders",
        "Views created":   "SELECT COUNT(*) FROM sqlite_master WHERE type='view'",
        "Indexes created": "SELECT COUNT(*) FROM sqlite_master WHERE type='index'",
    }
    results = {}
    for label, query in checks.items():
        val = conn.execute(query).fetchone()[0]
        log.info(f"  {label}: {val}")
        results[label] = val
    return results


def run():
    """Full load pipeline: CSVs → SQLite."""
    log.info("=" * 50)
    log.info("LOAD PIPELINE STARTING")
    log.info("=" * 50)

    products_path = os.path.join(DATA_DIR, 'products.csv')
    orders_path   = os.path.join(DATA_DIR, 'orders.csv')

    if not os.path.exists(orders_path):
        raise FileNotFoundError("orders.csv not found. Run pipeline/fetch.py first.")

    products_df = pd.read_csv(products_path)
    orders_df   = pd.read_csv(orders_path, parse_dates=['date'])

    conn = get_connection()
    create_schema(conn)
    load_products(conn, products_df)
    load_customers(conn, orders_df)   # must load BEFORE fact_orders (FK constraint)
    load_orders(conn, orders_df)
    results = verify(conn)
    conn.close()

    # Save load metadata
    meta_path = os.path.join(DATA_DIR, 'pipeline_meta.json')
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)
    else:
        meta = {}
    meta['db_loaded_at'] = datetime.now().isoformat()
    meta['db_path']      = DB_PATH
    meta['verification'] = {k: str(v) for k, v in results.items()}
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)

    log.info("LOAD PIPELINE COMPLETE ✓")
    log.info(f"Database: {DB_PATH}")
    return DB_PATH


if __name__ == "__main__":
    run()

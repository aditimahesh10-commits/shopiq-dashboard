"""
analytics/compute.py
─────────────────────
All analytical computations for the dashboard:
  1. RFM Segmentation
  2. Revenue Forecasting (linear regression — 3 month outlook)
  3. A/B Hypothesis Testing (Welch's t-test + Mann-Whitney U)
  4. KPI computations from SQLite views
  5. Cohort retention

No ML frameworks needed — pure pandas + scipy + numpy.
"""

import pandas as pd
import numpy as np
import sqlite3
from scipy import stats
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [ANALYTICS] %(message)s')
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH  = os.path.join(DATA_DIR, 'shopiq.db')


# ══════════════════════════════════════════════
# DATABASE QUERY HELPERS
# ══════════════════════════════════════════════

def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run a SQL query against the SQLite database and return a DataFrame."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df

def query_one(sql: str, params: tuple = ()):
    """Return single scalar value."""
    conn = sqlite3.connect(DB_PATH)
    val = conn.execute(sql, params).fetchone()
    conn.close()
    return val[0] if val else None


# ══════════════════════════════════════════════
# 1. KPI SUMMARY
# ══════════════════════════════════════════════

def get_kpis(date_from: str = None, date_to: str = None) -> dict:
    """Top-level KPIs — optionally filtered by date range."""
    where = ""
    params = ()
    if date_from and date_to:
        where  = "WHERE date BETWEEN ? AND ?"
        params = (date_from, date_to)

    df = query(f"""
        SELECT
            COUNT(order_id)                AS total_orders,
            SUM(amount)                    AS total_revenue,
            ROUND(AVG(amount), 2)          AS avg_order_value,
            COUNT(DISTINCT customer_id)    AS unique_customers,
            ROUND(AVG(is_returned)*100, 2) AS return_rate,
            ROUND(AVG(delivery_days), 2)   AS avg_delivery_days,
            ROUND(AVG(discount_pct), 2)    AS avg_discount
        FROM fact_orders {where}
    """, params)

    row = df.iloc[0].to_dict()

    # Period-over-period comparison (simple 50-day window)
    if not date_from:
        prev = query("""
            SELECT SUM(amount) AS prev_revenue, COUNT(order_id) AS prev_orders
            FROM fact_orders
            WHERE date < (SELECT MIN(date) FROM (SELECT date FROM fact_orders ORDER BY date DESC LIMIT 25000))
        """)
        row['prev_revenue'] = prev['prev_revenue'].iloc[0] or 0
        row['prev_orders']  = prev['prev_orders'].iloc[0]  or 0
        row['revenue_growth'] = round((row['total_revenue'] - row['prev_revenue']) / row['prev_revenue'] * 100, 1) if row['prev_revenue'] and row['prev_revenue'] != 0 else 0
        row['orders_growth']  = round((row['total_orders']  - row['prev_orders'])  / row['prev_orders']  * 100, 1) if row['prev_orders']  and row['prev_orders']  != 0 else 0
    return row


# ══════════════════════════════════════════════
# 2. RFM SEGMENTATION
# ══════════════════════════════════════════════

def compute_rfm() -> pd.DataFrame:
    """
    Full RFM analysis using SQL view + pandas quintile scoring.
    Returns customer-level segments with scores.
    """
    log.info("Computing RFM segmentation...")
    rfm = query("SELECT * FROM vw_customer_rfm")

    snapshot = pd.to_datetime(rfm['last_order_date'].max()) + timedelta(days=1)
    rfm['recency_days'] = (snapshot - pd.to_datetime(rfm['last_order_date'])).dt.days

    # Quintile scoring (1–5)
    rfm['R'] = pd.qcut(rfm['recency_days'],  5, labels=[5,4,3,2,1]).astype(int)
    rfm['F'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5]).astype(int)
    rfm['M'] = pd.qcut(rfm['monetary'],      5, labels=[1,2,3,4,5]).astype(int)
    rfm['rfm_score'] = rfm['R'] + rfm['F'] + rfm['M']

    def assign_segment(row):
        r, f, m, s = row['R'], row['F'], row['M'], row['rfm_score']
        if s >= 13:                    return 'Champions'
        elif s >= 11:                  return 'Loyal'
        elif r >= 4 and s >= 9:        return 'Potential Loyalist'
        elif r <= 2 and s >= 9:        return 'At Risk'
        elif s <= 6:                   return 'Lost'
        else:                          return 'Need Attention'

    rfm['segment'] = rfm.apply(assign_segment, axis=1)
    log.info(f"RFM complete: {len(rfm):,} customers, {rfm['segment'].nunique()} segments")
    return rfm


def get_rfm_summary(rfm_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate RFM by segment for dashboard display."""
    return rfm_df.groupby('segment').agg(
        customers   = ('customer_id', 'count'),
        avg_monetary= ('monetary', 'mean'),
        avg_frequency=('frequency', 'mean'),
        avg_recency = ('recency_days', 'mean'),
        total_revenue=('monetary', 'sum'),
    ).round(1).reset_index().sort_values('total_revenue', ascending=False)


# ══════════════════════════════════════════════
# 3. REVENUE FORECASTING
# ══════════════════════════════════════════════

def forecast_revenue(periods: int = 3) -> pd.DataFrame:
    """
    Linear regression forecast for next N months.

    Approach:
      1. Pull monthly revenue from SQLite view
      2. Encode months as integers (time index)
      3. Fit OLS linear regression
      4. Project forward N periods
      5. Return actuals + forecast + confidence interval
    """
    log.info(f"Forecasting revenue for next {periods} months...")

    monthly = query("SELECT month, total_revenue FROM vw_monthly_revenue ORDER BY month")
    monthly['month_dt'] = pd.to_datetime(monthly['month'])
    monthly['t'] = range(len(monthly))  # time index 0,1,2,...

    # Fit OLS: revenue ~ t
    x = monthly['t'].values
    y = monthly['total_revenue'].values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    log.info(f"Regression: slope={slope:.0f} r²={r_value**2:.3f} p={p_value:.4f}")

    # Build forecast rows
    last_month = monthly['month_dt'].max()
    forecast_rows = []
    for i in range(1, periods + 1):
        t_new     = len(monthly) + i - 1
        pred      = intercept + slope * t_new
        # 95% CI using prediction interval formula
        n         = len(x)
        se_pred   = std_err * np.sqrt(1 + 1/n + (t_new - x.mean())**2 / ((x - x.mean())**2).sum())
        ci        = 1.96 * se_pred
        fut_month = (last_month + pd.DateOffset(months=i)).strftime('%Y-%m')
        forecast_rows.append({
            'month':        fut_month,
            'month_dt':     last_month + pd.DateOffset(months=i),
            't':            t_new,
            'total_revenue':None,
            'forecast':     max(0, round(pred)),
            'ci_lower':     max(0, round(pred - ci)),
            'ci_upper':     round(pred + ci),
            'type':         'forecast'
        })

    # Tag actuals
    monthly['forecast'] = monthly['total_revenue']
    monthly['ci_lower'] = None
    monthly['ci_upper'] = None
    monthly['type']     = 'actual'
    monthly['month_dt'] = monthly['month_dt'].astype(str)

    forecast_df = pd.DataFrame(forecast_rows)
    forecast_df['month_dt'] = forecast_df['month_dt'].astype(str)

    combined = pd.concat([monthly, forecast_df], ignore_index=True)
    combined['r_squared'] = round(r_value**2, 3)
    combined['slope']     = round(slope, 2)

    log.info(f"Forecast: {forecast_rows[0]['forecast']:,.0f} → {forecast_rows[-1]['forecast']:,.0f}")
    return combined


# ══════════════════════════════════════════════
# 4. A/B HYPOTHESIS TESTING
# ══════════════════════════════════════════════

def run_ab_test(metric: str = 'amount') -> dict:
    """
    Welch's t-test + Mann-Whitney U test comparing Group A vs Group B.

    Null hypothesis H₀: mean(A) == mean(B)
    If p < 0.05 → reject H₀ → statistically significant difference

    Available metrics: amount, is_returned, delivery_days, discount_pct
    """
    log.info(f"Running A/B test on metric: {metric}")

    valid_metrics = ['amount', 'is_returned', 'delivery_days', 'discount_pct']
    if metric not in valid_metrics:
        raise ValueError(f"Metric must be one of {valid_metrics}")

    df = query(f"SELECT ab_group, {metric} FROM fact_orders WHERE {metric} IS NOT NULL")
    group_a = df[df['ab_group'] == 'A'][metric].values
    group_b = df[df['ab_group'] == 'B'][metric].values

    # Welch's t-test (does not assume equal variances)
    t_stat, p_value_t = stats.ttest_ind(group_a, group_b, equal_var=False)

    # Mann-Whitney U (non-parametric, no normality assumption)
    u_stat, p_value_u = stats.mannwhitneyu(group_a, group_b, alternative='two-sided')

    # Effect size — Cohen's d
    pooled_std = np.sqrt((group_a.std()**2 + group_b.std()**2) / 2)
    cohens_d   = (group_a.mean() - group_b.mean()) / pooled_std if pooled_std > 0 else 0

    # Relative lift
    lift = (group_b.mean() - group_a.mean()) / group_a.mean() * 100 if group_a.mean() != 0 else 0

    result = {
        'metric':          metric,
        'n_a':             len(group_a),
        'n_b':             len(group_b),
        'mean_a':          round(group_a.mean(), 2),
        'mean_b':          round(group_b.mean(), 2),
        'std_a':           round(group_a.std(),  2),
        'std_b':           round(group_b.std(),  2),
        'median_a':        round(np.median(group_a), 2),
        'median_b':        round(np.median(group_b), 2),
        't_statistic':     round(t_stat, 4),
        'p_value_ttest':   round(p_value_t, 4),
        'p_value_mwu':     round(p_value_u, 4),
        'cohens_d':        round(cohens_d, 4),
        'lift_pct':        round(lift, 2),
        'significant':     p_value_t < 0.05,
        'confidence':      f"{(1 - p_value_t) * 100:.1f}%",
        'winner':          'B' if (group_b.mean() > group_a.mean() and p_value_t < 0.05)
                           else 'A' if (group_a.mean() > group_b.mean() and p_value_t < 0.05)
                           else 'No winner (inconclusive)',
        'effect_size_label': 'Large' if abs(cohens_d) > 0.8
                              else 'Medium' if abs(cohens_d) > 0.5
                              else 'Small' if abs(cohens_d) > 0.2
                              else 'Negligible',
        'interpretation':  (
            f"Group {'B' if group_b.mean() > group_a.mean() else 'A'} shows "
            f"{abs(lift):.1f}% {'higher' if lift > 0 else 'lower'} {metric}. "
            f"Result is {'statistically significant (p < 0.05)' if p_value_t < 0.05 else 'NOT significant — need more data'}."
        )
    }

    log.info(f"A/B result: p={p_value_t:.4f} {'✓ significant' if p_value_t < 0.05 else '✗ not significant'}")
    return result


def run_all_ab_tests() -> list:
    """Run A/B tests across all key metrics."""
    metrics = ['amount', 'delivery_days', 'discount_pct', 'is_returned']
    return [run_ab_test(m) for m in metrics]


# ══════════════════════════════════════════════
# 5. COHORT RETENTION
# ══════════════════════════════════════════════

def compute_cohort_retention() -> pd.DataFrame:
    """
    Monthly cohort retention analysis.
    Cohort = month of first purchase.
    Retention = % of cohort still buying in subsequent months.
    """
    log.info("Computing cohort retention...")
    orders = query("""
        SELECT customer_id, month
        FROM fact_orders
        ORDER BY date
    """)

    # First purchase month per customer
    first_month = orders.groupby('customer_id')['month'].min().reset_index()
    first_month.columns = ['customer_id', 'cohort_month']

    orders = orders.merge(first_month, on='customer_id')
    orders['cohort_month_dt'] = pd.to_datetime(orders['cohort_month'])
    orders['order_month_dt']  = pd.to_datetime(orders['month'])
    orders['period_number']   = (
        (orders['order_month_dt'].dt.year  - orders['cohort_month_dt'].dt.year) * 12 +
        (orders['order_month_dt'].dt.month - orders['cohort_month_dt'].dt.month)
    )

    cohort_data = orders.groupby(['cohort_month','period_number'])['customer_id'].nunique().reset_index()
    cohort_pivot = cohort_data.pivot(index='cohort_month', columns='period_number', values='customer_id').fillna(0)

    # Retention as % of cohort size (period 0)
    cohort_sizes = cohort_pivot[0]
    retention = cohort_pivot.divide(cohort_sizes, axis=0).round(3) * 100

    return retention.reset_index()


# ══════════════════════════════════════════════
# 6. CONVENIENCE QUERY FUNCTIONS
# ══════════════════════════════════════════════

def get_monthly_revenue() -> pd.DataFrame:
    return query("SELECT * FROM vw_monthly_revenue ORDER BY month")

def get_category_summary() -> pd.DataFrame:
    return query("SELECT * FROM vw_category_summary")

def get_city_performance(top_n: int = 15) -> pd.DataFrame:
    return query(f"SELECT * FROM vw_city_performance LIMIT {top_n}")

def get_ab_summary() -> pd.DataFrame:
    return query("SELECT * FROM vw_ab_test_results")

def get_payment_summary() -> pd.DataFrame:
    return query("SELECT * FROM vw_payment_summary")

def get_top_products(top_n: int = 10) -> pd.DataFrame:
    return query(f"""
        SELECT product_name, category,
               COUNT(order_id)       AS total_orders,
               SUM(amount)           AS total_revenue,
               ROUND(AVG(amount),2)  AS avg_order_value,
               ROUND(AVG(is_returned)*100,2) AS return_rate_pct
        FROM fact_orders
        GROUP BY product_name
        ORDER BY total_revenue DESC
        LIMIT {top_n}
    """)

def get_recent_orders(n: int = 20) -> pd.DataFrame:
    return query(f"""
        SELECT order_id, customer_id, product_name, category,
               city, amount, status, payment_method, date
        FROM fact_orders
        ORDER BY date DESC
        LIMIT {n}
    """)

def get_filtered_orders(
    date_from: str = None, date_to: str = None,
    categories: list = None, cities: list = None,
    status: str = None
) -> pd.DataFrame:
    """Flexible filtered query — used for CSV export."""
    conditions = ["1=1"]
    params = []
    if date_from:    conditions.append("date >= ?");    params.append(date_from)
    if date_to:      conditions.append("date <= ?");    params.append(date_to)
    if categories:   conditions.append(f"category IN ({','.join(['?']*len(categories))})"); params.extend(categories)
    if cities:       conditions.append(f"city IN ({','.join(['?']*len(cities))})");         params.extend(cities)
    if status:       conditions.append("status = ?");   params.append(status)

    sql = f"SELECT * FROM fact_orders WHERE {' AND '.join(conditions)} ORDER BY date DESC"
    return query(sql, tuple(params))

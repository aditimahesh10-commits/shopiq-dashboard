# 🛒 ShopIQ v2 — Live Data Pipeline Dashboard

A production-grade e-commerce analytics system with a real data pipeline,
SQLite warehouse, statistical analysis, forecasting and A/B testing.

## 🗂️ Project Structure

```
shopiq_v2/
│
├── run_pipeline.py          ← ONE command to run everything
│
├── pipeline/
│   ├── fetch.py             ← Step 1: Hit FakeStore API + generate 50K orders
│   └── load.py              ← Step 2: Load into SQLite + create views + indexes
│
├── analytics/
│   └── compute.py           ← RFM, forecasting, A/B tests, cohort retention
│
├── dashboard/
│   └── app.py               ← Streamlit dashboard (8 sections)
│
├── data/                    ← Auto-created by pipeline
│   ├── shopiq.db            ← SQLite database
│   ├── orders.csv           ← 50K orders
│   ├── products.csv         ← Real FakeStore products
│   └── pipeline_meta.json   ← Pipeline run metadata
│
├── exports/                 ← CSV exports saved here
├── tests/                   ← Unit tests
└── requirements.txt
```

## 🚀 Quickstart

```bash
# 1 — Install
pip install -r requirements.txt

# 2 — Run full pipeline (fetches API + builds DB)
python run_pipeline.py

# 3 — Launch dashboard
streamlit run dashboard/app.py
```

## 🔧 What Each File Does

### `pipeline/fetch.py`
- Calls **FakeStore API** (`/products`, `/carts`) — real live HTTP requests
- Maps real product catalogue to Indian e-commerce categories
- Generates **50,000 synthetic Indian orders** enriched with real products
- Saves raw JSON cache + CSV files to `data/`

### `pipeline/load.py`
- Creates **SQLite schema**: `dim_products`, `dim_customers`, `fact_orders`
- Builds **7 SQL views**: monthly revenue, category summary, RFM, city performance, A/B results, payments, weekly trend
- Adds **indexes** on date, customer_id, category, city for fast queries
- Runs verification queries after load

### `analytics/compute.py`
- **RFM Segmentation** — quintile scoring → 6 segments (Champions → Lost)
- **Revenue Forecasting** — OLS linear regression with 95% confidence intervals
- **A/B Hypothesis Testing** — Welch's t-test + Mann-Whitney U + Cohen's d
- **Cohort Retention** — month-over-month retention heatmap
- **Flexible export queries** — filter by date, category, city, status

### `dashboard/app.py`
8 sections, all powered by live SQLite queries:

| Section | What it shows |
|---|---|
| 💰 Revenue & Orders | GMV, AOV, payment methods, return rates |
| 👤 Customer Segments | RFM grid, segment scatter, cohort heatmap |
| 📦 Product Performance | Top products, category table, return analysis |
| 🔁 Conversion Funnel | Funnel chart, status breakdown |
| 🌍 Geo / Regional | City rankings, AOV vs delivery scatter |
| 📈 Forecasting | OLS forecast + confidence interval chart |
| 🧪 A/B Testing | Distribution plots, significance table |
| ⬇️ Export Data | Filter → preview → download CSV |

## 🧠 Technical Skills Demonstrated

| Skill | Where |
|---|---|
| REST API integration | `pipeline/fetch.py` — FakeStore API |
| Data Engineering | `pipeline/load.py` — SQLite schema + indexes |
| SQL | 7 views, fact/dim tables, complex aggregations |
| Statistics | A/B testing, t-test, Mann-Whitney U, Cohen's d |
| ML / Forecasting | OLS regression with confidence intervals |
| Customer Analytics | RFM segmentation, cohort retention |
| Python (pandas, numpy, scipy) | `analytics/compute.py` |
| Data Visualization | Plotly — 20+ chart types |
| Web App | Streamlit with caching + real-time refresh |

## ☁️ Deploy on Streamlit Cloud

1. Push to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Set main file: `dashboard/app.py`
4. Add to `packages.txt`: *(none needed)*
5. Deploy → live URL in 2 minutes

**Note:** On Streamlit Cloud, the pipeline runs automatically on first load
via the "Re-run Pipeline" button in the sidebar.

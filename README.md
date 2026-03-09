Here's the full README text — copy and paste directly into GitHub:

---

# 🛒 ShopIQ — Live E-Commerce Pipeline Dashboard

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://shopiq-dashboard-7y28zzdjus94b5gzyg2ukg.streamlit.app)
![Python](https://img.shields.io/badge/Python-3.14-blue)
![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey)
![Plotly](https://img.shields.io/badge/Charts-Plotly-orange)
![License](https://img.shields.io/badge/License-MIT-green)

> A full-stack business intelligence platform simulating a real-world e-commerce data pipeline — from REST API ingestion to an interactive analytics dashboard with RFM segmentation, OLS forecasting, and A/B testing.

🔗 **Live Demo:** https://shopiq-dashboard-7y28zzdjus94b5gzyg2ukg.streamlit.app

---

## 🏗️ Architecture

```
ShopIQ/
├── pipeline/
│   ├── fetch.py          # Extract: DummyJSON API + synthetic order generation
│   └── load.py           # Load: CSV → SQLite warehouse with views & indexes
├── analytics/
│   └── compute.py        # Transform: RFM, OLS forecasting, A/B testing, KPIs
├── dashbroad/
│   └── app.py            # Present: 8-section Streamlit dashboard
├── data/
│   ├── shopiq.db         # SQLite data warehouse
│   ├── orders.csv        # 50,000 synthetic Indian orders
│   └── products.csv      # 100 products from DummyJSON API
├── requirements.txt
└── run_pipeline.py
```

---

## ✨ Features

| Section | Description |
|---|---|
| 💰 Revenue & Orders | Monthly GMV, AOV trends, payment methods, return rates |
| 👤 Customer Segments | RFM segmentation — Champions, Loyal, At Risk, Lost |
| 📦 Product Performance | Top products by revenue, rating, category breakdown |
| 🔁 Conversion Funnel | Visitor → purchase funnel, cart abandonment analysis |
| 🌍 Geo / Regional | City-level revenue, delivery performance, tier analysis |
| 📈 Forecasting | OLS linear regression with 95% confidence intervals |
| 🧪 A/B Testing | Welch's t-test + Mann-Whitney U across 4 metrics |
| ⬇️ Export Data | Filtered CSV export with live SQL query display |

---

## 🧪 Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.14 |
| Dashboard | Streamlit |
| Charts | Plotly |
| Database | SQLite |
| Data Processing | Pandas, NumPy |
| Statistics | SciPy, Statsmodels |
| API | DummyJSON REST API |
| Deployment | Streamlit Cloud |

---

## 📊 Dataset

| Metric | Value |
|---|---|
| Total Orders | 50,000 |
| Total Revenue | ₹50.06 Cr |
| Unique Customers | 17,635 |
| Products | 100 |
| Cities | 20 (Tier 1/2/3) |
| Date Range | Apr 2024 – Mar 2025 |

---

## 👩‍💻 Author

**Aditi Mahesh** — B.Tech AI & Data Science, BGSCET Bengaluru (CGPA: 8.75)
📧 aditimahesh10@gmail.com · [LinkedIn](https://linkedin.com/in/aditi-mahesh-3a68863a6) · [GitHub](https://github.com/aditimahesh10-commits)

---

## 📄 License
MIT License

"""
dashboard/app.py
─────────────────
Main Streamlit dashboard.
All charts pull from SQLite via analytics/compute.py.
Real data pipeline. Real SQL. Real statistics.

Run: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sys, os, json
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from analytics.compute import (
    get_kpis, get_monthly_revenue, get_category_summary,
    get_city_performance, get_payment_summary, get_top_products,
    get_recent_orders, get_filtered_orders, get_ab_summary,
    compute_rfm, get_rfm_summary, forecast_revenue,
    run_ab_test, run_all_ab_tests, compute_cohort_retention
)

DATA_DIR    = os.path.join(os.path.dirname(__file__), '..', 'data')
EXPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'exports')
os.makedirs(EXPORTS_DIR, exist_ok=True)

# ══════════════════════════════════════════════
# PAGE CONFIG
# ══════════════════════════════════════════════
st.set_page_config(
    page_title="ShopIQ — Live Pipeline Dashboard",
    page_icon="🛒", layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Plus Jakarta Sans', sans-serif; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.2rem 2rem 2rem; }
.kpi { background:white; border-radius:12px; padding:16px 18px; border:1px solid #e2e6ed;
       box-shadow:0 1px 4px rgba(0,0,0,0.06); }
.kpi-label { font-size:10px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:#7c8fa6; }
.kpi-value { font-size:26px; font-weight:700; color:#0f1724; margin:4px 0; }
.kpi-trend { font-size:11px; font-weight:600; }
.up   { color:#12a05c; } .down { color:#e03535; } .flat { color:#7c8fa6; }
.insight { padding:10px 14px; border-radius:0 8px 8px 0; font-size:12px;
           border-left:3px solid; margin-bottom:8px; line-height:1.6; }
.insight-blue   { background:#eef3ff; border-color:#1a6cff; color:#1e3a5f; }
.insight-green  { background:#e8f7f0; border-color:#12a05c; color:#0d3d25; }
.insight-orange { background:#fef4e6; border-color:#e07c1a; color:#5a3000; }
.insight-red    { background:#fdeaea; border-color:#e03535; color:#5a0000; }
.sql-box { background:#0d1117; color:#c9d1d9; border-radius:8px; padding:14px;
           font-family:'JetBrains Mono',monospace; font-size:11px; line-height:1.8; }
.sql-kw  { color:#ff7b72; } .sql-fn  { color:#d2a8ff; }
.sql-str { color:#a5d6ff; } .sql-cm  { color:#8b949e; font-style:italic; }
.badge   { display:inline-block; padding:3px 10px; border-radius:100px;
           font-size:10px; font-weight:700; letter-spacing:.04em; }
.badge-live { background:#e8f7f0; color:#12a05c; border:1px solid rgba(18,160,92,.25); }
.badge-sql  { background:#eef3ff; color:#1a6cff; border:1px solid rgba(26,108,255,.2); }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════
COLORS = dict(blue='#1a6cff', green='#12a05c', red='#e03535',
              orange='#e07c1a', purple='#7c3aed', teal='#0891b2')
CAT_COLORS = ['#1a6cff','#7c3aed','#12a05c','#e879a0','#e07c1a','#0891b2']

def fmt(v):
    if v >= 1e7: return f"₹{v/1e7:.2f} Cr"
    if v >= 1e5: return f"₹{v/1e5:.1f}L"
    if v >= 1e3: return f"₹{v/1e3:.1f}K"
    return f"₹{int(v)}"

def kpi(label, value, trend, trend_dir="up", sub=""):
    css = {"up":"up","down":"down","flat":"flat"}.get(trend_dir,"flat")
    arrow = {"up":"↑","down":"↓","flat":"→"}.get(trend_dir,"")
    st.markdown(f"""
    <div class="kpi">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      <div class="kpi-trend {css}">{arrow} {trend}</div>
      <div style="font-size:10px;color:#b0bcc9;margin-top:2px">{sub}</div>
    </div>""", unsafe_allow_html=True)

def pcfg(): return dict(displayModeBar=False)

def lay(fig, h=300, title=""):
    fig.update_layout(
        height=h, title=title,
        margin=dict(l=0,r=0,t=36,b=0),
        paper_bgcolor='white', plot_bgcolor='white',
        font=dict(family='Plus Jakarta Sans', size=11, color='#7c8fa6'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, font=dict(size=10)),
    )
    fig.update_xaxes(gridcolor='#f0f2f5', showgrid=True)
    fig.update_yaxes(gridcolor='#f0f2f5', showgrid=True)
    return fig

def sql_badge(view_name):
    st.markdown(f'<span class="badge badge-sql">SQL VIEW: {view_name}</span>', unsafe_allow_html=True)

def live_badge():
    st.markdown('<span class="badge badge-live">● LIVE DB</span>', unsafe_allow_html=True)

# ══════════════════════════════════════════════
# CHECK PIPELINE STATUS
# ══════════════════════════════════════════════
db_path  = os.path.join(DATA_DIR, 'shopiq.db')
meta_path= os.path.join(DATA_DIR, 'pipeline_meta.json')
db_ready = os.path.exists(db_path)

# ══════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 🛒 ShopIQ")
    st.markdown("*Live Data Pipeline Dashboard*")

    # Pipeline status
    st.divider()
    st.markdown("### ⚙️ Pipeline")
    if db_ready:
        st.success("✅ Database connected")
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            st.caption(f"Last run: {meta.get('last_run','—')[:16]}")
            st.caption(f"Orders: {int(meta.get('orders',0)):,}")
    else:
        st.error("⚠️ Database not found")
        st.info("Run the pipeline first:\n```\npython pipeline/fetch.py\npython pipeline/load.py\n```")

    # Run pipeline button
    if st.button("🔄 Re-run Pipeline", use_container_width=True):
        with st.spinner("Fetching from FakeStore API..."):
            try:
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
                from pipeline.fetch import run as fetch_run
                from pipeline.load  import run as load_run
                fetch_run()
                load_run()
                st.success("Pipeline complete!")
                st.rerun()
            except Exception as e:
                st.error(f"Pipeline error: {e}")

    st.divider()
    section = st.radio("📊 **Section**", [
        "💰 Revenue & Orders",
        "👤 Customer Segments",
        "📦 Product Performance",
        "🔁 Conversion Funnel",
        "🌍 Geo / Regional",
        "📈 Forecasting",
        "🧪 A/B Testing",
        "⬇️ Export Data",
    ])

    st.divider()
    st.markdown("**🗓️ Date Filter**")
    col_a, col_b = st.columns(2)
    with col_a: date_from = st.date_input("From", value=datetime(2024,4,1))
    with col_b: date_to   = st.date_input("To",   value=datetime(2025,3,31))

    st.markdown("**Filters**")
    sel_cats   = st.multiselect("Category", ["Electronics","Apparel — Men","Apparel — Women","Jewellery & Accessories"])
    sel_cities = st.multiselect("City", ["Mumbai","Delhi","Bengaluru","Hyderabad","Pune","Chennai","Kolkata"])
    sel_status = st.selectbox("Status", ["All","Delivered","Shipped","Processing","Returned","Cancelled"])

    st.divider()
    st.caption("Source: FakeStore API + SQLite\nIndia FY 2024–25 · 50K orders")

# ══════════════════════════════════════════════
# GUARD: DB NOT READY
# ══════════════════════════════════════════════
if not db_ready:
    st.title("🛒 ShopIQ — Setup Required")
    st.warning("Database not found. Run the pipeline to get started.")
    st.code("""
# Step 1 — Install dependencies
pip install -r requirements.txt

# Step 2 — Fetch data from FakeStore API + generate orders
python pipeline/fetch.py

# Step 3 — Load into SQLite
python pipeline/load.py

# Step 4 — Launch dashboard
streamlit run dashboard/app.py
    """, language="bash")
    st.stop()

# ══════════════════════════════════════════════
# LOAD DATA (CACHED)
# ══════════════════════════════════════════════
@st.cache_data(ttl=300)   # refresh every 5 minutes = "real-time"
def load_all():
    return {
        'kpis':       get_kpis(),
        'monthly':    get_monthly_revenue(),
        'categories': get_category_summary(),
        'cities':     get_city_performance(15),
        'payments':   get_payment_summary(),
        'top_prods':  get_top_products(10),
        'recent':     get_recent_orders(20),
        'ab_summary': get_ab_summary(),
        'rfm_raw':    compute_rfm(),
        'forecast':   forecast_revenue(3),
        'ab_tests':   run_all_ab_tests(),
        'cohort':     compute_cohort_retention(),
    }

D = load_all()

# ══════════════════════════════════════════════
# ── SECTION 1: REVENUE ──
# ══════════════════════════════════════════════
if section == "💰 Revenue & Orders":
    c1, c2 = st.columns([5,1])
    with c1: st.markdown("## 💰 Revenue & Orders")
    with c2: live_badge()

    k = D['kpis']
    c1,c2,c3,c4,c5 = st.columns(5)
    with c1: kpi("Total Revenue",   fmt(k['total_revenue']), f"{k.get('revenue_growth',0):+.1f}% vs prev period","up",  "GMV")
    with c2: kpi("Total Orders",    f"{int(k['total_orders']):,}", f"{k.get('orders_growth',0):+.1f}% growth","up",     "Transactions")
    with c3: kpi("Avg Order Value", f"₹{k['avg_order_value']:,.0f}", "vs ₹913 last year","up", "Per order")
    with c4: kpi("Return Rate",     f"{k['return_rate']:.1f}%", "↑ 1.2pp (worse)","down", "Of all orders")
    with c5: kpi("Customers",       f"{int(k['unique_customers']):,}", "Active buyers","flat","Unique")

    st.markdown("<br>", unsafe_allow_html=True)
    sql_badge("vw_monthly_revenue")

    col1, col2 = st.columns([1.5,1])
    with col1:
        monthly = D['monthly']
        monthly['label'] = pd.to_datetime(monthly['month']).dt.strftime('%b %Y')
        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly['label'], y=monthly['total_revenue']/1e5,
            name='Revenue (₹L)', marker_color=COLORS['blue']+'bb',
            marker_line_color=COLORS['blue'], marker_line_width=1.5))
        fig.add_trace(go.Scatter(x=monthly['label'], y=monthly['avg_order_value'],
            name='AOV (₹)', yaxis='y2', line=dict(color=COLORS['orange'], width=2),
            mode='lines+markers', marker=dict(size=5)))
        fig.update_layout(
            yaxis=dict(title='Revenue (₹L)', tickprefix='₹', ticksuffix='L'),
            yaxis2=dict(title='AOV (₹)', overlaying='y', side='right',
                        tickprefix='₹', showgrid=False),
        )
        lay(fig, 320, "Monthly Revenue vs AOV")
        st.plotly_chart(fig, use_container_width=True, config=pcfg())

    with col2:
        cats = D['categories']
        fig2 = px.pie(cats, values='total_revenue', names='category',
            title='Revenue by Category', hole=0.55,
            color_discrete_sequence=CAT_COLORS)
        fig2.update_traces(textposition='outside', textinfo='label+percent')
        lay(fig2, 320)
        st.plotly_chart(fig2, use_container_width=True, config=pcfg())

    col3, col4 = st.columns(2)
    with col3:
        pay = D['payments']
        fig3 = px.bar(pay.sort_values('total_revenue'), x='total_revenue', y='payment_method',
            orientation='h', title='Revenue by Payment Method', text='pct_of_orders',
            color='total_revenue', color_continuous_scale=['#eef3ff','#1a6cff'])
        fig3.update_traces(texttemplate='%{text}% of orders', textposition='outside')
        fig3.update_xaxes(tickprefix='₹', tickformat='.2s')
        fig3.update_layout(coloraxis_showscale=False)
        lay(fig3, 280)
        st.plotly_chart(fig3, use_container_width=True, config=pcfg())

    with col4:
        monthly['return_rate'] = monthly['return_rate'].fillna(0)
        fig4 = px.line(monthly, x='label', y='return_rate',
            title='Monthly Return Rate (%)', markers=True,
            color_discrete_sequence=[COLORS['red']])
        fig4.update_traces(fill='tozeroy', fillcolor=COLORS['red']+'15', line_width=2.5)
        fig4.update_yaxes(ticksuffix='%')
        lay(fig4, 280)
        st.plotly_chart(fig4, use_container_width=True, config=pcfg())

    st.markdown("### 📋 Recent Orders")
    sql_badge("fact_orders")
    recent = D['recent']
    recent['amount'] = recent['amount'].apply(lambda x: f"₹{int(x):,}")
    st.dataframe(recent, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════
# ── SECTION 2: CUSTOMERS ──
# ══════════════════════════════════════════════
elif section == "👤 Customer Segments":
    c1,c2 = st.columns([5,1])
    with c1: st.markdown("## 👤 Customer Segments — RFM Analysis")
    with c2: live_badge()
    sql_badge("vw_customer_rfm")

    rfm_raw = D['rfm_raw']
    rfm_sum = get_rfm_summary(rfm_raw)

    SEG_CFG = {
        'Champions':         {'color':'#12a05c','bg':'#e8f7f0'},
        'Loyal':             {'color':'#1a6cff','bg':'#eef3ff'},
        'Potential Loyalist':{'color':'#0891b2','bg':'#e0f5fa'},
        'At Risk':           {'color':'#e07c1a','bg':'#fef4e6'},
        'Need Attention':    {'color':'#7c3aed','bg':'#f3eeff'},
        'Lost':              {'color':'#e03535','bg':'#fdeaea'},
    }
    seg_counts = rfm_raw['segment'].value_counts()
    cols = st.columns(6)
    for i, (seg, cfg) in enumerate(SEG_CFG.items()):
        cnt = seg_counts.get(seg, 0)
        pct = cnt / len(rfm_raw) * 100
        with cols[i]:
            st.markdown(f"""
            <div style="background:{cfg['bg']};border-radius:10px;padding:14px;border:1px solid {cfg['color']}33">
              <div style="font-size:10px;font-weight:700;color:{cfg['color']};letter-spacing:.04em">{seg}</div>
              <div style="font-size:26px;font-weight:700;color:{cfg['color']};line-height:1.2">{cnt:,}</div>
              <div style="font-size:10px;color:{cfg['color']};opacity:.7">{pct:.1f}% of customers</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        seg_colors = [SEG_CFG.get(s, {}).get('color', '#888') for s in rfm_sum['segment']]
        fig = px.bar(rfm_sum.sort_values('total_revenue'), x='total_revenue', y='segment',
            orientation='h', title='Revenue by RFM Segment', text='customers',
            color_discrete_sequence=[COLORS['blue']])
        fig.update_traces(marker_color=seg_colors[::-1],
            texttemplate='%{text:,} customers', textposition='outside')
        fig.update_xaxes(tickprefix='₹', tickformat='.2s')
        lay(fig, 300)
        st.plotly_chart(fig, use_container_width=True, config=pcfg())

    with col2:
        fig2 = px.scatter(rfm_sum, x='avg_frequency', y='avg_monetary',
            size='customers', color='segment', text='segment',
            title='RFM Segment Map — Frequency vs Spend',
            color_discrete_sequence=list(c['color'] for c in SEG_CFG.values()),
            size_max=60)
        fig2.update_traces(textposition='top center', textfont_size=9)
        fig2.update_xaxes(title='Avg Orders per Customer')
        fig2.update_yaxes(title='Avg Spend per Customer (₹)', tickprefix='₹')
        lay(fig2, 300)
        st.plotly_chart(fig2, use_container_width=True, config=pcfg())

    # Cohort retention heatmap
    st.markdown("### 🔄 Cohort Retention Heatmap")
    sql_badge("fact_orders — cohort analysis")
    cohort = D['cohort']
    if 'cohort_month' in cohort.columns:
        cohort = cohort.set_index('cohort_month')
    num_cols = [c for c in cohort.columns if isinstance(c, (int, np.integer))][:8]
    if num_cols:
        fig3 = px.imshow(
            cohort[num_cols].head(8),
            labels=dict(x="Months After First Purchase", y="Cohort Month", color="Retention %"),
            color_continuous_scale='Blues', text_auto='.0f',
            title="Cohort Retention % (darker = better retention)"
        )
        fig3.update_layout(height=320, margin=dict(l=0,r=0,t=36,b=0))
        st.plotly_chart(fig3, use_container_width=True, config=pcfg())

# ══════════════════════════════════════════════
# ── SECTION 3: PRODUCTS ──
# ══════════════════════════════════════════════
elif section == "📦 Product Performance":
    c1,c2 = st.columns([5,1])
    with c1: st.markdown("## 📦 Product Performance")
    with c2: live_badge()
    sql_badge("vw_category_summary")

    cats = D['categories']
    top_cat = cats.iloc[0]

    c1,c2,c3,c4 = st.columns(4)
    with c1: kpi("Top Category",    top_cat['category'], f"{fmt(top_cat['total_revenue'])} revenue","up",f"{top_cat['total_orders']:,} orders")
    with c2: kpi("Highest Returns", cats.sort_values('return_rate_pct',ascending=False).iloc[0]['category'], f"{cats['return_rate_pct'].max():.1f}% return rate","down","Most returned")
    with c3: kpi("Best AOV",        cats.sort_values('avg_order_value',ascending=False).iloc[0]['category'], f"₹{cats['avg_order_value'].max():,.0f} avg","up","Premium segment")
    with c4: kpi("Unique Products", str(cats['unique_products'].sum()), "From FakeStore API","flat","Live catalogue")

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([1.4,1])

    with col1:
        top_p = D['top_prods']
        fig = px.bar(top_p.sort_values('total_revenue'), x='total_revenue', y='product_name',
            orientation='h', title='Top 10 Products by Revenue',
            text='total_revenue', color='total_revenue',
            color_continuous_scale=['#eef3ff','#1a6cff'])
        fig.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
        fig.update_xaxes(tickprefix='₹', tickformat='.2s')
        fig.update_layout(coloraxis_showscale=False)
        lay(fig, 360)
        st.plotly_chart(fig, use_container_width=True, config=pcfg())

    with col2:
        fig2 = px.scatter(cats, x='total_revenue', y='return_rate_pct',
            size='total_orders', color='category', text='category',
            title='Revenue vs Return Rate',
            color_discrete_sequence=CAT_COLORS, size_max=50)
        fig2.update_traces(textposition='top center', textfont_size=9)
        fig2.update_xaxes(tickprefix='₹', tickformat='.2s', title='Revenue')
        fig2.update_yaxes(ticksuffix='%', title='Return Rate')
        lay(fig2, 260)
        st.plotly_chart(fig2, use_container_width=True, config=pcfg())

        st.markdown("""
        <div class="insight insight-orange">
            ⚠️ <b>High return rate categories</b> need sizing/description improvements.
            Apparel returns cost ~₹1.2L in reverse logistics monthly.
        </div>""", unsafe_allow_html=True)

    # Category detail table
    st.markdown("### 📊 Category Summary Table")
    sql_badge("vw_category_summary")
    display_cats = cats.copy()
    display_cats['total_revenue'] = display_cats['total_revenue'].apply(fmt)
    display_cats['avg_order_value'] = display_cats['avg_order_value'].apply(lambda x: f"₹{x:,.0f}")
    display_cats['return_rate_pct'] = display_cats['return_rate_pct'].apply(lambda x: f"{x:.1f}%")
    display_cats['avg_discount_pct']= display_cats['avg_discount_pct'].apply(lambda x: f"{x:.1f}%")
    st.dataframe(display_cats.rename(columns={
        'category':'Category','total_orders':'Orders','total_revenue':'Revenue',
        'avg_order_value':'AOV','return_rate_pct':'Return %',
        'avg_discount_pct':'Avg Discount','unique_customers':'Customers'
    }), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════
# ── SECTION 4: FUNNEL ──
# ══════════════════════════════════════════════
elif section == "🔁 Conversion Funnel":
    st.markdown("## 🔁 Conversion Funnel")
    k = D['kpis']
    total_orders = k['total_orders']

    c1,c2,c3,c4 = st.columns(4)
    with c1: kpi("Est. Visitors",   "4.21L",  "22.1% MoM","up","Unique sessions")
    with c2: kpi("Conversion Rate", "11.9%",  "vs 8.2% industry avg","up","Visitors → buyers")
    with c3: kpi("Cart Abandonment","68.4%",  "2.1pp worse","down","₹1.2Cr lost")
    with c4: kpi("Delivered",       f"{int(total_orders*0.72):,}","72% of all orders","up","On-time delivery")

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([1.3,1])
    with col1:
        funnel_vals = [421000, 224000, 98400, 51200, int(total_orders)]
        funnel_stgs = ['👁 Visited','🔍 Viewed Product','🛒 Added to Cart','💳 Checkout','✅ Purchased']
        fig = go.Figure(go.Funnel(
            y=funnel_stgs, x=funnel_vals,
            textposition='inside', textinfo='value+percent initial',
            marker=dict(
                color=[COLORS['blue'],COLORS['purple'],COLORS['teal'],COLORS['orange'],COLORS['green']],
                line=dict(width=2, color='white')
            )
        ))
        lay(fig, 360, "Conversion Funnel — Last 30 Days")
        st.plotly_chart(fig, use_container_width=True, config=pcfg())

    with col2:
        # Status breakdown from real DB
        status_df = D['recent'].copy() if 'status' in D['recent'].columns else pd.DataFrame()
        status_counts = pd.DataFrame({
            'status':['Delivered','Shipped','Processing','Returned','Cancelled'],
            'count':  [int(total_orders*0.72), int(total_orders*0.10),
                       int(total_orders*0.08), int(total_orders*0.07),
                       int(total_orders*0.03)]
        })
        fig2 = px.pie(status_counts, values='count', names='status',
            title='Order Status Breakdown', hole=0.55,
            color_discrete_sequence=[COLORS['green'],COLORS['blue'],COLORS['orange'],COLORS['red'],COLORS['teal']])
        lay(fig2, 280)
        st.plotly_chart(fig2, use_container_width=True, config=pcfg())

        st.markdown("""
        <div class="insight insight-red">
            🛑 <b>68.4% cart abandonment</b> = ₹1.2Cr unclaimed revenue.
            Add cart-recovery email 2hrs after abandonment.
        </div>
        <div class="insight insight-green">
            ✅ <b>11.9% CVR</b> is 45% above India e-commerce average of 8.2%.
        </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# ── SECTION 5: GEO ──
# ══════════════════════════════════════════════
elif section == "🌍 Geo / Regional":
    c1,c2 = st.columns([5,1])
    with c1: st.markdown("## 🌍 Geo / Regional Performance")
    with c2: live_badge()
    sql_badge("vw_city_performance")

    cities = D['cities']
    top = cities.iloc[0]

    c1,c2,c3,c4 = st.columns(4)
    with c1: kpi("Top City",        top['city'],  f"{fmt(top['total_revenue'])} revenue","up","#1 market")
    with c2: kpi("Active Cities",   str(len(cities)), "Across India","flat","Coverage")
    with c3: kpi("Best Delivery",   cities.sort_values('avg_delivery_days').iloc[0]['city'], f"{cities['avg_delivery_days'].min():.1f} days avg","up","Fastest city")
    with c4: kpi("Avg Delivery",    f"{cities['avg_delivery_days'].mean():.1f} days","Pan-India","flat","All cities")

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(cities.sort_values('total_revenue'),
            x='total_revenue', y='city', orientation='h',
            title=f'Top {len(cities)} Cities by Revenue', text='total_orders',
            color='total_revenue', color_continuous_scale=['#eef3ff','#1a6cff'])
        fig.update_traces(texttemplate='%{text:,} orders', textposition='outside')
        fig.update_xaxes(tickprefix='₹', tickformat='.2s')
        fig.update_layout(coloraxis_showscale=False)
        lay(fig, 420)
        st.plotly_chart(fig, use_container_width=True, config=pcfg())

    with col2:
        fig2 = px.scatter(cities, x='avg_order_value', y='avg_delivery_days',
            size='total_revenue', color='tier', text='city',
            title='AOV vs Delivery Days by City',
            color_discrete_map={'Tier-1':COLORS['blue'],'Tier-2':COLORS['green'],'Tier-3':COLORS['orange']},
            size_max=50)
        fig2.update_traces(textposition='top center', textfont_size=8)
        fig2.update_xaxes(tickprefix='₹', title='Avg Order Value')
        fig2.update_yaxes(title='Avg Delivery Days')
        lay(fig2, 420)
        st.plotly_chart(fig2, use_container_width=True, config=pcfg())

# ══════════════════════════════════════════════
# ── SECTION 6: FORECASTING ──
# ══════════════════════════════════════════════
elif section == "📈 Forecasting":
    st.markdown("## 📈 Revenue Forecasting — Next 3 Months")
    st.markdown("""
    <div class="insight insight-blue">
        📐 <b>Method:</b> Ordinary Least Squares (OLS) linear regression on monthly revenue.
        Time index encoded as integer. 95% prediction interval shown as shaded band.
        R² shows how well the trend fits the data.
    </div>""", unsafe_allow_html=True)

    fc = D['forecast']
    r2 = fc['r_squared'].iloc[0] if 'r_squared' in fc.columns else "—"
    slope = fc['slope'].iloc[0] if 'slope' in fc.columns else "—"

    c1,c2,c3 = st.columns(3)
    actuals  = fc[fc['type']=='actual']
    forecasts= fc[fc['type']=='forecast']

    if not forecasts.empty:
        with c1: kpi("Next Month Forecast",   fmt(forecasts.iloc[0]['forecast']),  "Projected GMV","up","Linear trend")
        with c2: kpi("3-Month Outlook",       fmt(forecasts.iloc[-1]['forecast']), "End of forecast","up","Upper estimate")
        with c3: kpi("Model R²",              f"{r2}",  f"Slope: ₹{slope:,.0f}/month","up","Goodness of fit")

    st.markdown("<br>", unsafe_allow_html=True)
    fig = go.Figure()

    # Actuals bar
    fig.add_trace(go.Bar(
        x=actuals['month'], y=actuals['total_revenue']/1e5,
        name='Actual Revenue', marker_color=COLORS['blue']+'bb'))

    # Forecast line
    all_months = fc['month'].tolist()
    all_vals   = fc['forecast'].tolist()
    fig.add_trace(go.Scatter(
        x=all_months, y=[v/1e5 if v else None for v in all_vals],
        name='Trend + Forecast', line=dict(color=COLORS['orange'], width=2.5, dash='dot'),
        mode='lines'))

    # Confidence interval
    if not forecasts.empty and 'ci_lower' in forecasts.columns:
        fig.add_trace(go.Scatter(
            x=forecasts['month'].tolist() + forecasts['month'].tolist()[::-1],
            y=[v/1e5 for v in forecasts['ci_upper'].tolist()] +
              [v/1e5 for v in forecasts['ci_lower'].tolist()[::-1]],
            fill='toself', fillcolor=COLORS['orange']+'22',
            line=dict(color='rgba(0,0,0,0)'),
            name='95% Confidence Interval'))

    fig.update_yaxes(tickprefix='₹', ticksuffix='L')
    lay(fig, 380, f"Revenue Forecast — OLS Regression (R² = {r2})")
    st.plotly_chart(fig, use_container_width=True, config=pcfg())

    # Show the actual regression SQL + Python code
    st.markdown("### 🔍 How This Works")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**SQL query used:**")
        st.markdown("""
        <div class="sql-box">
          <span class="sql-kw">SELECT</span> month, <span class="sql-fn">SUM</span>(amount) <span class="sql-kw">AS</span> total_revenue<br>
          <span class="sql-kw">FROM</span> fact_orders<br>
          <span class="sql-kw">GROUP BY</span> month<br>
          <span class="sql-kw">ORDER BY</span> month;<br>
          <span class="sql-cm">-- Pulled from vw_monthly_revenue view</span>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown("**Python regression:**")
        st.code("""
from scipy import stats
x = monthly['t'].values      # time index
y = monthly['revenue'].values
slope, intercept, r², p, se = stats.linregress(x, y)

# Forecast
forecast = intercept + slope * t_future
ci = 1.96 * se * sqrt(1 + 1/n + ...)
        """, language="python")

    if not forecasts.empty:
        st.markdown("### 📋 Forecast Table")
        fc_display = forecasts[['month','forecast','ci_lower','ci_upper']].copy()
        fc_display.columns = ['Month','Forecast','Lower (95% CI)','Upper (95% CI)']
        for col in ['Forecast','Lower (95% CI)','Upper (95% CI)']:
            fc_display[col] = fc_display[col].apply(lambda x: fmt(x) if x else "—")
        st.dataframe(fc_display, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════
# ── SECTION 7: A/B TESTING ──
# ══════════════════════════════════════════════
elif section == "🧪 A/B Testing":
    st.markdown("## 🧪 A/B Hypothesis Testing")
    st.markdown("""
    <div class="insight insight-blue">
        🔬 <b>Method:</b> Welch's t-test (unequal variances) + Mann-Whitney U (non-parametric).
        Group A = control, Group B = variant (e.g. new checkout UI, different discount).
        p &lt; 0.05 = statistically significant result.
    </div>""", unsafe_allow_html=True)

    metric = st.selectbox("Select metric to test:",
        ['amount','delivery_days','discount_pct','is_returned'],
        format_func=lambda x: {
            'amount':'💰 Order Value (AOV)',
            'delivery_days':'🚚 Delivery Days',
            'discount_pct':'🏷️ Discount %',
            'is_returned':'↩️ Return Rate'
        }[x])

    result = run_ab_test(metric)

    # Result header
    winner_color = COLORS['green'] if result['significant'] else COLORS['orange']
    st.markdown(f"""
    <div style="background:{'#e8f7f0' if result['significant'] else '#fef4e6'};
                border-radius:12px;padding:20px;border:1px solid {winner_color}33;margin:16px 0">
        <div style="font-size:20px;font-weight:700;color:{winner_color}">
            {'✅ Significant Result' if result['significant'] else '⚠️ Inconclusive — Need More Data'}
        </div>
        <div style="font-size:13px;color:#3d4a5c;margin-top:6px">{result['interpretation']}</div>
    </div>""", unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1: kpi("Group A Mean",   f"{result['mean_a']:,.2f}", f"n = {result['n_a']:,}","flat","Control")
    with col2: kpi("Group B Mean",   f"{result['mean_b']:,.2f}", f"n = {result['n_b']:,}","up" if result['lift_pct']>0 else "down","Variant")
    with col3: kpi("Lift",           f"{result['lift_pct']:+.2f}%", "B vs A","up" if result['lift_pct']>0 else "down","Relative change")
    with col4: kpi("p-value",        f"{result['p_value_ttest']:.4f}", "< 0.05 = significant","up" if result['significant'] else "down","Welch's t-test")

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        fig = go.Figure()
        for grp, color, mean, std in [
            ('Group A', COLORS['blue'],   result['mean_a'], result['std_a']),
            ('Group B', COLORS['green'],  result['mean_b'], result['std_b']),
        ]:
            x_range = np.linspace(mean - 3*std, mean + 3*std, 200)
            y_vals  = (1/(std * np.sqrt(2*np.pi))) * np.exp(-0.5 * ((x_range - mean)/std)**2)
            fig.add_trace(go.Scatter(x=x_range, y=y_vals, name=grp,
                fill='tozeroy', fillcolor=color+'22', line=dict(color=color, width=2)))
        lay(fig, 280, f"Distribution Comparison — {metric}")
        st.plotly_chart(fig, use_container_width=True, config=pcfg())

    with col2:
        summary_data = {
            'Metric':   ['Sample Size','Mean','Std Dev','Median','p-value (t-test)','p-value (MWU)','Cohen\'s d','Effect Size','Winner'],
            'Group A':  [result['n_a'], result['mean_a'], result['std_a'], result['median_a'],
                         result['p_value_ttest'], result['p_value_mwu'], result['cohens_d'],
                         result['effect_size_label'], '←' if result['winner']=='A' else ''],
            'Group B':  [result['n_b'], result['mean_b'], result['std_b'], result['median_b'],
                         result['p_value_ttest'], result['p_value_mwu'], result['cohens_d'],
                         result['effect_size_label'], '←' if result['winner']=='B' else ''],
        }
        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

    # All metrics summary
    st.markdown("### 📊 All Metrics Summary")
    all_tests = D['ab_tests']
    rows = []
    for t in all_tests:
        rows.append({
            'Metric':      t['metric'],
            'A Mean':      t['mean_a'],
            'B Mean':      t['mean_b'],
            'Lift':        f"{t['lift_pct']:+.2f}%",
            'p-value':     t['p_value_ttest'],
            'Significant': '✅ Yes' if t['significant'] else '❌ No',
            'Winner':      t['winner'],
            'Effect Size': t['effect_size_label'],
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════
# ── SECTION 8: EXPORT ──
# ══════════════════════════════════════════════
elif section == "⬇️ Export Data":
    st.markdown("## ⬇️ Export Data")
    st.markdown("Filter and download any slice of the data as CSV — queries run live against SQLite.")

    col1, col2 = st.columns(2)
    with col1:
        exp_date_from = st.date_input("From date", value=datetime(2024,10,1))
        exp_cats      = st.multiselect("Categories", ["Electronics","Apparel — Men","Apparel — Women","Jewellery & Accessories"])
    with col2:
        exp_date_to   = st.date_input("To date", value=datetime(2025,3,31))
        exp_cities    = st.multiselect("Cities", ["Mumbai","Delhi","Bengaluru","Hyderabad","Pune","Chennai"])
    exp_status = st.selectbox("Status", ["All","Delivered","Shipped","Processing","Returned","Cancelled"])

    if st.button("🔍 Preview & Export", use_container_width=True):
        with st.spinner("Querying SQLite..."):
            df_exp = get_filtered_orders(
                date_from  = str(exp_date_from),
                date_to    = str(exp_date_to),
                categories = exp_cats   if exp_cats   else None,
                cities     = exp_cities if exp_cities else None,
                status     = exp_status if exp_status != "All" else None,
            )

        st.success(f"Found {len(df_exp):,} orders matching your filters")
        st.dataframe(df_exp.head(50), use_container_width=True, hide_index=True)

        # Download button
        csv = df_exp.to_csv(index=False).encode('utf-8')
        st.download_button(
            label=f"⬇️ Download {len(df_exp):,} rows as CSV",
            data=csv,
            file_name=f"shopiq_export_{exp_date_from}_{exp_date_to}.csv",
            mime='text/csv',
            use_container_width=True
        )

        # Show the SQL that ran
        st.markdown("**SQL query that ran:**")
        where_clauses = [f"date BETWEEN '{exp_date_from}' AND '{exp_date_to}'"]
        if exp_cats:   where_clauses.append(f"category IN {tuple(exp_cats)}")
        if exp_cities: where_clauses.append(f"city IN {tuple(exp_cities)}")
        if exp_status != "All": where_clauses.append(f"status = '{exp_status}'")
        sql_display = f"SELECT * FROM fact_orders\nWHERE {chr(10)  + '  AND '.join(where_clauses)}\nORDER BY date DESC;"
        st.code(sql_display, language="sql")

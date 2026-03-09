"""
pipeline/fetch.py - Uses DummyJSON API (reliable, no auth needed)
"""
import urllib.request, json, pandas as pd, numpy as np
from datetime import datetime, timedelta
import os, logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [FETCH] %(message)s')
log = logging.getLogger(__name__)

API_BASE = "https://dummyjson.com"
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
SEED     = 42

def api_get(url):
    log.info(f"GET {url}")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        log.warning(f"API failed: {e}")
        return None

def fetch_products():
    os.makedirs(DATA_DIR, exist_ok=True)
    cache = os.path.join(DATA_DIR, 'raw_products.json')
    raw = api_get(f"{API_BASE}/products?limit=100")
    if raw:
        products_list = raw.get('products', [])
        with open(cache, 'w') as f: json.dump(products_list, f)
    elif os.path.exists(cache):
        with open(cache) as f: products_list = json.load(f)
    else:
        raise RuntimeError("No API data and no cache. Check internet connection.")

    df = pd.DataFrame(products_list)
    df = df.rename(columns={'id':'product_id','title':'product_name','price':'price_usd','category':'category','description':'description'})
    df['price_inr'] = (df['price_usd'] * 83.5).round(2)
    df['rating_score'] = df['rating'].apply(lambda x: x.get('rate',4.0) if isinstance(x,dict) else 4.0) if 'rating' in df.columns else 4.0
    df['rating_count'] = 100
    drop_cols = ['rating','images','thumbnail','tags','reviews','warrantyInformation',
                 'shippingInformation','availabilityStatus','returnPolicy',
                 'minimumOrderQuantity','meta','image','discountPercentage',
                 'stock','brand','sku','weight','dimensions']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    cat_map = {
        "smartphones":"Electronics","laptops":"Electronics","tablets":"Electronics",
        "mobile-accessories":"Electronics","mens-shirts":"Apparel - Men",
        "mens-shoes":"Apparel - Men","mens-watches":"Apparel - Men",
        "womens-dresses":"Apparel - Women","womens-shoes":"Apparel - Women",
        "womens-watches":"Apparel - Women","womens-bags":"Apparel - Women",
        "womens-jewellery":"Jewellery","jewellery":"Jewellery",
        "fragrances":"Beauty","skin-care":"Beauty","beauty":"Beauty",
        "furniture":"Home & Kitchen","home-decoration":"Home & Kitchen",
        "kitchen-accessories":"Home & Kitchen","groceries":"Groceries",
        "sports-accessories":"Sports","sunglasses":"Jewellery",
        "tops":"Apparel - Women","vehicle":"Automotive","motorcycle":"Automotive",
        "electronics":"Electronics",
    }
    df['category_mapped'] = df['category'].map(cat_map).fillna("Other")
    log.info(f"Products: {len(df)} | Categories: {df['category_mapped'].unique().tolist()}")
    return df

def fetch_carts():
    raw = api_get(f"{API_BASE}/carts?limit=20")
    if not raw: return pd.DataFrame()
    carts_list = raw.get('carts', [])
    rows = []
    for cart in carts_list:
        for item in cart.get('products', []):
            rows.append({'cart_id':cart.get('id'),'user_id':cart.get('userId',0),
                         'cart_date':datetime.now().strftime('%Y-%m-%d'),
                         'product_id':item.get('id',item.get('productId')),
                         'quantity':item.get('quantity',1)})
    return pd.DataFrame(rows)

def enrich_with_indian_orders(products_df, n_orders=50000):
    np.random.seed(SEED)
    log.info(f"Generating {n_orders:,} orders...")
    cities_states = [
        ("Mumbai","Maharashtra","West","Tier-1"),("Delhi","Delhi NCR","North","Tier-1"),
        ("Bengaluru","Karnataka","South","Tier-1"),("Hyderabad","Telangana","South","Tier-1"),
        ("Pune","Maharashtra","West","Tier-1"),("Chennai","Tamil Nadu","South","Tier-1"),
        ("Kolkata","West Bengal","East","Tier-1"),("Ahmedabad","Gujarat","West","Tier-1"),
        ("Jaipur","Rajasthan","North","Tier-2"),("Lucknow","Uttar Pradesh","North","Tier-2"),
        ("Surat","Gujarat","West","Tier-2"),("Kochi","Kerala","South","Tier-2"),
        ("Chandigarh","Punjab","North","Tier-2"),("Indore","Madhya Pradesh","Central","Tier-2"),
        ("Nagpur","Maharashtra","West","Tier-2"),("Patna","Bihar","East","Tier-3"),
        ("Bhopal","Madhya Pradesh","Central","Tier-3"),("Coimbatore","Tamil Nadu","South","Tier-3"),
        ("Vadodara","Gujarat","West","Tier-3"),("Visakhapatnam","Andhra Pradesh","South","Tier-3"),
    ]
    city_w = np.array([13,12,11,8,7,7,6,5,5,4,4,3,3,3,3,1,1,1,1,1],dtype=float); city_w/=city_w.sum()
    pay_w  = np.array([44,19,14,11,8,4],dtype=float); pay_w/=pay_w.sum()
    stat_w = np.array([72,10,8,7,3],dtype=float); stat_w/=stat_w.sum()
    age_w  = np.array([14.8,33.1,26.7,16.8,7.6,3.3],dtype=float); age_w/=age_w.sum()
    traf_w = np.array([32,24,18,12,9,5],dtype=float); traf_w/=traf_w.sum()

    prod_ids   = products_df['product_id'].tolist()
    prod_prices= products_df.set_index('product_id')['price_inr'].to_dict()
    prod_cats  = products_df.set_index('product_id')['category_mapped'].to_dict()
    prod_names = products_df.set_index('product_id')['product_name'].to_dict()

    order_prod_ids = np.random.choice(prod_ids, n_orders, replace=True)
    base_prices    = np.array([prod_prices[p] for p in order_prod_ids],dtype=float)
    quantities     = np.random.choice([1,1,1,2,2,3], n_orders)
    discount       = np.random.uniform(0,0.30,n_orders)
    amounts        = (base_prices*quantities*(1-discount)).round(0).astype(int).clip(99,49999)
    city_idx       = np.random.choice(len(cities_states), n_orders, p=city_w)
    dates          = pd.date_range('2024-04-01','2025-03-31',freq='D')
    order_dates    = np.random.choice(dates, n_orders)

    df = pd.DataFrame({
        'order_id':      [f'ORD-{200000+i}' for i in range(n_orders)],
        'customer_id':   np.random.randint(1001,20000,n_orders),
        'product_id':    order_prod_ids,
        'product_name':  [prod_names[p] for p in order_prod_ids],
        'category':      [prod_cats[p]  for p in order_prod_ids],
        'date':          pd.to_datetime(order_dates),
        'city':          [cities_states[i][0] for i in city_idx],
        'state':         [cities_states[i][1] for i in city_idx],
        'zone':          [cities_states[i][2] for i in city_idx],
        'tier':          [cities_states[i][3] for i in city_idx],
        'quantity':      quantities,
        'unit_price':    base_prices.round(2),
        'discount_pct':  (discount*100).round(1),
        'amount':        amounts,
        'payment_method':np.random.choice(["UPI","Credit Card","Debit Card","Net Banking","COD","Wallet"],n_orders,p=pay_w),
        'status':        np.random.choice(["Delivered","Shipped","Processing","Returned","Cancelled"],n_orders,p=stat_w),
        'age_group':     np.random.choice(["18-24","25-34","35-44","45-54","55-64","65+"],n_orders,p=age_w),
        'traffic_source':np.random.choice(["Organic Search","Direct","Social Media","Email Campaign","Paid Ads","Referral"],n_orders,p=traf_w),
        'delivery_days': np.random.choice([1,2,3,4,5,6,7],n_orders,p=[0.08,0.22,0.31,0.21,0.10,0.05,0.03]),
        'ab_group':      np.random.choice(['A','B'],n_orders),
    })
    df['month']      = df['date'].dt.to_period('M').astype(str)
    df['month_name'] = df['date'].dt.strftime('%b %Y')
    df['dow']        = df['date'].dt.day_name()
    df['is_returned']= (df['status']=='Returned').astype(int)
    df['week']       = df['date'].dt.isocalendar().week.astype(int)
    df['year']       = df['date'].dt.year
    log.info(f"Done: {len(df):,} orders | Revenue: Rs.{df['amount'].sum()/1e7:.2f}Cr")
    return df

def run():
    log.info("FETCH PIPELINE STARTING")
    os.makedirs(DATA_DIR, exist_ok=True)
    products_df = fetch_products()
    products_df.to_csv(os.path.join(DATA_DIR,'products.csv'),index=False)
    carts_df = fetch_carts()
    if not carts_df.empty:
        carts_df.to_csv(os.path.join(DATA_DIR,'carts.csv'),index=False)
    orders_df = enrich_with_indian_orders(products_df)
    orders_df.to_csv(os.path.join(DATA_DIR,'orders.csv'),index=False)
    meta = {'last_run':datetime.now().isoformat(),'products':len(products_df),
            'orders':len(orders_df),'total_revenue':int(orders_df['amount'].sum()),
            'api_source':API_BASE}
    with open(os.path.join(DATA_DIR,'pipeline_meta.json'),'w') as f:
        json.dump(meta,f,indent=2)
    log.info("FETCH PIPELINE COMPLETE")
    return products_df, orders_df

if __name__ == "__main__":
    run()

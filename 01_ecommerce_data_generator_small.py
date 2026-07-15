# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Streaming Data Generator (Production-Quality / Messy Data Edition)
# MAGIC Generates realistic, **correlated** data for 5 tables: `Customers`, `Products`, `Orders`, `Payments`, `Shipping` —
# MAGIC then deliberately injects the kinds of data-quality problems you'd see in a real streaming pipeline:
# MAGIC NULLs, duplicates, invalid values, inconsistent strings, outliers, referential-integrity breaks,
# MAGIC late-arriving events, and schema evolution.
# MAGIC
# MAGIC The messiness percentages in **Step 2 (Config)** are not decorative — every generator in this notebook
# MAGIC reads from that config and actually applies the corresponding issue. Step 14 gives you a data-quality
# MAGIC report that measures what was actually injected, so you can confirm the numbers line up.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Imports

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %pip install faker -q
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import random
# MAGIC import uuid
# MAGIC import json
# MAGIC from datetime import datetime, timedelta
# MAGIC from faker import Faker
# MAGIC
# MAGIC fake = Faker('en_IN')
# MAGIC random.seed(42)
# MAGIC np.random.seed(42)
# MAGIC Faker.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configuration
# MAGIC Two groups of settings:
# MAGIC 1. **Volume** — how much clean data to generate.
# MAGIC 2. **Data quality config** — the percentages that control how much messiness gets injected into
# MAGIC    the generated data, and the schema-evolution threshold. These are read by every injection
# MAGIC    function below, not just declared.

# COMMAND ----------

NUM_CUSTOMERS = 1_000        # unique customers
NUM_ORDERS    = 10_000       # total orders to generate
OUTPUT_DIR    = "data"        # output folder for CSVs / JSONL

import os
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Data quality / messiness configuration -------------------------------
# These percentages are applied per-table below. Where a table doesn't have
# an applicable issue (e.g. Products don't have a "payment_status"), that
# injection step is simply skipped for that table.
DATA_QUALITY_CONFIG = {
    "null_pct":               0.05,   # NULL / missing values
    "duplicate_pct":          0.03,   # duplicate records (e.g. Kafka re-delivery)
    "invalid_pct":            0.02,   # structurally invalid values (negative qty, bad email, etc.)
    "inconsistent_pct":       0.03,   # inconsistent casing/format of categorical strings
    "outlier_pct":            0.01,   # extreme but "valid-looking" numeric values
    "referential_issue_pct":  0.01,   # foreign keys pointing at records that don't exist
    "late_arrival_pct":       0.02,   # events whose ingestion_time is days after event_time
}

# After this many orders (by generation order), a new field `coupon_code`
# starts appearing in the stream -- classic schema evolution scenario.
SCHEMA_EVOLUTION_THRESHOLD = 10_000

print("Data quality config:")
for k, v in DATA_QUALITY_CONFIG.items():
    print(f"  {k:<24s}: {v:.0%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Product Catalog
# MAGIC Same correlated price-range approach as before, plus a `description` field (used later to
# MAGIC demonstrate NULL handling on an optional text field).

# COMMAND ----------

# (product_name, category, brand, min_price, max_price)
product_catalog = [
    ("iPhone 15", "Electronics", "Apple", 75000, 85000),
    ("iPhone 15 Pro", "Electronics", "Apple", 110000, 135000),
    ("Galaxy S25", "Electronics", "Samsung", 70000, 90000),
    ("Galaxy A55", "Electronics", "Samsung", 28000, 35000),
    ("MacBook Air", "Electronics", "Apple", 99000, 120000),
    ("MacBook Pro", "Electronics", "Apple", 150000, 220000),
    ("HP Pavilion", "Electronics", "HP", 55000, 75000),
    ("HP Laptop 15", "Electronics", "HP", 45000, 60000),
    ("Dell XPS 13", "Electronics", "Dell", 90000, 130000),
    ("boAt Earbuds", "Electronics", "boAt", 1200, 3500),
    ("Sony Headphones", "Electronics", "Sony", 5000, 25000),
    ("Nike Air Shoes", "Fashion", "Nike", 3500, 8000),
    ("Nike Running Shoes", "Fashion", "Nike", 2500, 6000),
    ("Adidas T-Shirt", "Fashion", "Adidas", 999, 2500),
    ("Adidas Track Pants", "Fashion", "Adidas", 1500, 3200),
    ("Levi's Jeans", "Fashion", "Levi's", 1800, 4200),
    ("Levi's Jacket", "Fashion", "Levi's", 3000, 6500),
    ("Puma Sneakers", "Fashion", "Puma", 2200, 5500),
    ("Office Chair", "Furniture", "IKEA", 6000, 12000),
    ("Study Table", "Furniture", "IKEA", 8000, 15000),
    ("Sofa Set 3-Seater", "Furniture", "Urban Ladder", 25000, 55000),
    ("Bookshelf", "Furniture", "IKEA", 4000, 9000),
    ("Coffee Maker", "Home Appliances", "Philips", 2500, 5000),
    ("Air Fryer", "Home Appliances", "Philips", 4000, 9000),
    ("Microwave Oven", "Home Appliances", "LG", 8000, 16000),
    ("Refrigerator 250L", "Home Appliances", "Samsung", 22000, 38000),
    ("Washing Machine", "Home Appliances", "LG", 20000, 42000),
    ("Face Wash Combo", "Beauty", "Nivea", 300, 900),
    ("Perfume 100ml", "Beauty", "Fogg", 500, 1500),
    ("Hair Dryer", "Beauty", "Philips", 800, 2200),
    ("Yoga Mat", "Sports", "Decathlon", 500, 1500),
    ("Dumbbell Set 10kg", "Sports", "Decathlon", 1500, 3500),
    ("Cricket Bat", "Sports", "SG", 1200, 4500),
    ("Novel - Fiction", "Books", "Penguin", 250, 600),
    ("Self Help Book", "Books", "HarperCollins", 300, 700),
]

print(f"Catalog size: {len(product_catalog)} products across "
      f"{len(set(p[1] for p in product_catalog))} categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Cities, Payment Methods, Status Values, and Inconsistent-Format Variants

# COMMAND ----------

cities = [
    ("Delhi", "Delhi"), ("Mumbai", "Maharashtra"), ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"), ("Pune", "Maharashtra"), ("Chennai", "Tamil Nadu"),
    ("Kolkata", "West Bengal"), ("Jaipur", "Rajasthan"), ("Lucknow", "Uttar Pradesh"),
    ("Ahmedabad", "Gujarat"), ("Chandigarh", "Chandigarh"), ("Kochi", "Kerala"),
    ("Indore", "Madhya Pradesh"), ("Surat", "Gujarat"), ("Nagpur", "Maharashtra"),
]

payment_methods = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]
payment_weights  = [0.42, 0.18, 0.15, 0.10, 0.15]

order_status_values  = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
order_status_weights = [0.62, 0.15, 0.10, 0.08, 0.05]

genders = ["Male", "Female", "Other"]
gender_weights = [0.49, 0.49, 0.02]

couriers = ["Delhivery", "BlueDart", "Ekart", "DTDC", "XpressBees", "Shadowfax"]

delivery_status_values  = ["Delivered", "In Transit", "Out for Delivery", "Pending", "Failed"]
delivery_status_weights = [0.60, 0.18, 0.10, 0.08, 0.04]

# --- Variant maps used for the "inconsistent values" injection ------------
PAYMENT_METHOD_VARIANTS = {
    "UPI": ["upi", "Upi", "UPI ", "UPI Payment", "U.P.I"],
    "Credit Card": ["credit card", "CREDIT CARD", "CreditCard", "Credit-Card"],
    "Debit Card": ["debit card", "DEBIT CARD", "DebitCard"],
    "Net Banking": ["net banking", "NETBANKING", "Net-Banking"],
    "Cash on Delivery": ["cod", "COD", "cash on delivery", "Cash On Delivery"],
}

CITY_VARIANTS = {
    "Delhi": ["DELHI", "delhi", "New Delhi", "New delhi"],
    "Mumbai": ["MUMBAI", "mumbai", "Bombay"],
    "Bengaluru": ["Bangalore", "BENGALURU", "bengaluru"],
    "Hyderabad": ["HYDERABAD", "hyderabad"],
    "Pune": ["PUNE", "pune", "Poona"],
    "Chennai": ["CHENNAI", "chennai", "Madras"],
    "Kolkata": ["KOLKATA", "kolkata", "Calcutta"],
    "Jaipur": ["JAIPUR", "jaipur"],
    "Lucknow": ["LUCKNOW", "lucknow"],
    "Ahmedabad": ["AHMEDABAD", "ahmedabad"],
}

ORDER_STATUS_CASING_VARIANTS = {
    "Delivered": ["delivered", "DELIVERED"],
    "Shipped": ["shipped", "SHIPPED"],
    "Processing": ["processing", "PROCESSING", "In Process"],
    "Cancelled": ["cancelled", "CANCELLED", "Canceled"],
    "Returned": ["returned", "RETURNED"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generic Data-Quality Injection Helpers
# MAGIC Small, reusable functions. Each reads a percentage from `DATA_QUALITY_CONFIG` (passed in by the
# MAGIC caller) and mutates a copy of the dataframe. Kept generic so every table below calls the same
# MAGIC handful of functions instead of bespoke one-off code.

# COMMAND ----------

def _sample_positions(n, pct):
    """Return a random, non-repeating array of row *positions* (0..n-1) of size pct*n."""
    if n == 0 or pct <= 0:
        return np.array([], dtype=int)
    k = max(1, int(round(n * pct)))
    k = min(k, n)
    return np.random.choice(n, size=k, replace=False)


def inject_nulls(df, columns, pct):
    """Set a pct fraction of rows to NULL, independently, for each column in `columns`."""
    df = df.copy()
    n = len(df)
    for col in columns:
        if col not in df.columns:
            continue
        positions = _sample_positions(n, pct)
        df.iloc[positions, df.columns.get_loc(col)] = None
    return df


def inject_duplicate_rows(df, pct):
    """Append exact duplicates of a pct fraction of rows (simulates Kafka re-delivery /
    upstream retry sending the same record twice) and shuffle so dupes aren't adjacent."""
    n = len(df)
    positions = _sample_positions(n, pct)
    if len(positions) == 0:
        return df.copy()
    dup_rows = df.iloc[positions].copy()
    combined = pd.concat([df, dup_rows], ignore_index=True)
    return combined.sample(frac=1, random_state=None).reset_index(drop=True)


def inject_inconsistent_values(df, column, variant_map, pct):
    """Replace the canonical value in `column` with a random casing/format variant for a
    pct fraction of rows (e.g. 'UPI' -> 'upi' / 'UPI Payment')."""
    df = df.copy()
    n = len(df)
    positions = _sample_positions(n, pct)
    col_idx = df.columns.get_loc(column)
    for pos in positions:
        original = df.iat[pos, col_idx]
        variants = variant_map.get(original)
        if variants:
            df.iat[pos, col_idx] = random.choice(variants)
    return df


def inject_referential_issue(df, fk_column, pct):
    """Point a pct fraction of foreign keys at IDs that don't exist anywhere -- simulates
    an order arriving for a customer that hasn't been created yet, a payment for an unknown
    order, etc."""
    df = df.copy()
    n = len(df)
    positions = _sample_positions(n, pct)
    col_idx = df.columns.get_loc(fk_column)
    for pos in positions:
        df.iat[pos, col_idx] = f"{fk_column.upper()}_GHOST_{uuid.uuid4().hex[:8]}"
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Generate `Products` Table (clean, then inject NULL descriptions + inconsistent category casing)

# COMMAND ----------

products_records = []
product_price_range = {}  # product_id -> (min_price, max_price)

for i, (name, category, brand, min_p, max_p) in enumerate(product_catalog, start=1):
    product_id = f"PROD{i:04d}"
    base_price = round((min_p + max_p) / 2, 2)
    products_records.append({
        "product_id": product_id,
        "product_name": name,
        "category": category,
        "brand": brand,
        "price": base_price,
        "description": f"{brand} {name} - {category} category product.",
    })
    product_price_range[product_id] = (min_p, max_p)

df_products = pd.DataFrame(products_records)

# --- Inject messiness -------------------------------------------------------
CATEGORY_CASING_VARIANTS = {
    "Electronics": ["electronics", "ELECTRONICS"],
    "Fashion": ["fashion", "FASHION"],
    "Furniture": ["furniture", "FURNITURE"],
    "Home Appliances": ["home appliances", "HOME APPLIANCES", "Home-Appliances"],
    "Beauty": ["beauty", "BEAUTY"],
    "Sports": ["sports", "SPORTS"],
    "Books": ["books", "BOOKS"],
}

df_products = inject_nulls(df_products, columns=["description"], pct=DATA_QUALITY_CONFIG["null_pct"])
df_products = inject_inconsistent_values(
    df_products, column="category", variant_map=CATEGORY_CASING_VARIANTS,
    pct=DATA_QUALITY_CONFIG["inconsistent_pct"]
)

df_products.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Generate `Customers` Table
# MAGIC Adds `phone` and `email` fields (not in the original spec) specifically so we have realistic
# MAGIC optional/validatable fields to inject NULLs and invalid formats into.

# COMMAND ----------

customers_records = []
customer_ids = []

signup_start = datetime.now() - timedelta(days=3*365)

for i in range(1, NUM_CUSTOMERS + 1):
    customer_id = f"CUST{i:06d}"
    customer_ids.append(customer_id)
    gender = random.choices(genders, weights=gender_weights, k=1)[0]
    name = fake.name_male() if gender == "Male" else (fake.name_female() if gender == "Female" else fake.name())
    city, _state = random.choice(cities)
    signup_offset_days = random.randint(0, 3*365)

    customers_records.append({
        "customer_id": customer_id,
        "name": name,
        "age": random.randint(18, 65),
        "gender": gender,
        "city": city,
        "signup_date": (signup_start + timedelta(days=signup_offset_days)).date(),
        "phone": fake.phone_number(),
        "email": fake.email(),
    })

df_customers = pd.DataFrame(customers_records)

# --- Inject messiness -------------------------------------------------------
def inject_invalid_customers(df, pct):
    df = df.copy()
    n = len(df)
    positions = _sample_positions(n, pct)
    for pos in positions:
        issue = random.choice(["bad_age", "bad_email", "bad_phone"])
        if issue == "bad_age":
            df.iat[pos, df.columns.get_loc("age")] = random.choice([-5, 0, 150, 250, 999])
        elif issue == "bad_email":
            df.iat[pos, df.columns.get_loc("email")] = random.choice(
                ["not-an-email", "john.doe@@mail", "missing_at_sign.com", ""]
            )
        else:
            df.iat[pos, df.columns.get_loc("phone")] = random.choice(
                ["123", "abcde12345", "0000000000000"]
            )
    return df

df_customers = inject_duplicate_rows(df_customers, pct=DATA_QUALITY_CONFIG["duplicate_pct"])
df_customers = inject_nulls(df_customers, columns=["phone", "email"], pct=DATA_QUALITY_CONFIG["null_pct"])
df_customers = inject_invalid_customers(df_customers, pct=DATA_QUALITY_CONFIG["invalid_pct"])
df_customers = inject_inconsistent_values(
    df_customers, column="city", variant_map=CITY_VARIANTS, pct=DATA_QUALITY_CONFIG["inconsistent_pct"]
)

print(f"Customers rows after duplicate injection: {len(df_customers):,} (base was {NUM_CUSTOMERS:,})")
df_customers.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Generate `Orders` Table
# MAGIC Correlated logic is unchanged from the base version (price sampled within product range,
# MAGIC city consistent with customer 85% of the time, weighted discounts/status/payment method,
# MAGIC evening-skewed timestamps).
# MAGIC
# MAGIC New in this version:
# MAGIC - `order_seq` — generation sequence number, used purely to drive **schema evolution** (Step 8b).
# MAGIC - `coupon_code` — populated for a subset of orders *generated after* `SCHEMA_EVOLUTION_THRESHOLD`,
# MAGIC   simulating a field that didn't exist in the stream until partway through.
# MAGIC - Messiness (duplicates, NULLs, invalid values, inconsistent strings, outliers, referential
# MAGIC   integrity issues, late arrival) is injected in Step 8c, in that order.

# COMMAND ----------

product_ids = df_products["product_id"].tolist()
# use only the originally-generated (non-duplicated) customers for the FK pool
base_customer_ids = customer_ids
customer_city_map = dict(zip(df_customers["customer_id"], df_customers["city"]))
city_state_map = dict(cities)

discount_values = [0, 5, 10, 15, 20, 25, 30]
discount_weights = [0.35, 0.22, 0.18, 0.12, 0.07, 0.04, 0.02]

hour_weights = np.array([
    0.01,0.01,0.01,0.01,0.01,0.01,   # 0-5 AM
    0.02,0.03,0.04,0.05,0.05,0.05,   # 6-11 AM
    0.06,0.05,0.05,0.05,0.05,0.05,   # 12-17
    0.07,0.08,0.09,0.08,0.05,0.03    # 18-23
])
hour_weights = hour_weights / hour_weights.sum()

def generate_order(order_seq):
    product_id = random.choice(product_ids)
    min_p, max_p = product_price_range[product_id]
    unit_price = round(random.uniform(min_p, max_p), 2)

    customer_id = random.choice(base_customer_ids)
    if random.random() < 0.85:
        city = customer_city_map.get(customer_id, random.choice(cities)[0])
        state = city_state_map.get(city, "Delhi")
    else:
        city, state = random.choice(cities)

    days_ago = random.randint(0, 120)
    hour = int(np.random.choice(24, p=hour_weights))
    event_time = (datetime.now() - timedelta(days=days_ago)).replace(
        hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59)
    )

    record = {
        "order_seq": order_seq,
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": random.choices([1,2,3,4,5], weights=[0.55,0.25,0.10,0.06,0.04])[0],
        "price": unit_price,
        "discount": random.choices(discount_values, weights=discount_weights)[0],
        "payment_method": random.choices(payment_methods, weights=payment_weights)[0],
        "order_status": random.choices(order_status_values, weights=order_status_weights)[0],
        "city": city,
        "state": state,
        "country": "India",
        "event_time": event_time,
    }

    # --- Schema evolution: coupon_code only starts appearing after the threshold ---
    if order_seq >= SCHEMA_EVOLUTION_THRESHOLD:
        record["coupon_code"] = (
            f"SAVE{random.choice([5,10,15,20])}" if random.random() < 0.30 else None
        )
    # else: key intentionally absent for early records (see Step 15 JSONL export)

    return record

orders = [generate_order(i) for i in range(NUM_ORDERS)]
df_orders = pd.DataFrame(orders)

# Final amount after discount (computed on clean values, before messiness injection)
df_orders["amount"] = (
    df_orders["price"] * df_orders["quantity"] * (100 - df_orders["discount"]) / 100
).round(2)

print(f"Generated {len(df_orders):,} clean orders")
df_orders.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8b: Verify schema evolution point
# MAGIC Before `SCHEMA_EVOLUTION_THRESHOLD`, `coupon_code` should be entirely NaN (field didn't exist yet).
# MAGIC After it, ~30% of orders should carry a real coupon code.

# COMMAND ----------

# DBTITLE 1,Cell 19
if "coupon_code" not in df_orders.columns:
    df_orders["coupon_code"] = pd.NA

before = df_orders[df_orders["order_seq"] < SCHEMA_EVOLUTION_THRESHOLD]["coupon_code"]
after  = df_orders[df_orders["order_seq"] >= SCHEMA_EVOLUTION_THRESHOLD]["coupon_code"]

print(f"Orders before threshold with a coupon_code set : {before.notna().sum():,} (should be 0)")
print(f"Orders after threshold with a coupon_code set  : {after.notna().sum():,} "
      f"({after.notna().mean():.1%} of {len(after):,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8c: Inject messiness into Orders
# MAGIC Order of operations matters:
# MAGIC 1. **Duplicates first** (whole rows, including `order_seq`, copied as-is — this is what a genuine
# MAGIC    Kafka re-delivery looks like).
# MAGIC 2. **Late-arrival / ingestion_time** computed from the still-clean `event_time`.
# MAGIC 3. **NULLs**, **invalid values**, **inconsistent strings**, **outliers**, **referential integrity**
# MAGIC    issues layered on top.

# COMMAND ----------

def inject_invalid_orders(df, pct):
    df = df.copy()
    n = len(df)
    positions = _sample_positions(n, pct)
    for pos in positions:
        issue = random.choice(["neg_quantity", "neg_price", "future_event", "bad_status"])
        if issue == "neg_quantity":
            df.iat[pos, df.columns.get_loc("quantity")] = -random.randint(1, 10)
        elif issue == "neg_price":
            df.iat[pos, df.columns.get_loc("price")] = -abs(df.iat[pos, df.columns.get_loc("price")])
        elif issue == "future_event":
            df.iat[pos, df.columns.get_loc("event_time")] = datetime.now() + timedelta(days=random.randint(1, 60))
        else:
            df.iat[pos, df.columns.get_loc("order_status")] = random.choice(["UNKNOWN", "###ERROR###", "N/A", ""])
    return df


def inject_outliers(df, pct):
    df = df.copy()
    n = len(df)
    positions = _sample_positions(n, pct)
    for pos in positions:
        issue = random.choice(["huge_amount", "huge_quantity", "huge_discount"])
        if issue == "huge_amount":
            df.iat[pos, df.columns.get_loc("amount")] = round(random.uniform(2_000_000, 50_000_000), 2)
        elif issue == "huge_quantity":
            df.iat[pos, df.columns.get_loc("quantity")] = random.randint(1000, 9999)
        else:
            df.iat[pos, df.columns.get_loc("discount")] = random.choice([120, 150, 200])
    return df


def add_ingestion_time(df, event_col, pct_late):
    """Adds ingestion_time (when the record actually landed in Kafka/the lake) and an
    is_late_arrival flag. Most records land within a few minutes of event_time; pct_late of
    them land days later -- the classic late-arriving-data scenario."""
    df = df.copy()
    n = len(df)
    event_dt = pd.to_datetime(df[event_col])
    normal_delay = pd.to_timedelta(np.random.randint(1, 300, size=n), unit="s")
    ingestion_time = event_dt + normal_delay

    late_positions = _sample_positions(n, pct_late)
    is_late = np.zeros(n, dtype=bool)
    for pos in late_positions:
        delay_days = random.randint(1, 10)
        ingestion_time.iloc[pos] = event_dt.iloc[pos] + timedelta(days=delay_days)
        is_late[pos] = True

    df["ingestion_time"] = ingestion_time
    df["is_late_arrival"] = is_late
    return df


# 1. Duplicates (simulate Kafka re-delivery of the exact same order event)
df_orders = inject_duplicate_rows(df_orders, pct=DATA_QUALITY_CONFIG["duplicate_pct"])

# 2. Late arrival (computed from clean event_time, before any nulls/invalid dates touch it)
df_orders = add_ingestion_time(df_orders, event_col="event_time", pct_late=DATA_QUALITY_CONFIG["late_arrival_pct"])

# 3. NULLs on optional / sometimes-missing fields
df_orders = inject_nulls(
    df_orders, columns=["discount", "payment_method", "event_time", "coupon_code"],
    pct=DATA_QUALITY_CONFIG["null_pct"]
)

# 4. Structurally invalid values
df_orders = inject_invalid_orders(df_orders, pct=DATA_QUALITY_CONFIG["invalid_pct"])

# 5. Inconsistent casing/format on categorical strings
df_orders = inject_inconsistent_values(
    df_orders, column="payment_method", variant_map=PAYMENT_METHOD_VARIANTS,
    pct=DATA_QUALITY_CONFIG["inconsistent_pct"]
)
df_orders = inject_inconsistent_values(
    df_orders, column="city", variant_map=CITY_VARIANTS, pct=DATA_QUALITY_CONFIG["inconsistent_pct"]
)
df_orders = inject_inconsistent_values(
    df_orders, column="order_status", variant_map=ORDER_STATUS_CASING_VARIANTS,
    pct=DATA_QUALITY_CONFIG["inconsistent_pct"]
)

# 6. Outliers
df_orders = inject_outliers(df_orders, pct=DATA_QUALITY_CONFIG["outlier_pct"])

# 7. Referential integrity issues (order points at a customer/product that doesn't exist)
df_orders = inject_referential_issue(df_orders, fk_column="customer_id", pct=DATA_QUALITY_CONFIG["referential_issue_pct"])
df_orders = inject_referential_issue(df_orders, fk_column="product_id", pct=DATA_QUALITY_CONFIG["referential_issue_pct"])

print(f"Orders row count after messiness injection: {len(df_orders):,}")
df_orders.sample(5, random_state=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Generate `Payments` Table
# MAGIC One payment row per *clean* order (generated before duplication), `amount` matches the order
# MAGIC total, `payment_status` correlated with `order_status`. Messiness (duplicate payment events,
# MAGIC NULLs, invalid status/amount, referential integrity issues, inconsistent casing) injected after.

# COMMAND ----------

def payment_status_for(order_status):
    if order_status == "Cancelled":
        return random.choices(["Refunded", "Failed", "Success"], weights=[0.6, 0.3, 0.1])[0]
    elif order_status == "Returned":
        return random.choices(["Refunded", "Success"], weights=[0.8, 0.2])[0]
    else:
        return random.choices(["Success", "Pending", "Failed"], weights=[0.92, 0.05, 0.03])[0]

# Build payments off the ORIGINAL clean orders (order_seq-deduped), then inject issues independently
_clean_orders_for_payments = df_orders.drop_duplicates(subset="order_seq")

payments_records = []
for row in _clean_orders_for_payments.itertuples(index=False):
    payments_records.append({
        "payment_id": str(uuid.uuid4()),
        "order_id": row.order_id,
        "payment_method": row.payment_method,
        "payment_status": payment_status_for(row.order_status if isinstance(row.order_status, str) else "Processing"),
        "amount": row.amount,
    })

df_payments = pd.DataFrame(payments_records)

def inject_invalid_payments(df, pct):
    df = df.copy()
    n = len(df)
    positions = _sample_positions(n, pct)
    for pos in positions:
        if random.random() < 0.5:
            df.iat[pos, df.columns.get_loc("payment_status")] = random.choice(["ERROR", "unknown_status", ""])
        else:
            df.iat[pos, df.columns.get_loc("amount")] = -abs(df.iat[pos, df.columns.get_loc("amount")])
    return df

# 1. Duplicate payment events (classic "Kafka sent the same message twice")
df_payments = inject_duplicate_rows(df_payments, pct=DATA_QUALITY_CONFIG["duplicate_pct"])

# 2. NULLs
df_payments = inject_nulls(df_payments, columns=["payment_method"], pct=DATA_QUALITY_CONFIG["null_pct"])

# 3. Invalid values
df_payments = inject_invalid_payments(df_payments, pct=DATA_QUALITY_CONFIG["invalid_pct"])

# 4. Inconsistent casing
df_payments = inject_inconsistent_values(
    df_payments, column="payment_method", variant_map=PAYMENT_METHOD_VARIANTS,
    pct=DATA_QUALITY_CONFIG["inconsistent_pct"]
)

# 5. Referential integrity (payment for an order that doesn't exist)
df_payments = inject_referential_issue(df_payments, fk_column="order_id", pct=DATA_QUALITY_CONFIG["referential_issue_pct"])

print(f"Payments row count after messiness injection: {len(df_payments):,}")
df_payments.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Generate `Shipping` Table
# MAGIC One shipment per clean order, `delivery_status` correlated with `order_status`. Messiness:
# MAGIC NULL courier/status, invalid status values, referential integrity issues.

# COMMAND ----------

def delivery_status_for(order_status):
    if order_status == "Delivered":
        return "Delivered"
    elif order_status == "Cancelled":
        return "Failed"
    elif order_status == "Returned":
        return random.choices(["Delivered", "Failed"], weights=[0.7, 0.3])[0]
    elif order_status == "Shipped":
        return random.choices(["In Transit", "Out for Delivery"], weights=[0.6, 0.4])[0]
    else:
        return "Pending"

shipping_records = []
for row in _clean_orders_for_payments.itertuples(index=False):
    shipping_records.append({
        "shipment_id": str(uuid.uuid4()),
        "order_id": row.order_id,
        "courier": random.choice(couriers),
        "delivery_status": delivery_status_for(row.order_status if isinstance(row.order_status, str) else "Processing"),
    })

df_shipping = pd.DataFrame(shipping_records)

def inject_invalid_shipping(df, pct):
    df = df.copy()
    n = len(df)
    positions = _sample_positions(n, pct)
    for pos in positions:
        df.iat[pos, df.columns.get_loc("delivery_status")] = random.choice(["LOST_IN_SYSTEM", "???", ""])
    return df

df_shipping = inject_nulls(df_shipping, columns=["courier"], pct=DATA_QUALITY_CONFIG["null_pct"])
df_shipping = inject_invalid_shipping(df_shipping, pct=DATA_QUALITY_CONFIG["invalid_pct"])
df_shipping = inject_referential_issue(df_shipping, fk_column="order_id", pct=DATA_QUALITY_CONFIG["referential_issue_pct"])

print(f"Shipping row count after messiness injection: {len(df_shipping):,}")
df_shipping.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Sanity Checks
# MAGIC With messiness injected, some of these checks are now *expected* to show non-zero issue counts —
# MAGIC that's the point. This cell reports the baseline structural facts (row counts, catalog coverage).

# COMMAND ----------

print("Row counts (post-messiness):")
print(f"  Customers : {len(df_customers):,}")
print(f"  Products  : {len(df_products):,}")
print(f"  Orders    : {len(df_orders):,}")
print(f"  Payments  : {len(df_payments):,}")
print(f"  Shipping  : {len(df_shipping):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Data Quality Report
# MAGIC Measures what actually landed in the data, so you can confirm the injected issues roughly match
# MAGIC `DATA_QUALITY_CONFIG`. This is also a template for the kind of profiling step you'd run in a real
# MAGIC pipeline before deciding on cleaning rules.

# COMMAND ----------

def pct(n, d):
    return f"{(n/d):.2%}" if d else "n/a"

report_lines = []

# --- NULLs ---
report_lines.append("NULL values:")
for table_name, df, cols in [
    ("orders", df_orders, ["discount", "payment_method", "event_time", "coupon_code"]),
    ("customers", df_customers, ["phone", "email"]),
    ("products", df_products, ["description"]),
    ("payments", df_payments, ["payment_method"]),
    ("shipping", df_shipping, ["courier"]),
]:
    for c in cols:
        if c in df.columns:
            n_null = df[c].isna().sum()
            report_lines.append(f"  {table_name}.{c:<16s}: {n_null:>8,} null ({pct(n_null, len(df))})")

# --- Duplicates ---
report_lines.append("\nDuplicate records (by primary key):")
report_lines.append(f"  orders.order_seq duplicated rows : {df_orders.duplicated(subset='order_seq').sum():,}")
report_lines.append(f"  payments duplicated rows (all cols minus payment_id): "
                     f"{df_payments.drop(columns=['payment_id']).duplicated().sum():,}")
report_lines.append(f"  customers duplicated rows (all cols minus customer_id): "
                     f"{df_customers.drop(columns=['customer_id']).duplicated().sum():,}")

# --- Invalid values ---
report_lines.append("\nInvalid values:")
report_lines.append(f"  orders.quantity < 0        : {(pd.to_numeric(df_orders['quantity'], errors='coerce') < 0).sum():,}")
report_lines.append(f"  orders.price < 0            : {(pd.to_numeric(df_orders['price'], errors='coerce') < 0).sum():,}")
report_lines.append(f"  customers.age outside 0-120  : {((pd.to_numeric(df_customers['age'], errors='coerce') < 0) | (pd.to_numeric(df_customers['age'], errors='coerce') > 120)).sum():,}")
report_lines.append(f"  payments.amount < 0          : {(pd.to_numeric(df_payments['amount'], errors='coerce') < 0).sum():,}")
future_cutoff = pd.Timestamp.now()
report_lines.append(f"  orders.event_time in future  : {(pd.to_datetime(df_orders['event_time'], errors='coerce') > future_cutoff).sum():,}")

# --- Outliers ---
report_lines.append("\nOutliers:")
report_lines.append(f"  orders.amount > 1,000,000    : {(pd.to_numeric(df_orders['amount'], errors='coerce') > 1_000_000).sum():,}")
report_lines.append(f"  orders.quantity > 100         : {(pd.to_numeric(df_orders['quantity'], errors='coerce') > 100).sum():,}")
report_lines.append(f"  orders.discount > 100          : {(pd.to_numeric(df_orders['discount'], errors='coerce') > 100).sum():,}")

# --- Referential integrity ---
report_lines.append("\nReferential integrity issues:")
report_lines.append(f"  orders.customer_id not in customers : {(~df_orders['customer_id'].isin(df_customers['customer_id'])).sum():,}")
report_lines.append(f"  orders.product_id not in products    : {(~df_orders['product_id'].isin(df_products['product_id'])).sum():,}")
report_lines.append(f"  payments.order_id not in orders      : {(~df_payments['order_id'].isin(df_orders['order_id'])).sum():,}")
report_lines.append(f"  shipping.order_id not in orders      : {(~df_shipping['order_id'].isin(df_orders['order_id'])).sum():,}")

# --- Late arrival ---
report_lines.append("\nLate-arriving events:")
report_lines.append(f"  orders.is_late_arrival = True : {df_orders['is_late_arrival'].sum():,} ({pct(df_orders['is_late_arrival'].sum(), len(df_orders))})")

print("\n".join(report_lines))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Referential Integrity Recap (Clean vs. Messy)
# MAGIC For clarity: the sanity checks above are the *whole point* of injecting referential issues —
# MAGIC downstream Spark/SQL joins should be written to detect and quarantine these, not silently drop
# MAGIC or crash on them. Nothing to run here; see Step 12's report.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Save All Tables to CSV
# MAGIC CSV is a fixed-schema format, so `coupon_code` will show up as an empty column for pre-threshold
# MAGIC orders here (there's no way to represent "the field doesn't exist for this row" in CSV — see
# MAGIC Step 15 for how JSONL represents that properly).

# COMMAND ----------

df_customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
df_products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
df_orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
df_payments.to_csv(f"{OUTPUT_DIR}/payments.csv", index=False)
df_shipping.to_csv(f"{OUTPUT_DIR}/shipping.csv", index=False)

print(f"All 5 CSVs written to ./{OUTPUT_DIR}/")
for f in ["customers", "products", "orders", "payments", "shipping"]:
    size_mb = os.path.getsize(f"{OUTPUT_DIR}/{f}.csv") / (1024*1024)
    print(f"  {f}.csv - {size_mb:.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15: Save as JSON Lines for Kafka Producers (with true schema evolution)
# MAGIC Each line is one JSON record, matching how a Kafka producer would publish messages one at a time.
# MAGIC For `orders.jsonl`, records generated **before** `SCHEMA_EVOLUTION_THRESHOLD` have the
# MAGIC `coupon_code` key **omitted entirely** (not just null) -- this is what real schema evolution looks
# MAGIC like on the wire, and it's the scenario your Spark job should handle with `mergeSchema` /
# MAGIC `readStream` schema evolution settings rather than failing.

# COMMAND ----------

import datetime as _dt

def _normalize_value(v):
    if isinstance(v, pd.Timestamp):
        return None if pd.isna(v) else v.isoformat()
    if isinstance(v, (_dt.date, _dt.datetime)):
        return v.isoformat()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return None if pd.isna(v) else float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, (list, dict)):
        return v
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v

def orders_to_jsonl(df, path, evolution_threshold):
    with open(path, "w") as f:
        for row in df.to_dict(orient="records"):
            row = {k: _normalize_value(v) for k, v in row.items()}
            # True schema evolution: drop the key for pre-threshold records
            if row.get("order_seq", evolution_threshold) < evolution_threshold:
                row.pop("coupon_code", None)
            f.write(json.dumps(row) + "\n")

def simple_to_jsonl(df, path):
    with open(path, "w") as f:
        for row in df.to_dict(orient="records"):
            row = {k: _normalize_value(v) for k, v in row.items()}
            f.write(json.dumps(row) + "\n")

orders_to_jsonl(df_orders, f"{OUTPUT_DIR}/orders.jsonl", SCHEMA_EVOLUTION_THRESHOLD)
simple_to_jsonl(df_payments, f"{OUTPUT_DIR}/payments.jsonl")
simple_to_jsonl(df_shipping, f"{OUTPUT_DIR}/shipping.jsonl")
simple_to_jsonl(df_customers, f"{OUTPUT_DIR}/customers.jsonl")
simple_to_jsonl(df_products, f"{OUTPUT_DIR}/products.jsonl")

print("JSONL files ready for Kafka producer scripts.")

# Quick proof that schema evolution is real in the JSONL output
with open(f"{OUTPUT_DIR}/orders.jsonl") as f:
    first_line = json.loads(f.readline())
print("\nFirst order record has coupon_code key:", "coupon_code" in first_line)

with open(f"{OUTPUT_DIR}/orders.jsonl") as f:
    lines = f.readlines()
later_line = json.loads(lines[-1])
print("Last order record has coupon_code key:", "coupon_code" in later_line)
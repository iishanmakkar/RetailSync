# Databricks notebook source
# MAGIC %md
# MAGIC # RetailSync - 07 Dashboard & Validation
# MAGIC
# MAGIC Final notebook to validate the Medallion pipeline and prepare dashboard outputs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Delta Tables

# COMMAND ----------

# DBTITLE 1,Cell 3
BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA  = "default"
SILVER_CATALOG = "workspace"
SILVER_SCHEMA  = "default"
GOLD_CATALOG   = "workspace"
GOLD_SCHEMA    = "default"

tables = ["customers", "products", "orders", "payments", "shipping"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Record Counts

# COMMAND ----------

# DBTITLE 1,Cell 5
results = []

for t in tables:
    bronze = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_{t}")
    silver = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_{t}")
    results.append((t, bronze.count(), silver.count()))

validation_df = spark.createDataFrame(
    results,
    ["table", "bronze_count", "silver_count"]
)

display(validation_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Gold Tables

# COMMAND ----------

# DBTITLE 1,Cell 7
gold_tables = [
    # Revenue
    "daily_revenue", "monthly_revenue", "yearly_revenue",
    "revenue_by_category", "revenue_by_city", "revenue_by_state",
    # Customer
    "customer_lifetime_value", "repeat_customers", "new_vs_returning",
    "rfm_segmentation", "top_customers",
    # Product
    "top_products", "bottom_products", "units_sold",
    "revenue_per_product", "sales_by_category",
    # Order
    "average_order_value", "order_trend", "orders_by_day", "orders_by_month",
    # Payment
    "payment_summary", "payment_success_rate", "failed_payments",
    "revenue_by_payment_method",
    # Shipping
    "delivery_performance", "late_deliveries", "avg_delivery_time",
    "shipping_status_distribution",
]

print(f"{'Table':<45} {'Rows':>8}")
print("-" * 55)
for t in gold_tables:
    df = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_{t}")
    print(f"{t:<45} {df.count():>8,}")
print("-" * 55)
print(f"Total Gold tables: {len(gold_tables)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard KPIs

# COMMAND ----------

# DBTITLE 1,Cell 9
G = f"{GOLD_CATALOG}.{GOLD_SCHEMA}"

# Core KPI Cards
display(spark.sql(f"SELECT ROUND(SUM(revenue),2) AS total_revenue FROM {G}.gold_daily_revenue"))
display(spark.sql(f"SELECT SUM(orders) AS total_orders FROM {G}.gold_daily_revenue"))
display(spark.sql(f"SELECT * FROM {G}.gold_average_order_value"))

# Revenue KPIs
display(spark.sql(f"SELECT * FROM {G}.gold_monthly_revenue ORDER BY order_month"))
display(spark.sql(f"SELECT * FROM {G}.gold_revenue_by_category ORDER BY revenue DESC"))
display(spark.sql(f"SELECT * FROM {G}.gold_revenue_by_state ORDER BY revenue DESC"))

# Customer KPIs
display(spark.sql(f"SELECT * FROM {G}.gold_new_vs_returning"))
display(spark.sql(f"SELECT segment, COUNT(*) AS customers FROM {G}.gold_rfm_segmentation GROUP BY segment ORDER BY customers DESC"))
display(spark.sql(f"SELECT * FROM {G}.gold_top_customers ORDER BY lifetime_value DESC LIMIT 10"))

# Product KPIs
display(spark.sql(f"SELECT * FROM {G}.gold_top_products ORDER BY revenue DESC LIMIT 10"))
display(spark.sql(f"SELECT * FROM {G}.gold_sales_by_category ORDER BY revenue DESC"))

# Payment KPIs
display(spark.sql(f"SELECT * FROM {G}.gold_payment_success_rate ORDER BY count DESC"))
display(spark.sql(f"SELECT * FROM {G}.gold_revenue_by_payment_method ORDER BY revenue DESC"))

# Shipping KPIs
display(spark.sql(f"SELECT * FROM {G}.gold_delivery_performance ORDER BY delivery_rate_pct DESC"))
display(spark.sql(f"SELECT * FROM {G}.gold_orders_by_day ORDER BY day_num"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommended Dashboard Visuals

# COMMAND ----------

# DBTITLE 1,Cell 11
dashboard_items = {
    "Revenue KPIs": [
        "Total Revenue (KPI Card)",
        "Total Orders (KPI Card)",
        "Average Order Value (KPI Card)",
        "Monthly Revenue Trend (Line Chart)",
        "Revenue by Category (Bar Chart)",
        "Revenue by State (Bar Chart)",
    ],
    "Customer KPIs": [
        "Top 10 Customers by CLV (Bar Chart)",
        "New vs Returning Customers (Pie Chart)",
        "RFM Segmentation Distribution (Donut Chart)",
        "Repeat Customer Rate (KPI Card)",
    ],
    "Product KPIs": [
        "Top 10 Products by Revenue (Bar Chart)",
        "Bottom 10 Products (Bar Chart)",
        "Sales by Category (Stacked Bar)",
        "Units Sold per Product (Table)",
    ],
    "Order KPIs": [
        "Order Trend by Month (Line Chart)",
        "Orders by Day of Week (Bar Chart)",
    ],
    "Payment KPIs": [
        "Payment Success Rate (Pie Chart)",
        "Revenue by Payment Method (Bar Chart)",
        "Failed Payment Revenue at Risk (KPI Card)",
    ],
    "Shipping KPIs": [
        "Delivery Performance by Courier (Bar Chart)",
        "Shipping Status Distribution (Pie Chart)",
        "Late Deliveries by Courier (Bar Chart)",
    ],
}

for section, items in dashboard_items.items():
    print(f"\n{section}:")
    for item in items:
        print(f"  ✅ {item}")

total_items = sum(len(v) for v in dashboard_items.values())
print(f"\nTotal dashboard visuals: {total_items}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Checklist

# COMMAND ----------

checklist=[
"Data Generator",
"Kafka Producer",
"Kafka Consumer",
"Bronze Layer",
"Silver Layer",
"Gold Layer",
"Databricks SQL",
"Dashboard",
"Validation"
]

for step in checklist:
    print(f"✅ {step}")

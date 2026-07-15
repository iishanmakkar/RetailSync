# Databricks notebook source
# MAGIC %md
# MAGIC # RetailSync - 06 Databricks SQL & Dashboard Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold Delta Tables

# COMMAND ----------

# DBTITLE 1,Cell 3
GOLD_CATALOG = "workspace"
GOLD_SCHEMA  = "default"

# Revenue
daily_revenue             = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_daily_revenue")
monthly_revenue           = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_monthly_revenue")
yearly_revenue            = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_yearly_revenue")
revenue_by_category       = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_category")
revenue_by_city           = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_city")
revenue_by_state          = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_state")
# Customer
customer_lifetime_value   = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_customer_lifetime_value")
repeat_customers          = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_repeat_customers")
new_vs_returning          = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_new_vs_returning")
rfm_segmentation          = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_rfm_segmentation")
top_customers             = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_top_customers")
# Product
top_products              = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_top_products")
bottom_products           = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_bottom_products")
units_sold                = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_units_sold")
revenue_per_product       = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_per_product")
sales_by_category         = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_sales_by_category")
# Order
average_order_value       = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_average_order_value")
order_trend               = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_order_trend")
orders_by_day             = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_orders_by_day")
orders_by_month           = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_orders_by_month")
# Payment
payment_summary           = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_payment_summary")
payment_success_rate      = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_payment_success_rate")
failed_payments           = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_failed_payments")
revenue_by_payment_method = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_payment_method")
# Shipping
delivery_performance      = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_delivery_performance")
late_deliveries           = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_late_deliveries")
avg_delivery_time         = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_avg_delivery_time")
shipping_status_dist      = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_shipping_status_distribution")

print(f"Loaded 28 Gold tables from {GOLD_CATALOG}.{GOLD_SCHEMA}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Permanent Views

# COMMAND ----------

# DBTITLE 1,Cell 5
gold_view_map = {
    # Revenue
    "vw_daily_revenue":               daily_revenue,
    "vw_monthly_revenue":             monthly_revenue,
    "vw_yearly_revenue":              yearly_revenue,
    "vw_revenue_by_category":         revenue_by_category,
    "vw_revenue_by_city":             revenue_by_city,
    "vw_revenue_by_state":            revenue_by_state,
    # Customer
    "vw_customer_lifetime_value":     customer_lifetime_value,
    "vw_repeat_customers":            repeat_customers,
    "vw_new_vs_returning":            new_vs_returning,
    "vw_rfm_segmentation":            rfm_segmentation,
    "vw_top_customers":               top_customers,
    # Product
    "vw_top_products":                top_products,
    "vw_bottom_products":             bottom_products,
    "vw_units_sold":                  units_sold,
    "vw_revenue_per_product":         revenue_per_product,
    "vw_sales_by_category":           sales_by_category,
    # Order
    "vw_average_order_value":         average_order_value,
    "vw_order_trend":                 order_trend,
    "vw_orders_by_day":               orders_by_day,
    "vw_orders_by_month":             orders_by_month,
    # Payment
    "vw_payment_summary":             payment_summary,
    "vw_payment_success_rate":        payment_success_rate,
    "vw_failed_payments":             failed_payments,
    "vw_revenue_by_payment_method":   revenue_by_payment_method,
    # Shipping
    "vw_delivery_performance":        delivery_performance,
    "vw_late_deliveries":             late_deliveries,
    "vw_avg_delivery_time":           avg_delivery_time,
    "vw_shipping_status_distribution": shipping_status_dist,
}

for view_name, df in gold_view_map.items():
    df.createOrReplaceTempView(view_name)

print(f"Registered {len(gold_view_map)} views successfully.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Revenue

# COMMAND ----------

display(spark.sql("""SELECT * FROM vw_daily_revenue ORDER BY order_date"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 10 Products

# COMMAND ----------

display(spark.sql("""SELECT * FROM vw_top_products ORDER BY revenue DESC LIMIT 10"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 10 Customers

# COMMAND ----------

display(spark.sql("""SELECT * FROM vw_customer_lifetime_value ORDER BY lifetime_value DESC LIMIT 10"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average Order Value

# COMMAND ----------

display(spark.sql("""SELECT * FROM vw_average_order_value"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payment Summary

# COMMAND ----------

display(spark.sql("""SELECT * FROM vw_payment_summary"""))

# COMMAND ----------

# DBTITLE 1,Revenue KPIs - Additional
# MAGIC %md
# MAGIC ## Revenue KPIs — Monthly, Yearly, By Category, City, State

# COMMAND ----------

# DBTITLE 1,Monthly Revenue SQL
display(spark.sql("SELECT * FROM vw_monthly_revenue ORDER BY order_month"))

# COMMAND ----------

# DBTITLE 1,Yearly Revenue SQL
display(spark.sql("SELECT * FROM vw_yearly_revenue ORDER BY order_year"))

# COMMAND ----------

# DBTITLE 1,Revenue by Category SQL
display(spark.sql("SELECT * FROM vw_revenue_by_category ORDER BY revenue DESC"))

# COMMAND ----------

# DBTITLE 1,Revenue by City and State SQL
display(spark.sql("SELECT * FROM vw_revenue_by_city  ORDER BY revenue DESC LIMIT 15"))
display(spark.sql("SELECT * FROM vw_revenue_by_state ORDER BY revenue DESC"))

# COMMAND ----------

# DBTITLE 1,Customer KPIs - Header
# MAGIC %md
# MAGIC ## Customer KPIs — New vs Returning, RFM, Top Customers

# COMMAND ----------

# DBTITLE 1,New vs Returning SQL
display(spark.sql("SELECT * FROM vw_new_vs_returning ORDER BY segment"))

# COMMAND ----------

# DBTITLE 1,RFM Segmentation SQL
display(spark.sql("SELECT segment, COUNT(*) AS customers FROM vw_rfm_segmentation GROUP BY segment ORDER BY customers DESC"))

# COMMAND ----------

# DBTITLE 1,Top Customers SQL
display(spark.sql("SELECT * FROM vw_top_customers ORDER BY lifetime_value DESC LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,Product KPIs - Header
# MAGIC %md
# MAGIC ## Product KPIs — Bottom Products, Sales by Category

# COMMAND ----------

# DBTITLE 1,Bottom Products SQL
display(spark.sql("SELECT * FROM vw_bottom_products ORDER BY revenue ASC LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,Sales by Category SQL
display(spark.sql("SELECT * FROM vw_sales_by_category ORDER BY revenue DESC"))

# COMMAND ----------

# DBTITLE 1,Order KPIs - Header
# MAGIC %md
# MAGIC ## Order KPIs — Trend, By Day of Week, By Month

# COMMAND ----------

# DBTITLE 1,Order Trend SQL
display(spark.sql("SELECT * FROM vw_orders_by_month ORDER BY order_month"))
display(spark.sql("SELECT * FROM vw_orders_by_day ORDER BY day_num"))

# COMMAND ----------

# DBTITLE 1,Payment KPIs - Header
# MAGIC %md
# MAGIC ## Payment KPIs — Success Rate, Failed, Revenue by Method

# COMMAND ----------

# DBTITLE 1,Payment Success Rate SQL
display(spark.sql("SELECT * FROM vw_payment_success_rate ORDER BY count DESC"))
display(spark.sql("SELECT * FROM vw_revenue_by_payment_method ORDER BY revenue DESC"))

# COMMAND ----------

# DBTITLE 1,Shipping KPIs - Header
# MAGIC %md
# MAGIC ## Shipping KPIs — Delivery Performance, Late Deliveries, Status Distribution

# COMMAND ----------

# DBTITLE 1,Delivery Performance SQL
display(spark.sql("SELECT * FROM vw_delivery_performance ORDER BY delivery_rate_pct DESC"))
display(spark.sql("SELECT * FROM vw_late_deliveries ORDER BY count DESC"))
display(spark.sql("SELECT * FROM vw_shipping_status_distribution ORDER BY delivery_status, courier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard KPIs

# COMMAND ----------

# DBTITLE 1,Dashboard KPIs
# Core KPI cards
display(spark.sql("SELECT ROUND(SUM(revenue), 2) AS total_revenue FROM vw_daily_revenue"))
display(spark.sql("SELECT SUM(orders) AS total_orders FROM vw_daily_revenue"))
display(spark.sql("SELECT * FROM vw_average_order_value"))
# New vs Returning
display(spark.sql("SELECT * FROM vw_new_vs_returning"))
# RFM segment distribution
display(spark.sql("SELECT segment, COUNT(*) AS customers FROM vw_rfm_segmentation GROUP BY segment ORDER BY customers DESC"))
# Payment health
display(spark.sql("SELECT payment_status, count, pct_of_total FROM vw_payment_success_rate ORDER BY count DESC"))
# Delivery health
display(spark.sql("SELECT courier, total_shipments, delivered, delivery_rate_pct FROM vw_delivery_performance ORDER BY delivery_rate_pct DESC"))


# COMMAND ----------

# DBTITLE 1,Build a Databricks SQL Dashboard
# MAGIC %md
# MAGIC ## Build a Databricks SQL Dashboard
# MAGIC
# MAGIC All 28 Gold views (prefixed `vw_`) are registered in this session.
# MAGIC
# MAGIC ### Recommended Visualizations
# MAGIC
# MAGIC **Revenue**
# MAGIC - Monthly Revenue Trend (Line Chart) — `vw_monthly_revenue`
# MAGIC - Revenue by Category (Bar Chart) — `vw_revenue_by_category`
# MAGIC - Revenue by State (Bar/Map) — `vw_revenue_by_state`
# MAGIC
# MAGIC **Customer**
# MAGIC - New vs Returning (Pie Chart) — `vw_new_vs_returning`
# MAGIC - RFM Segment Distribution (Donut Chart) — `vw_rfm_segmentation`
# MAGIC - Top Customers (Table / Bar) — `vw_top_customers`
# MAGIC
# MAGIC **Product**
# MAGIC - Top 10 Products (Bar Chart) — `vw_top_products`
# MAGIC - Bottom 10 Products (Bar Chart) — `vw_bottom_products`
# MAGIC - Sales by Category (Stacked Bar) — `vw_sales_by_category`
# MAGIC
# MAGIC **Order**
# MAGIC - Order Trend by Month (Line Chart) — `vw_orders_by_month`
# MAGIC - Orders by Day of Week (Bar Chart) — `vw_orders_by_day`
# MAGIC
# MAGIC **Payment**
# MAGIC - Payment Success Rate (Pie Chart) — `vw_payment_success_rate`
# MAGIC - Revenue by Payment Method (Bar Chart) — `vw_revenue_by_payment_method`
# MAGIC
# MAGIC **Shipping**
# MAGIC - Delivery Performance by Courier (Bar Chart) — `vw_delivery_performance`
# MAGIC - Shipping Status Distribution (Pie Chart) — `vw_shipping_status_distribution`
# MAGIC
# MAGIC **KPI Cards**
# MAGIC - Total Revenue (`vw_daily_revenue`)
# MAGIC - Total Orders (`vw_daily_revenue`)
# MAGIC - Average Order Value (`vw_average_order_value`)
# MAGIC - Payment Success Rate (`vw_payment_success_rate`)
# MAGIC
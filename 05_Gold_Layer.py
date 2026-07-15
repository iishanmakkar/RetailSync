# Databricks notebook source
# MAGIC %md
# MAGIC # RetailSync - 05 Gold Layer
# MAGIC
# MAGIC Build business-ready Gold Delta tables from the Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Tables

# COMMAND ----------

# DBTITLE 1,Cell 5
SILVER_CATALOG = "workspace"
SILVER_SCHEMA  = "default"
GOLD_CATALOG   = "workspace"
GOLD_SCHEMA    = "default"

def parse_silver(table_name):
    """Load a Silver table and expand the JSON value column into typed columns."""
    df = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_{table_name}")
    sample_json = (
        df.select("value").filter(col("value").isNotNull())
          .limit(1).collect()[0][0]
    )
    json_schema = (
        spark.range(1)
             .select(schema_of_json(lit(sample_json)))
             .collect()[0][0]
    )
    return (
        df.withColumn("data", from_json(col("value"), json_schema))
          .select("data.*", col("timestamp").alias("kafka_timestamp"))
    )

customers = parse_silver("customers")
products  = parse_silver("products")
orders    = parse_silver("orders")
payments  = parse_silver("payments")
shipping  = parse_silver("shipping")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Business Data Model

# COMMAND ----------

# DBTITLE 1,Cell 7
# Enrich orders with customer and product dimensions.
# Payments and shipping link via order_id (not payment_id/shipment_id).
pay_join_key  = "order_id" if "order_id" in payments.columns  else None
ship_join_key = "order_id" if "order_id" in shipping.columns  else None

sales = (
    orders
    .join(customers.select("customer_id", "name", "age", "gender",
                            col("city").alias("customer_city")),
          "customer_id", "left")
    .join(products.select("product_id", "product_name", "category", "brand"),
          "product_id", "left")
)

if pay_join_key:
    pay_cols = [c for c in payments.columns
                if c not in set(sales.columns) - {pay_join_key}]
    sales = sales.join(payments.select(*pay_cols), pay_join_key, "left")

if ship_join_key:
    ship_cols = [c for c in shipping.columns
                 if c not in set(sales.columns) - {ship_join_key}]
    sales = sales.join(shipping.select(*ship_cols), ship_join_key, "left")

display(sales.limit(10))


# COMMAND ----------

# DBTITLE 1,Date Helper
# Helper: safely parse event_time string, treating "NaT" as null
def _event_date(col_name="event_time"):
    return to_date(
        when(col(col_name).isin("NaT", "", "null"), None)
        .otherwise(col(col_name))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 1 - Daily Revenue

# COMMAND ----------

# DBTITLE 1,Cell 9
daily_revenue=(sales
.groupBy(to_date(
    when(col("event_time").isin("NaT", "", "null"), None)
    .otherwise(col("event_time"))
).alias("order_date"))
.agg(
    sum("amount").alias("revenue"),
    count("*").alias("orders")
))

daily_revenue.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_daily_revenue")
display(daily_revenue)


# COMMAND ----------

# DBTITLE 1,KPI - Monthly Revenue
# MAGIC %md
# MAGIC ## KPI - Monthly Revenue

# COMMAND ----------

# DBTITLE 1,Monthly Revenue
monthly_revenue = (sales
    .withColumn("order_month", date_trunc("month", _event_date()))
    .groupBy("order_month")
    .agg(sum("amount").alias("revenue"), count("*").alias("orders"))
    .orderBy("order_month"))

monthly_revenue.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_monthly_revenue")
display(monthly_revenue)

# COMMAND ----------

# DBTITLE 1,KPI - Yearly Revenue
# MAGIC %md
# MAGIC ## KPI - Yearly Revenue

# COMMAND ----------

# DBTITLE 1,Yearly Revenue
yearly_revenue = (sales
    .withColumn("order_year", year(_event_date()))
    .groupBy("order_year")
    .agg(sum("amount").alias("revenue"), count("*").alias("orders"))
    .orderBy("order_year"))

yearly_revenue.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_yearly_revenue")
display(yearly_revenue)

# COMMAND ----------

# DBTITLE 1,KPI - Revenue by Category
# MAGIC %md
# MAGIC ## KPI - Revenue by Category

# COMMAND ----------

# DBTITLE 1,Revenue by Category
revenue_by_category = (sales
    .groupBy("category")
    .agg(sum("amount").alias("revenue"), countDistinct("order_id").alias("orders"))
    .orderBy(desc("revenue")))

revenue_by_category.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_category")
display(revenue_by_category)

# COMMAND ----------

# DBTITLE 1,KPI - Revenue by City
# MAGIC %md
# MAGIC ## KPI - Revenue by City

# COMMAND ----------

# DBTITLE 1,Revenue by City
revenue_by_city = (sales
    .groupBy("city")
    .agg(sum("amount").alias("revenue"), countDistinct("order_id").alias("orders"))
    .orderBy(desc("revenue")))

revenue_by_city.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_city")
display(revenue_by_city)

# COMMAND ----------

# DBTITLE 1,KPI - Revenue by State
# MAGIC %md
# MAGIC ## KPI - Revenue by State

# COMMAND ----------

# DBTITLE 1,Revenue by State
revenue_by_state = (sales
    .groupBy("state")
    .agg(sum("amount").alias("revenue"), countDistinct("order_id").alias("orders"))
    .orderBy(desc("revenue")))

revenue_by_state.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_state")
display(revenue_by_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 2 - Top Products

# COMMAND ----------

# DBTITLE 1,Cell 11
top_products=(sales
.groupBy("product_id")
.agg(
    sum("quantity").alias("units_sold"),
    sum("amount").alias("revenue")
)
.orderBy(desc("revenue")))

top_products.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_top_products")
display(top_products.limit(20))


# COMMAND ----------

# DBTITLE 1,KPI - Bottom Products
# MAGIC %md
# MAGIC ## KPI - Bottom Products

# COMMAND ----------

# DBTITLE 1,Bottom Products
bottom_products = (sales
    .groupBy("product_id", "product_name", "category", "brand")
    .agg(sum("quantity").alias("units_sold"), sum("amount").alias("revenue"))
    .orderBy(asc("revenue")))

bottom_products.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_bottom_products")
display(bottom_products.limit(20))

# COMMAND ----------

# DBTITLE 1,KPI - Units Sold
# MAGIC %md
# MAGIC ## KPI - Units Sold

# COMMAND ----------

# DBTITLE 1,Units Sold
units_sold = (sales
    .groupBy("product_id", "product_name", "category", "brand")
    .agg(sum("quantity").alias("units_sold"))
    .orderBy(desc("units_sold")))

units_sold.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_units_sold")
display(units_sold.limit(20))

# COMMAND ----------

# DBTITLE 1,KPI - Revenue per Product
# MAGIC %md
# MAGIC ## KPI - Revenue per Product

# COMMAND ----------

# DBTITLE 1,Revenue per Product
revenue_per_product = (sales
    .groupBy("product_id", "product_name", "category", "brand")
    .agg(
        sum("amount").alias("total_revenue"),
        sum("quantity").alias("units_sold"),
        countDistinct("order_id").alias("orders"),
        round(avg("amount"), 2).alias("avg_order_revenue")
    )
    .orderBy(desc("total_revenue")))

revenue_per_product.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_per_product")
display(revenue_per_product.limit(20))

# COMMAND ----------

# DBTITLE 1,KPI - Sales by Category
# MAGIC %md
# MAGIC ## KPI - Sales by Category

# COMMAND ----------

# DBTITLE 1,Sales by Category
sales_by_category = (sales
    .groupBy("category")
    .agg(
        sum("amount").alias("revenue"),
        sum("quantity").alias("units_sold"),
        countDistinct("order_id").alias("orders"),
        countDistinct("product_id").alias("products")
    )
    .orderBy(desc("revenue")))

sales_by_category.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_sales_by_category")
display(sales_by_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 3 - Customer Lifetime Value

# COMMAND ----------

# DBTITLE 1,Cell 13
clv=(sales
.groupBy("customer_id")
.agg(
    sum("amount").alias("lifetime_value"),
    countDistinct("order_id").alias("orders")
)
.orderBy(desc("lifetime_value")))

clv.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_customer_lifetime_value")
display(clv.limit(20))


# COMMAND ----------

# DBTITLE 1,KPI - Repeat Customers
# MAGIC %md
# MAGIC ## KPI - Repeat Customers

# COMMAND ----------

# DBTITLE 1,Repeat Customers
repeat_customers = (sales
    .groupBy("customer_id", "name", "gender", "customer_city")
    .agg(countDistinct("order_id").alias("order_count"), sum("amount").alias("total_spend"))
    .filter(col("order_count") > 1)
    .orderBy(desc("order_count")))

repeat_customers.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_repeat_customers")
display(repeat_customers.limit(20))

# COMMAND ----------

# DBTITLE 1,KPI - New vs Returning Customers
# MAGIC %md
# MAGIC ## KPI - New vs Returning Customers

# COMMAND ----------

# DBTITLE 1,New vs Returning Customers
new_vs_returning = (sales
    .groupBy("customer_id")
    .agg(countDistinct("order_id").alias("order_count"))
    .withColumn("segment", when(col("order_count") == 1, lit("New")).otherwise(lit("Returning")))
    .groupBy("segment")
    .agg(count("*").alias("customers"), sum("order_count").alias("total_orders"))
    .orderBy("segment"))

new_vs_returning.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_new_vs_returning")
display(new_vs_returning)

# COMMAND ----------

# DBTITLE 1,KPI - Customer Segmentation (RFM)
# MAGIC %md
# MAGIC ## KPI - Customer Segmentation (RFM)
# MAGIC Scores each customer on Recency (days since last order), Frequency (distinct orders), and Monetary (total spend) — each bucketed into 3 tiers. Segments: Champions (8-9), Loyal (6-7), Potential (4-5), At Risk (3).

# COMMAND ----------

# DBTITLE 1,RFM Segmentation
snapshot_date = to_date(lit(str(date.today())))

rfm_base = (sales
    .withColumn("event_date", _event_date())
    .groupBy("customer_id")
    .agg(
        datediff(snapshot_date, max("event_date")).alias("recency_days"),
        countDistinct("order_id").alias("frequency"),
        sum("amount").alias("monetary")
    )
    .filter(col("recency_days").isNotNull())
)

rfm = (rfm_base
    .withColumn("R", ntile(3).over(Window.orderBy(asc("recency_days"))))  # 1=most recent
    .withColumn("F", ntile(3).over(Window.orderBy(asc("frequency"))))     # 3=most frequent
    .withColumn("M", ntile(3).over(Window.orderBy(asc("monetary"))))      # 3=highest spend
    .withColumn("rfm_score", (lit(4) - col("R")) + col("F") + col("M"))  # R flipped: best=3
    .withColumn("segment",
        when(col("rfm_score") >= 8, lit("Champions"))
        .when(col("rfm_score") >= 6, lit("Loyal"))
        .when(col("rfm_score") >= 4, lit("Potential"))
        .otherwise(lit("At Risk")))
)

rfm.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_rfm_segmentation")
display(rfm.groupBy("segment").count().orderBy(desc("count")))

# COMMAND ----------

# DBTITLE 1,KPI - Top Customers
# MAGIC %md
# MAGIC ## KPI - Top Customers

# COMMAND ----------

# DBTITLE 1,Top Customers
top_customers = (sales
    .groupBy("customer_id", "name", "gender", "age", "customer_city")
    .agg(
        sum("amount").alias("lifetime_value"),
        countDistinct("order_id").alias("orders"),
        round(avg("amount"), 2).alias("avg_order_value")
    )
    .orderBy(desc("lifetime_value")))

top_customers.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_top_customers")
display(top_customers.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 4 - Average Order Value

# COMMAND ----------

# DBTITLE 1,Cell 15
aov=sales.agg(avg("amount").alias("average_order_value"))
aov.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_average_order_value")
display(aov)


# COMMAND ----------

# DBTITLE 1,KPI - Order Trend
# MAGIC %md
# MAGIC ## KPI - Order Trend

# COMMAND ----------

# DBTITLE 1,Order Trend
order_trend = (sales
    .withColumn("order_date", _event_date())
    .groupBy("order_date")
    .agg(countDistinct("order_id").alias("orders"), sum("amount").alias("revenue"))
    .orderBy("order_date"))

order_trend.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_order_trend")
display(order_trend)

# COMMAND ----------

# DBTITLE 1,KPI - Orders by Day of Week
# MAGIC %md
# MAGIC ## KPI - Orders by Day of Week

# COMMAND ----------

# DBTITLE 1,Orders by Day
orders_by_day = (sales
    .withColumn("event_date", _event_date())
    .filter(col("event_date").isNotNull())
    .withColumn("day_num",  dayofweek(col("event_date")))
    .withColumn("day_name", date_format(col("event_date"), "EEEE"))
    .groupBy("day_num", "day_name")
    .agg(countDistinct("order_id").alias("orders"), sum("amount").alias("revenue"))
    .orderBy("day_num"))

orders_by_day.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_orders_by_day")
display(orders_by_day)

# COMMAND ----------

# DBTITLE 1,KPI - Orders by Month
# MAGIC %md
# MAGIC ## KPI - Orders by Month

# COMMAND ----------

# DBTITLE 1,Orders by Month
orders_by_month = (sales
    .withColumn("order_month", date_format(_event_date(), "yyyy-MM"))
    .groupBy("order_month")
    .agg(countDistinct("order_id").alias("orders"), sum("amount").alias("revenue"))
    .orderBy("order_month"))

orders_by_month.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_orders_by_month")
display(orders_by_month)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 5 - Payment Status

# COMMAND ----------

# DBTITLE 1,Cell 17
if "payment_status" in payments.columns:
    payment_summary = payments.groupBy("payment_status").count()
else:
    payment_summary = payments.groupBy().count()

payment_summary.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_payment_summary")
display(payment_summary)


# COMMAND ----------

# DBTITLE 1,KPI - Payment Success Rate
# MAGIC %md
# MAGIC ## KPI - Payment Success Rate

# COMMAND ----------

# DBTITLE 1,Payment Success Rate
payment_success_rate = (payments
    .groupBy("payment_status")
    .agg(count("*").alias("count"), sum("amount").alias("total_amount"))
    .withColumn("total", sum("count").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    .withColumn("pct_of_total", round(col("count") / col("total") * 100, 2))
    .drop("total")
    .orderBy(desc("count")))

payment_success_rate.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_payment_success_rate")
display(payment_success_rate)

# COMMAND ----------

# DBTITLE 1,KPI - Failed Payments
# MAGIC %md
# MAGIC ## KPI - Failed Payments

# COMMAND ----------

# DBTITLE 1,Failed Payments
failed_payments = (payments
    .filter(
        col("payment_status").isin("Failed", "ERROR", "unknown_status") |
        col("payment_status").isNull() |
        (col("payment_status") == lit(""))
    )
    .groupBy("payment_status", "payment_method")
    .agg(count("*").alias("failed_count"), sum("amount").alias("revenue_at_risk"))
    .orderBy(desc("failed_count")))

failed_payments.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_failed_payments")
display(failed_payments)

# COMMAND ----------

# DBTITLE 1,KPI - Revenue by Payment Method
# MAGIC %md
# MAGIC ## KPI - Revenue by Payment Method

# COMMAND ----------

# DBTITLE 1,Revenue by Payment Method
revenue_by_payment = (payments
    .groupBy("payment_method")
    .agg(
        count("*").alias("transactions"),
        sum("amount").alias("revenue"),
        round(avg("amount"), 2).alias("avg_transaction")
    )
    .orderBy(desc("revenue")))

revenue_by_payment.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_revenue_by_payment_method")
display(revenue_by_payment)

# COMMAND ----------

# DBTITLE 1,Shipping KPIs
# MAGIC %md
# MAGIC ---
# MAGIC ## Shipping KPIs

# COMMAND ----------

# DBTITLE 1,KPI - Delivery Performance
# MAGIC %md
# MAGIC ## KPI - Delivery Performance

# COMMAND ----------

# DBTITLE 1,Delivery Performance
delivery_performance = (shipping
    .groupBy("courier")
    .agg(
        count("*").alias("total_shipments"),
        sum(when(col("delivery_status") == "Delivered", 1).otherwise(0)).alias("delivered"),
        sum(when(col("delivery_status").isin("Pending", "Failed"), 1).otherwise(0)).alias("pending_or_failed")
    )
    .withColumn("delivery_rate_pct", round(col("delivered") / col("total_shipments") * 100, 2))
    .orderBy(desc("delivery_rate_pct")))

delivery_performance.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_delivery_performance")
display(delivery_performance)

# COMMAND ----------

# DBTITLE 1,KPI - Late Deliveries
# MAGIC %md
# MAGIC ## KPI - Late Deliveries

# COMMAND ----------

# DBTITLE 1,Late Deliveries
late_deliveries = (shipping
    .filter(col("delivery_status").isin("Pending", "Failed"))
    .groupBy("courier", "delivery_status")
    .agg(count("*").alias("count"))
    .orderBy(desc("count")))

late_deliveries.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_late_deliveries")
display(late_deliveries)

# COMMAND ----------

# DBTITLE 1,KPI - Average Delivery Time
# MAGIC %md
# MAGIC ## KPI - Average Delivery Time
# MAGIC Proxy: days from order `event_time` to shipment Kafka ingestion timestamp, grouped by courier.

# COMMAND ----------

# DBTITLE 1,Average Delivery Time
avg_delivery_time = (shipping
    .join(
        sales.select("order_id", "event_time").dropDuplicates(["order_id"]),
        "order_id", "left"
    )
    .withColumn("order_date", _event_date())
    .withColumn("days_to_process",
        when(col("order_date").isNotNull(),
            datediff(col("kafka_timestamp").cast("date"), col("order_date")))
        .otherwise(None))
    .filter(col("days_to_process").isNotNull() & (col("days_to_process") >= 0))
    .groupBy("courier")
    .agg(
        round(avg("days_to_process"), 1).alias("avg_days_to_process"),
        count("*").alias("shipments")
    )
    .orderBy("avg_days_to_process"))

avg_delivery_time.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_avg_delivery_time")
display(avg_delivery_time)

# COMMAND ----------

# DBTITLE 1,KPI - Shipping Status Distribution
# MAGIC %md
# MAGIC ## KPI - Shipping Status Distribution

# COMMAND ----------

# DBTITLE 1,Shipping Status Distribution
shipping_status_dist = (shipping
    .groupBy("delivery_status", "courier")
    .agg(count("*").alias("shipments"))
    .orderBy("delivery_status", "courier"))

shipping_status_dist.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_shipping_status_distribution")
display(shipping_status_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register SQL Tables

# COMMAND ----------

# DBTITLE 1,Cell 19
gold_views = {
    # Revenue
    "gold_daily_revenue":               daily_revenue,
    "gold_monthly_revenue":             monthly_revenue,
    "gold_yearly_revenue":              yearly_revenue,
    "gold_revenue_by_category":         revenue_by_category,
    "gold_revenue_by_city":             revenue_by_city,
    "gold_revenue_by_state":            revenue_by_state,
    # Customer
    "gold_customer_lifetime_value":     clv,
    "gold_repeat_customers":            repeat_customers,
    "gold_new_vs_returning":            new_vs_returning,
    "gold_rfm_segmentation":            rfm,
    "gold_top_customers":               top_customers,
    # Product
    "gold_top_products":                top_products,
    "gold_bottom_products":             bottom_products,
    "gold_units_sold":                  units_sold,
    "gold_revenue_per_product":         revenue_per_product,
    "gold_sales_by_category":           sales_by_category,
    # Order
    "gold_average_order_value":         aov,
    "gold_order_trend":                 order_trend,
    "gold_orders_by_day":               orders_by_day,
    "gold_orders_by_month":             orders_by_month,
    # Payment
    "gold_payment_summary":             payment_summary,
    "gold_payment_success_rate":        payment_success_rate,
    "gold_failed_payments":             failed_payments,
    "gold_revenue_by_payment_method":   revenue_by_payment,
    # Shipping
    "gold_delivery_performance":        delivery_performance,
    "gold_late_deliveries":             late_deliveries,
    "gold_avg_delivery_time":           avg_delivery_time,
    "gold_shipping_status_distribution": shipping_status_dist,
}

for view_name, df in gold_views.items():
    df.createOrReplaceTempView(view_name)

print(f"Registered {len(gold_views)} Gold views.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# DBTITLE 1,Cell 21
gold_tables = {
    # Revenue
    "gold_daily_revenue":               daily_revenue,
    "gold_monthly_revenue":             monthly_revenue,
    "gold_yearly_revenue":              yearly_revenue,
    "gold_revenue_by_category":         revenue_by_category,
    "gold_revenue_by_city":             revenue_by_city,
    "gold_revenue_by_state":            revenue_by_state,
    # Customer
    "gold_customer_lifetime_value":     clv,
    "gold_repeat_customers":            repeat_customers,
    "gold_new_vs_returning":            new_vs_returning,
    "gold_rfm_segmentation":            rfm,
    "gold_top_customers":               top_customers,
    # Product
    "gold_top_products":                top_products,
    "gold_bottom_products":             bottom_products,
    "gold_units_sold":                  units_sold,
    "gold_revenue_per_product":         revenue_per_product,
    "gold_sales_by_category":           sales_by_category,
    # Order
    "gold_average_order_value":         aov,
    "gold_order_trend":                 order_trend,
    "gold_orders_by_day":               orders_by_day,
    "gold_orders_by_month":             orders_by_month,
    # Payment
    "gold_payment_summary":             payment_summary,
    "gold_payment_success_rate":        payment_success_rate,
    "gold_failed_payments":             failed_payments,
    "gold_revenue_by_payment_method":   revenue_by_payment,
    # Shipping
    "gold_delivery_performance":        delivery_performance,
    "gold_late_deliveries":             late_deliveries,
    "gold_avg_delivery_time":           avg_delivery_time,
    "gold_shipping_status_distribution": shipping_status_dist,
}

print(f"{'Table':<45} {'Rows':>8}")
print("-" * 55)
for name, df in gold_tables.items():
    print(f"{name:<45} {df.count():>8,}")
print("-" * 55)
print(f"Total Gold tables: {len(gold_tables)}")

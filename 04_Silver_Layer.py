# Databricks notebook source
# MAGIC %md
# MAGIC # RetailSync - 04 Silver Layer
# MAGIC
# MAGIC Clean and transform Bronze Delta tables into Silver Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze & Silver Paths

# COMMAND ----------

# DBTITLE 1,Cell 5
BRONZE_CATALOG = "workspace"
BRONZE_SCHEMA  = "default"
SILVER_CATALOG = "workspace"
SILVER_SCHEMA  = "default"

TABLES = ["customers", "products", "orders", "payments", "shipping"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic Cleaning Function

# COMMAND ----------

# DBTITLE 1,Cell 7
def clean_dataframe(df):

    # Remove duplicate rows
    df = df.dropDuplicates()

    # Remove rows where every column is null
    df = df.na.drop(how="all")

    # Trim string columns — compute dtypes once, apply all at once with withColumns
    string_cols = {c: trim(col(c)) for c, t in df.dtypes if t == "string"}
    if string_cols:
        df = df.withColumns(string_cols)

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Bronze Tables

# COMMAND ----------

# DBTITLE 1,Cell 9
silver_tables = {}

for table in TABLES:

    bronze_table = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_{table}"
    silver_table  = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_{table}"

    df = spark.table(bronze_table)

    clean_df = clean_dataframe(df)

    (clean_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table))

    silver_tables[table] = silver_table

    print(f"Created Silver table: {silver_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Cell 11
for table, table_name in silver_tables.items():

    df = spark.table(table_name)
    dtypes = df.dtypes  # fetch schema once

    print("=" * 60)
    print(table.upper())
    print("Rows:", df.count())
    print("Columns:", len(dtypes))
    df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Silver Tables

# COMMAND ----------

# DBTITLE 1,Cell 13
for table, table_name in silver_tables.items():

    print(f"===== {table.upper()} =====")
    display(spark.table(table_name).limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Proceed to **05_Gold_Layer.ipynb** to build business aggregations such as:
# MAGIC - Daily Revenue
# MAGIC - Monthly Revenue
# MAGIC - Customer Lifetime Value
# MAGIC - Top Products
# MAGIC - Average Order Value
# MAGIC - Payment Success Rate
# MAGIC
# Databricks notebook source
# MAGIC %md
# MAGIC # RetailSync - 03 Kafka Consumer
# MAGIC
# MAGIC Consume Kafka topics from Confluent Cloud using PySpark Structured Streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Kafka Connector

# COMMAND ----------

# Run once if required
# %pip install confluent-kafka


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Credentials

# COMMAND ----------

# DBTITLE 1,Cell 7
# Databricks Secrets (Recommended)
# BOOTSTRAP_SERVER = dbutils.secrets.get("retailsync","bootstrap-server")
# API_KEY = dbutils.secrets.get("retailsync","api-key")
# API_SECRET = dbutils.secrets.get("retailsync","api-secret")

# Or use environment variables if preferred
import os
from dotenv import load_dotenv
load_dotenv(override=True)
BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Kafka Options

# COMMAND ----------

# DBTITLE 1,Cell 9
TOPIC = "orders"

kafka_options = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
    "subscribe": TOPIC,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config":
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'
}


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read Kafka Stream

# COMMAND ----------

raw_df = (
    spark.readStream
         .format("kafka")
         .options(**kafka_options)
         .load()
)

raw_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Convert Kafka Values

# COMMAND ----------

# DBTITLE 1,Cell 13
json_df = raw_df.selectExpr(
    "CAST(key AS STRING) AS message_key",
    "CAST(value AS STRING) AS value",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

(
    json_df.writeStream
    .outputMode("append")
    .format("memory")
    .queryName("kafka_raw_preview")
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/kafka_consumer_mem_v3")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

display(spark.sql("SELECT * FROM kafka_raw_preview LIMIT 50"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Parse Orders JSON

# COMMAND ----------

# DBTITLE 1,Cell 15
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("payment_id", StringType()),
    StructField("shipment_id", StringType()),
    StructField("event_time", TimestampType())
])

orders_df = (
    json_df
    .select(from_json(col("value"), order_schema).alias("data"))
    .select("data.*")
)

(
    orders_df.writeStream
    .outputMode("append")
    .format("memory")
    .queryName("orders_parsed_preview")
    .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/kafka_consumer_orders_v2")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

display(spark.sql("SELECT * FROM orders_parsed_preview LIMIT 50"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Ready for Bronze Layer
# MAGIC The next notebook writes this streaming DataFrame to Delta Bronze tables with checkpointing.

# COMMAND ----------

# MAGIC %md
# MAGIC # Consume All Kafka Topics and Write Bronze Tables
# MAGIC
# MAGIC The following cells extend the notebook to consume every RetailSync topic and
# MAGIC write each stream to its own Bronze Delta table with a dedicated checkpoint.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 18
TOPICS = ["customers", "products", "orders", "payments", "shipping"]

BRONZE_CATALOG  = "workspace"
BRONZE_SCHEMA   = "default"
BASE_CHECKPOINT = "/Volumes/workspace/default/checkpoints/bronze"

streams = {}

for topic in TOPICS:

    options = {
        "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config":
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'
    }

    df = (
        spark.readStream
             .format("kafka")
             .options(**options)
             .load()
             .selectExpr(
                 "CAST(key AS STRING) AS message_key",
                 "CAST(value AS STRING) AS value",
                 "topic",
                 "partition",
                 "offset",
                 "timestamp"
             )
    )

    query = (
        df.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", f"{BASE_CHECKPOINT}/{topic}")
          .trigger(availableNow=True)
          .toTable(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_{topic}")
    )

    streams[topic] = query
    print(f"Started Bronze stream: {topic} → {BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_{topic}")

print("\nWaiting for all streams to complete...")
for topic, query in streams.items():
    query.awaitTermination()
    print(f"  ✓ {topic}")

print("\nAll Bronze tables written successfully.")


# COMMAND ----------

# DBTITLE 1,Cell 19
for topic in TOPICS:
    print(f"===== {topic.upper()} =====")
    display(spark.table(f"workspace.default.bronze_{topic}").limit(10))

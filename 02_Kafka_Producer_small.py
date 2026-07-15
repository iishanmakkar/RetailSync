# Databricks notebook source
# MAGIC %md
# MAGIC # RetailSync - 02 Kafka Producer
# MAGIC Production-ready Kafka Producer for Confluent Cloud.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Libraries

# COMMAND ----------

# MAGIC %pip install confluent-kafka python-dotenv tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

# DBTITLE 1,Cell 5
import json
import os
import time
import logging
from pathlib import Path
from confluent_kafka import Producer
from dotenv import load_dotenv


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration
# MAGIC Use Databricks Secrets (recommended) or environment variables.

# COMMAND ----------

# DBTITLE 1,Cell 7
load_dotenv(override=True)

# Databricks Secrets (recommended)
# BOOTSTRAP_SERVER = dbutils.secrets.get("retailsync","bootstrap-server")
# API_KEY = dbutils.secrets.get("retailsync","api-key")
# API_SECRET = dbutils.secrets.get("retailsync","api-secret")

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

DATA_FOLDER = Path(os.getcwd()) / "data"

TOPICS = {
    "customers":"customers",
    "products":"products",
    "orders":"orders",
    "payments":"payments",
    "shipping":"shipping"
}

KEY_COLUMNS = {
    "customers":"customer_id",
    "products":"product_id",
    "orders":"order_id",
    "payments":"payment_id",
    "shipping":"shipment_id"
}

BATCH_SIZE = 1000
SEND_DELAY = 0
MAX_RETRIES = 3


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Logger

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("RetailSyncProducer")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Producer Configuration

# COMMAND ----------

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
    "client.id": "RetailSyncProducer",
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "snappy",
    "linger.ms": 10,
    "batch.num.messages": 1000,
}
producer = Producer(producer_config)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delivery Callback

# COMMAND ----------

delivery_stats = {"success":0,"failed":0}

def delivery_report(err,msg):
    if err:
        delivery_stats["failed"] += 1
        logger.error(f"Delivery failed: {err}")
    else:
        delivery_stats["success"] += 1
        logger.info(
            f"Delivered | Topic={msg.topic()} | Partition={msg.partition()} | Offset={msg.offset()}"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Kafka Producer Service

# COMMAND ----------

class KafkaProducerService:

    def __init__(self, producer):
        self.producer = producer

    def publish_jsonl(self, file_path: Path, topic: str):

        if not file_path.exists():
            logger.error(f"{file_path} not found.")
            return

        key_column = KEY_COLUMNS.get(topic)
        total = 0
        failed = 0

        logger.info(f"Publishing {file_path.name} -> {topic}")

        start = time.time()

        with open(file_path,"r",encoding="utf-8") as f:

            for line in f:

                try:
                    record = json.loads(line)
                    key = str(record.get(key_column,total))

                    retry = 0

                    while retry < MAX_RETRIES:

                        try:
                            self.producer.produce(
                                topic=topic,
                                key=key,
                                value=json.dumps(record),
                                callback=delivery_report
                            )
                            break
                        except BufferError:
                            self.producer.poll(1)
                            retry += 1

                    total += 1
                    self.producer.poll(0)

                    if total % BATCH_SIZE == 0:
                        self.producer.flush()
                        time.sleep(SEND_DELAY)

                except Exception as e:
                    failed += 1
                    logger.exception(e)

        self.producer.flush()

        elapsed = time.time()-start

        logger.info("="*60)
        logger.info(f"Topic      : {topic}")
        logger.info(f"Published  : {total}")
        logger.info(f"Failed     : {failed}")
        logger.info(f"Duration   : {elapsed:.2f} sec")
        logger.info(f"Throughput : {total/max(elapsed,1):.2f} msg/sec")
        logger.info("="*60)

    def close(self):
        self.producer.flush()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Publish All Datasets

# COMMAND ----------

FILES = {
    "customers": DATA_FOLDER / "customers.jsonl",
    "products": DATA_FOLDER / "products.jsonl",
    "orders": DATA_FOLDER / "orders.jsonl",
    "payments": DATA_FOLDER / "payments.jsonl",
    "shipping": DATA_FOLDER / "shipping.jsonl"
}

service = KafkaProducerService(producer)

for topic,file_path in FILES.items():
    service.publish_jsonl(file_path,topic)

service.close()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Metrics

# COMMAND ----------

print("="*60)
print("RetailSync Kafka Producer Summary")
print("="*60)

for topic,path in FILES.items():
    print(f"{topic:<12} : {path.name}")

print("-"*60)
print(f"Successful Deliveries : {delivery_stats['success']}")
print(f"Failed Deliveries     : {delivery_stats['failed']}")
print("="*60)
print("Streaming Completed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Output
# MAGIC
# MAGIC - Connects securely to Confluent Cloud
# MAGIC - Publishes all JSONL datasets
# MAGIC - Uses message keys for partitioning
# MAGIC - Sends data in batches
# MAGIC - Prints delivery callbacks
# MAGIC - Prints summary metrics
# MAGIC
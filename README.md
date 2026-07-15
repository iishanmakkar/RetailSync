# RetailSync — E‑Commerce Streaming Data Engineering Demo

RetailSync is a compact, end-to-end data engineering reference project that demonstrates
a realistic streaming pipeline for e‑commerce data: synthetic data generation, publishing
to Kafka (Confluent Cloud), Bronze/Silver/Gold ETL on Spark/Delta, and Databricks SQL
dashboards.

**Key ideas:** real-looking correlated data, injected data-quality issues (NULLs, duplicates,
invalid values, outliers, late arrivals, and schema evolution), and a reference ETL/BI stack.

## Features
- Synthetic data generator for Customers, Products, Orders, Payments, Shipping
- JSONL + CSV outputs in `data/`
- Kafka Producer to publish JSONL to Confluent Cloud topics
- PySpark notebooks for Bronze → Silver → Gold Delta layers and dashboard preparation
- Example Databricks SQL queries and dashboard guidance

## Repo Structure
- 01_ecommerce_data_generator_small.py — data generator (writes CSV/JSONL to `data/`)
- 02_Kafka_Producer_small.py — Confluent Cloud producer (reads JSONL → publishes to topics)
- 03_Kafka_Consumer_All_Topics_Bronze.py — PySpark consumer to write Bronze Delta tables
- 04_Silver_Layer.py — cleaning & Silver Delta tables
- 05_Gold_Layer.py — business KPIs & Gold tables
- 06_Databricks_SQL.py — prepare views and dashboards from Gold tables
- 07_Dashboard_and_Validation.py — (additional dashboard/validation helpers)
- data/ — sample outputs (CSV & JSONL)
- .env.example — example environment variables for Confluent Cloud

## Quickstart (local / Databricks)

1. Create a virtualenv and install minimal packages (local testing of generator & producer):

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\Activate.ps1 on Windows PowerShell
pip install --upgrade pip
pip install faker pandas numpy python-dotenv confluent-kafka tqdm
```

2. Generate synthetic data (writes CSV and JSONL to `data/`):

```bash
python 01_ecommerce_data_generator_small.py
```

Notes: The generator was authored as a Databricks notebook (contains notebook magics). It runs
as a plain Python script for the parts that are standard Python; if you hit `%pip` or display
magics, run it inside a Databricks notebook or remove the magics for local use.

3. Publish JSONL datasets to Confluent Cloud (set credentials first):

```bash
cp .env.example .env
# Edit .env and fill BOOTSTRAP_SERVER, API_KEY, API_SECRET
python 02_Kafka_Producer_small.py
```

4. Run the PySpark notebooks on Databricks to build Bronze → Silver → Gold tables and dashboards:

- Import `03_Kafka_Consumer_All_Topics_Bronze.py`, `04_Silver_Layer.py`, `05_Gold_Layer.py`, and
  `06_Databricks_SQL.py` into Databricks as notebooks (they include Databricks-specific code).
- Ensure your cluster has the Kafka connector and Delta support. Use Databricks secrets or
  environment variables for Confluent Cloud credentials.

## Environment variables
Set the following (see `.env.example`):
- BOOTSTRAP_SERVER
- API_KEY
- API_SECRET

## Data outputs
- CSVs: `data/customers.csv`, `data/products.csv`, `data/orders.csv`, `data/payments.csv`, `data/shipping.csv`
- JSONL: `data/*.jsonl` (used by the Kafka producer)

## Notes & Tips
- The generator intentionally injects data-quality problems so that downstream cleaning
  (Silver layer) and validation logic in Gold layer / dashboards can be exercised.
- The PySpark notebooks target Databricks; if running on plain Spark, adapt checkpoint paths
  and storage locations and ensure the Kafka connector is available.
- The Kafka producer uses message keys for partitioning and supports retries and batching.

## Next steps / ideas
- Add CI tests to validate generated schema and data-quality metrics
- Add a small Airflow or Prefect DAG to orchestrate generation → publish → ETL
- Provide a minimal Docker Compose local Kafka + Schema Registry for end-to-end local runs

## License
MIT-style demo code — reuse as you like for learning and demos.

Enjoy exploring the pipeline! If you want, I can:
- Add a `requirements.txt` and a small `run_local.sh` helper
- Convert notebooks to runnable scripts for non-Databricks environments

## Documentation images
The `docs/` folder contains the architecture diagram, a Kafka topics screenshot, and a producer summary screenshot.

- Architecture diagram: [RetailSync/docs/architecture.png](docs/architecture.png)
- Kafka topics dashboard: [RetailSync/docs/kafka_topics.png](docs/kafka_topics.png)
- Kafka producer summary: [RetailSync/docs/kafka_producer_summary.png](docs/kafka_producer_summary.png)



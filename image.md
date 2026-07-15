# Image prompts for architecture.png and workflow.png

Below are two ready-to-use prompts you can paste into your image generator to create `architecture.png` and `workflow.png`. They include Confluent Cloud for Kafka and reference the project scripts.

## architecture.png

Prompt:

"Create a clean, flat infographic-style architecture diagram (PNG 3000×1800, transparent background) showing an end-to-end e‑commerce streaming pipeline. Left side: a box labeled `Data Generator` (script: 01_ecommerce_data_generator_small.py) producing `CSV/JSONL` files (file icons). Arrow to `Kafka Producer` (script: 02_Kafka_Producer_small.py) which pushes to Confluent Cloud Kafka — draw a cloud labeled 'Confluent Cloud (Kafka)' with a simple cloud icon and topic list. Inside the cloud show topics as small labeled chips: `customers`, `products`, `orders`, `payments`, `shipping`. From the cloud, arrows go to `Databricks / PySpark Consumers` (script: 03_Kafka_Consumer_All_Topics_Bronze.py) labeled 'Structured Streaming → Bronze (Delta)'. Arrow to `Silver Layer (cleaning)` (script: 04_Silver_Layer.py) and then to `Gold Layer (aggregations)` (script: 05_Gold_Layer.py). From Gold, arrow to `Databricks SQL / Dashboards` (script: 06_Databricks_SQL.py) with a dashboard icon. Place a Delta Lake storage icon next to Bronze/Silver/Gold with small checkpoint icons. Annotate key features near components: at Producer show 'message keys · idempotence · batching · retries'; near Generator/Orders show 'schema evolution · late arrivals · duplicates · referential issues'; near Consumers show 'checkpoints · streaming state'. Use a muted tech palette (blue / teal / orange), modern sans font, labelled arrows, and simple device-style icons. Keep layout balanced, readable, and suitable for documentation. Footer note: 'Local dev: run_local.ps1 · requirements.txt'."

## workflow.png

Prompt:

"Create a numbered workflow flowchart (PNG 3000×1800, transparent background) showing the pipeline run sequence and data-quality checks. Use 7 horizontal steps with prominent numbered badges and directional arrows:

1) `Generate` — `01_ecommerce_data_generator_small.py` → produces `data/*.csv` and `data/*.jsonl` (icon: script + files).
2) `Publish` — `02_Kafka_Producer_small.py` → publish to Confluent Cloud Kafka topics (`customers`, `products`, `orders`, `payments`, `shipping`) (icon: cloud labeled 'Confluent Cloud (Kafka)') — annotate 'SASL_SSL auth, partitioning by message key'.
3) `Ingest (Bronze)` — `03_Kafka_Consumer_All_Topics_Bronze.py` → streaming Bronze Delta tables (icon: streaming arrow → database) — annotate 'checkpointing'.
4) `Clean (Silver)` — `04_Silver_Layer.py` → dedupe, trim, drop all-null rows, normalize strings (icon: filter/broom).
5) `Aggregate (Gold)` — `05_Gold_Layer.py` → KPIs: daily/monthly revenue, CLV, top products (icon: bar/line chart).
6) `SQL & Dashboards` — `06_Databricks_SQL.py` → register views and build Databricks SQL dashboards (icon: dashboard).
7) `Validation & Monitoring` — data quality report and checks (nulls, duplicates, outliers, late arrivals, referential integrity) with alerts (icon: shield/alert).

Add small warning badges near the Orders step to call out 'late arrivals' and 'referential integrity issues'. Use color-coded bands for phases: Ingest (blue), Storage (teal), Transform (orange), Serve (green). Include side annotations: 'Producer: batching · retries · idempotence' and 'Confluent Cloud: secure SASL_SSL connection'. Style: modern flat icons, clear typography, labelled elements, and high-contrast arrows."

---

If you want these adapted for a specific generator (Midjourney, DALL·E, Stable Diffusion, Leonardo, etc.), tell me which and I will produce generator-optimized variants.

# End-to-End Enterprise Retail Logistics & Supply Chain Pipeline

An enterprise-scale data engineering pipeline architected to ingest, clean, mask, model, and visualize a multi-table, distributed Brazilian e-commerce ecosystem (100,000+ transactional records across 9 distinct relational domains). This project transforms messy operational source data into high-performance, business-critical insights using a **Medallion Architecture** running on **Azure Databricks (Serverless Spark Compute)** and **Power BI (Hybrid Composite Storage Model)**.

## 📊 Core Executive Metrics Delivered
* **Average Fulfillment Latency:** 12.41 Days
* **On-Time Delivery Rate:** 90.00%
* **Target Persona:** VP of Logistics & Supply Chain Operations & Director of E-Commerce Growth

---

## 🏗️ System Architecture & Data Flow

* **Raw CSV Sources:** Multi-table operational data inputs staged for ingestion.
* **Bronze Layer (Robust Batch Ingestion + Metadata Auditing):** Raw ingestion tables managed within Unity Catalog. Enforces audit tracing with `_input_file_name` and `_processed_timestamp`.
* **Silver Layer (Data Cleansing, Deduplication, Cryptographic Masking):** Type casting and schema enforcement. Applies PII masking using the SHA-256 algorithm on `customer_id`. Handles string hygiene (trimming) and validation gates. Implements a spatial aggregation pattern for zip-code center points.
* **Gold Layer (Dimensional Modeling & Performance Engineering):** Optimized Star Schema (`fact_sales_fulfillment`). Builds a dedicated pre-aggregated summary table for time-series analytics.
* **Power BI Canvas (Hybrid Composite Storage Model):** Connects the geolocation dimension via IMPORT mode (cached locally in RAM) and the transactional fact table via DIRECTQUERY (leveraging serverless Spark). Eliminates visual rendering bottlenecks, cutting latency from 41 seconds down to under 500 milliseconds.

---

## 🛠️ Detailed Project Lifecycle & Engineering Triage

### 1. Conceptual Design & Security Constraints
* **The Blueprint:** Before writing any code, the entire 9-table entity-relationship diagram was mapped manually to evaluate data credibility, primary/foreign key mappings, and cross-table grain sizes.
* **The Cloud Security Roadblock:** The initial architecture planned to mount Azure Data Lake Storage (ADLS Gen2) directly to Databricks using secrets securely wrapped in an **Azure Key Vault** backed secret scope to avoid exposing raw credentials on GitHub. Due to enterprise tenant organizational restrictions, cluster-level compute creation privileges were blocked, rendering standard legacy configuration mounts useless in a Serverless Databricks environment.
* **The Remediation:** Pivot to **Unity Catalog Managed Tables**. Data was securely staged and loaded directly into the Unity Catalog infrastructure, ensuring total access control, security compliance, and data lineage without requiring raw infrastructure-level access keys.

---

### 2. Phase 1: The Bronze Layer & The Autoloader Crisis
* **The Problem:** Initial ingestion attempted to deploy Spark Structured Streaming via **Delta Autoloader**. The client environment threw recurring `RETRIES_EXCEEDED` exceptions, crashing at exactly 3 minutes. Forcing execution using `.start()` paired with `.awaitTermination()` yielded the same 180-second timeout signature. Accompanying `TABLE_OR_VIEW_NOT_FOUND` errors confirmed that the Spark driver was dropping connections and terminating right before committing metadata transaction logs to disk.
* **The Solution:** Implemented a highly structured, scalable **Static Batch Ingestion Pattern**. Separated logic into isolated, modular notebooks for each Medallion tier. To ensure strict data lineage and auditability, all ingested files were injected with system metadata tracking parameters before being written out to Delta tables:
    ```python
    df = (raw_df
          .withColumn("_input_file_name", F.col("_metadata.file_path"))
          .withColumn("_processed_timestamp", F.current_timestamp()))
    ```

---

### 3. Phase 2: The Silver Layer & Data Integrity Gates
The Silver tier enforces data cleaning, formatting, string normalization, and compliance masking.
* **Temporal Validation Gate:** Enforced an operational timeline checker (`is_valid_timeline`) via Spark conditional logic to flag or filter out records where the delivery timestamp mistakenly predated the purchase timestamp:
    ```python
    F.when(F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp"), True).otherwise(False)
    ```
* **Anonymization & Security:** Even though the source dataset was heavily obfuscated, to replicate production PII (Personally Identifiable Information) security standards, a cryptographic masking pipeline was applied. The `customer_id` column was passed through a **SHA-256 hashing algorithm** to guarantee absolute data anonymity throughout downstream storage layers:
    ```python
    F.sha2(F.col("customer_id"), 256).alias("secured_customer_id")
    ```
* **Data Hygiene Rules:** Executed systematic primary key validation audits, dropped duplicate transactional records resulting from operational infrastructure retries, and applied `F.trim()` to wipe out trailing and leading whitespace bugs embedded within category text fields.

---

### 4. Phase 3: The Geolocation Granularity Trap
* **The Problem:** The `olist_geolocation_dataset.csv` source table was tracking data at an ultra-dense, micro-granular street coordinate level, resulting in millions of redundant rows for identical municipal areas. Executing a raw string join directly against our sales fact table would have caused data explosion, heavily inflating storage footprints and crashing analytical query performance. Dropping data using simple `.dropDuplicates()` routines was unacceptable, as it would arbitrarily toss away vital coordinate variance.
* **The Solution (Spatial Aggregation Pattern):** Developed a geographic compression pattern inside the Silver layer using PySpark aggregation. The table grain was shifted from individual street locations down to unique **Zip Code Prefixes**. The numerical coordinates were physically compressed using mathematical averages to find the exact regional center point of each zip code, while text attributes were captured cleanly using a deterministic extraction filter:
    ```python
    silver_geo_df = (bronze_geo
        .filter(F.col("geolocation_zip_code_prefix").isNotNull())
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            F.avg("geolocation_lat").alias("latitude"),
            F.avg("geolocation_lng").alias("longitude"),
            F.first(F.trim(F.col("geolocation_city"))).alias("city"),
            F.first(F.trim(F.col("geolocation_state"))).alias("state")
        ))
    ```

---

### 5. Phase 4: The DirectQuery Performance Emergency
* **The Crisis:** To ensure real-time compute delegation to Databricks, the Gold layer fact table was connected to Power BI using **DirectQuery**. However, when adding cross-filtering slicers, maps, donut charts, and time-series line trends to the canvas, the report completely froze. The Power BI Performance Analyzer revealed a catastrophic latency bottleneck: **the time-series line chart took 41,534 milliseconds (41.5+ seconds) to render a single visual.**
* **The Root Cause:** Power BI was generating massively complex, concurrent SQL query streams behind the scenes, hammering the Databricks cluster. Because the dashboard was executing massive, transactional cross-joins across millions of rows over the network just to plot single points on a timeline, the environment experienced extreme network and memory exhaustion.
* **The Solution (The Hybrid Architecture Shift):** The entire application data model was torn down and re-engineered to follow a **Hybrid Composite Star Schema Model**:
    1.  **Storage Mode Decoupling:** The static `silver_geolocation` dimension table was shifted into **Import Mode** (caching its rows directly into local Power BI memory). The massive transaction table remained on **DirectQuery**. They were linked via a clean **1-to-Many (1:*) relationship**. This allowed Power BI to process geographic slicing natively on the client device, sending clean, pre-filtered requests over the wire.
    2.  **Gold Pre-Aggregation Tables:** To protect the line chart, a dedicated summary table (`gold_daily_logistics_summary`) was built inside Databricks. Instead of checking individual transactions, this table pre-calculates daily operational KPIs at runtime.
    3.  **Data Warehouse Compaction:** Enforced Delta optimization commands on the Databricks engine to physically rearrange cloud files into dense, sequential indexing blocks:
        ```sql
        OPTIMIZE db_retail_analytics.supply_chain_project.fact_sales_fulfillment 
        ZORDER BY (customer_id, order_status);
        ```
* **The Result:** Re-engineering the architecture completely eliminated dashboard freezing. DirectQuery rendering speeds dropped from a broken **41,534ms down to under 500ms**, yielding a blistering fast, highly interactive user experience while drastically minimizing cloud compute billing overhead.

---

## 🎨 Dashboard Design Standards & Visual Insights

The final dashboard balances high-level metrics with granular root-cause analysis, formatted to true corporate reporting standards:
* **Executive Metrics Panel:** Positioned at the absolute top-left reading anchor, custom DAX metrics display the **12.41 Days Average Fulfillment Latency** and the **90% On-Time Delivery Rate**, explicitly avoiding messy database names by leveraging clean Category Title overrides.
* **Geographic Risk Heatmap:** Utilizing a standard bubble Map visual connected via local coordinates, bubbles are bound to the `Average of fulfillment_latency_days`. It uses a **Divergent Gradient Rule (Conditional Formatting)** mapping low latency to Green, moderate delays to Orange, and high-risk operational bottlenecks to Deep Red.
* **Financial Pipeline Analysis:** Swapped a redundant order volume donut chart for an industry-standard **Stacked Bar Financial Chart**. It charts `order_status` on the axis against the `SUM(price)`. This allows stakeholders to instantly view exactly how much capital volume is frozen in transit bottlenecks.
* **Time-Series Tracking:** A highly optimized line chart pulling dates strictly as continuous variables from the `gold_daily_logistics_summary` table tracks macro delivery performance variations without generating table-scanning lags.

---

## 🚀 How to Run this Project

### Prerequisites
* An active Azure Tenant with Databricks workspace access (Unity Catalog enabled).
* Power BI Desktop.

### Step 1: Databricks Execution
1. Open your Databricks workspace and run the `01_bronze_ingestion` notebook to build your managed tables.
2. Execute `02_silver_cleaning` to apply data normalization, spatial transformations, and PII anonymization.
3. Execute `03_gold_modeling` to generate your star-schema fact tables and optimized pre-aggregation logs.

### Step 2: Power BI Setup
1. Open Power BI Desktop $\rightarrow$ Click **Get Data** $\rightarrow$ **Azure Databricks**.
2. Connect your `fact_sales_fulfillment` and `gold_daily_logistics_summary` tables using **DirectQuery**.
3. Connect your `silver_geolocation` table using **Import Mode**.
4. Navigate to **Model View**, build a `1:*` relationship link using the zip code keys, and hit **Refresh**.
# End-to-End Enterprise Commerce, Logistics & Financial Data Platform

An enterprise-scale data platform engineered to ingest, clean, mask, model, and visualize a multi-table, distributed Brazilian e-commerce ecosystem (100,000+ transactional records across 9 distinct relational domains). This platform transforms fragmented operational source data into high-performance, business-critical insights using a unified **Medallion Architecture** running on **Azure Databricks (Serverless Spark Compute & Unity Catalog)** and **Power BI (Hybrid Composite Storage Model)**.

## 📊 Core Executive Platform Metrics
* **Average Fulfillment Latency:** 12.41 Days
* **On-Time Delivery Rate:** 90.00%
* **Target Persona:** VP of Supply Chain Operations, Director of E-Commerce Growth, and Chief Financial Officer (CFO)

---

## 🏗️ System Architecture & Data Flow
* **Bronze Layer:** Raw ingestion tables managed within Unity Catalog. Enforces strict audit tracing with system metadata injectors.
* **Silver Layer:** Type casting, schema enforcement, and data hygiene rules. Applies PII masking using the SHA-256 algorithm on sensitive strings. Implements a spatial compression pattern for coordinate center-points.
* **Gold Layer:** Optimized Star Schema (`fact_sales_fulfillment`), paired with domain-specific data marts engineered with Liquid Clustering and window functions to eliminate query-scanning bottlenecks.
* **Power BI Canvas:** Connects massive transactional tables via DIRECTQUERY and caches high-yield analytics tables and dimensions via IMPORT mode. **Eliminates visual rendering bottlenecks, cutting latency from 41 seconds down to under 500 milliseconds.**

---

## 🛠️ Deep-Dive Engineering Triage & Architecture Pivots

### 1. Security Infrastructure & Unity Catalog Migration
* **The Roadblock:** The initial blueprint planned to mount Azure Data Lake Storage (ADLS Gen2) directly to Databricks using secrets wrapped in an Azure Key Vault-backed secret scope. However, enterprise tenant organizational restrictions blocked cluster-level compute creation privileges, rendering standard legacy configuration mounts non-functional in a Serverless Databricks environment.
* **The Remediation:** Shifted permanently to **Unity Catalog Managed Tables**. Data was securely staged and loaded directly into governed schemas within Unity Catalog, ensuring granular access control, automated data lineage, and enterprise security compliance without exposing raw infrastructure connection strings on GitHub.

### 2. Ingestion Bottlenecks & The Autoloader Crisis
* **The Problem:** Initial ingestion attempted to deploy Spark Structured Streaming via Delta Autoloader. The environment threw recurring `RETRIES_EXCEEDED` exceptions, crashing at exactly 180 seconds. Accompanying `TABLE_OR_VIEW_NOT_FOUND` errors confirmed that the Spark driver was dropping connections and terminating before committing metadata transaction logs to disk.
* **The Solution:** Implemented a highly structured, scalable **Static Batch Ingestion Pattern**. Separated logic into isolated notebooks for each Medallion tier. To ensure strict data lineage and auditability, all ingested files were injected with system metadata tracking parameters before being written out to Delta tables:
    ```python
    df = (raw_df
          .withColumn("_input_file_name", F.col("_metadata.file_path"))
          .withColumn("_processed_timestamp", F.current_timestamp()))
    ```

### 3. Geolocation Inflation & Spatial Aggregation Pattern
* **The Problem:** The raw geolocation source table tracked data at an ultra-dense, micro-granular street coordinate level, resulting in millions of redundant rows for identical municipal areas. Executing a raw string join directly against our sales fact table would have caused a massive data explosion, heavily inflating storage footprints and crashing query performance. Dropping data using simple `.dropDuplicates()` routines was unacceptable, as it would arbitrarily toss away vital coordinate variance.
* **The Solution:** Developed a geographic compression pattern inside the Silver layer using PySpark aggregation. The table grain was shifted from individual street locations down to unique Zip Code Prefixes. Numerical coordinates were physically compressed using mathematical averages to find the exact regional center point of each zip code, while text attributes were captured cleanly using a deterministic extraction filter:
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

### 4. DirectQuery Performance Emergency & Hybrid Composite Star Schema
* **The Crisis:** To ensure real-time compute delegation to Databricks, the Gold layer fact table was connected to Power BI using DirectQuery. However, when adding cross-filtering slicers, maps, donut charts, and time-series line trends to the canvas, the report completely froze. The Power BI Performance Analyzer revealed a catastrophic latency bottleneck: **the time-series line chart took 41,534 milliseconds (41.5+ seconds) to render a single visual.**
* **The Root Cause:** Power BI was generating complex, concurrent SQL query streams behind the scenes, hammering the Databricks cluster. Because the dashboard was executing massive, transactional cross-joins across millions of rows over the network just to plot single points on a timeline, the environment experienced extreme network and memory exhaustion.
* **The Solution:** The entire application data model was torn down and re-engineered to follow a **Hybrid Composite Star Schema Model**:
    1. **Storage Mode Decoupling:** Static dimensions and domain marts sitting well under the 100k row threshold were shifted into **Import Mode** (caching rows directly into local Power BI memory). The massive transaction table remained on **DirectQuery**. They were linked via clean `1:M` relationships. This allowed Power BI to process slicing and dicing metrics natively on the client device, sending clean, pre-filtered requests over the wire.
    2. **Gold Pre-Aggregation Tables:** To protect the time-series visuals, a dedicated summary table (`gold_daily_logistics_summary`) was built inside Databricks. Instead of checking individual transactions, this table pre-calculates daily operational KPIs at runtime.
    3. **Data Warehouse Compaction:** Enforced Delta optimization commands on the Databricks engine to physically rearrange cloud files into dense, sequential indexing blocks:
        ```sql
        OPTIMIZE main.gold.fact_sales_fulfillment 
        ZORDER BY (customer_id, order_status);
        ```
* **The Result:** Re-engineering the architecture completely eliminated dashboard freezing. DirectQuery rendering speeds dropped from a broken **41,534ms down to under 500ms**, yielding a blistering fast, highly interactive user experience while drastically minimizing cloud compute billing overhead.

---

## 🚀 Advanced Analytics Domains Deployed (Expansion Phases)

### Phase 1: Seller & Product Performance
* **Data Engineering:** Engineered two specialized Gold marts in Unity Catalog. `main.gold.gold_seller_performance` consolidates sales volumes and total revenue yield per vendor (optimized using Liquid Clustering `CLUSTER BY seller_id`). `main.gold.gold_product_category_ranking` leverages Spark SQL window functions (`DENSE_RANK()`) to dynamically rank categories by financial contribution.
* **Visual Insight Strategy:** Deployed a **Strategic Scatter Plot** cross-referencing Category Revenue vs. Average Rating. This isolates "The Risk Zone" (High Revenue / Low Customer Satisfaction) so operations can step in before customer churn occurs.

### Phase 2: Customer Financial Domain
* **Data Engineering:** Built `main.gold.gold_customer_installment_correlation` to parse consumer financing behaviors. It groups order values into strict, logical purchasing thresholds (e.g., $0–$50, $51–$100, $101–$250, etc.) via SQL conditional logic to calculate installment choices on credit cards.
* **Visual Insight Strategy:** Deployed a dual-axis **Clustered Column & Line Chart**. It tracks average installments alongside total order volumes against price buckets, pinpointing the exact financial inflection thresholds where customers switch from single-payment to high-installment financing.

### Phase 3: Geographic Revenue Targeting
* **Data Engineering:** Built `main.gold.gold_geographic_revenue_density` to pinpoint high-yield customer hubs. It aggregates revenue, unique client counts, and spending habits per state and city.
* **Visual Insight Strategy:** Designed a non-cluttered **Treemap + Leaderboard Combo**. Large blocks represent state-level revenue. Selecting a state block instantly cross-filters a clean **Horizontal Bar Chart** showing the top 10 revenue-generating city hubs within that state, directing marketing teams where to focus localized ad spend.

### Phase 4: Sentiment & Root-Cause Analytics
* **Data Engineering:** Built `main.gold.gold_sentiment_root_cause`, joining customer text review ratings directly to operational logistics delay flags (e.g., actual late delivery timestamps and fulfillment latency metrics exceeding the 12.41-day baseline).
* **Visual Insight Strategy:** Implemented a **100% Stacked Bar Chart** tracking review scores (1 to 5 Stars) split against a binary `is_late_delivery` flag. This breaks down the blame game between logistics and product quality by visualizing exactly what percentage of 1-Star reviews are tied to late shipments vs. on-time operational deliveries.

---

## 🎨 Clean Dashboard Design & Governance Standards

To prevent visual fatigue, the reporting layer adheres to strict corporate layout guidelines:
* **No Clutter Principle:** Dense transaction details, specific IDs, and granular metrics are completely stripped from the main view and hidden inside **Report Page Tooltips**, appearing only when hovering over data points.
* **The Reading Anchor:** High-level overview KPI cards use clean Title Overrides (no raw database names) and sit strictly at the top banner of each page to anchor user focus.
* **Functional Color Usage:** Avoids decorative coloring. Colors are reserved for conditional formatting rules, such as a divergent green-to-red gradient reflecting operational risk levels.

---

## 🚀 How to Run this Project

### Prerequisites
* An active Azure Tenant with Databricks workspace access (Unity Catalog enabled).
* Power BI Desktop.
* Configured Databricks Git Folder connected to your feature/development branch.

### Step 1: Databricks Pipeline Execution
1. Run the `01_bronze_ingestion` notebook to build your raw audited structures inside Unity Catalog.
2. Execute `02_silver_cleaning` to apply data normalization, timeline validation gates, cryptographic masking, and spatial transformations.
3. Execute `03_gold_modeling` to generate your core star-schema fact tables and advanced analytical domain marts.

### Step 2: Power BI Connection & Modeling
1. Open Power BI Desktop $\rightarrow$ Click **Get Data** $\rightarrow$ **Azure Databricks**.
2. Connect `fact_sales_fulfillment` and `gold_daily_logistics_summary` using **DirectQuery** mode.
3. Connect all other dimension tables and domain data marts (`gold_seller_performance`, `gold_customer_installment_correlation`, etc.) using **Import** mode.
4. Navigate to **Model View**, establish your `1:M` relationships between the fact table and dimensions, and click **Refresh**.
# 📐 Design Decisions – StatsBomb Football Data Pipeline

This document explains the architectural and engineering decisions behind the StatsBomb pipeline.  
The goal was not only to build a working system, but to design a scalable, maintainable, and production-oriented data platform.

---

## 1️⃣ Why ELT Instead of ETL?

### Decision:

Adopt an **ELT (Extract → Load → Transform)** pattern instead of traditional ETL.

### Rationale:

- Raw JSON is stored unchanged in S3.
- Transformations occur after loading into the warehouse layer.
- Snowflake compute is separated from storage, enabling scalable transformation.

### Benefits:

- Preserves raw data for replayability.
- Supports schema evolution.
- Enables reprocessing without re-ingestion.
- Improves debuggability.

Traditional ETL would tightly couple transformation logic with ingestion, reducing flexibility.

---

## 2️⃣ Why rclone for Ingestion?

Instead of:

- AWS CLI
- Custom Python scripts
- Airbyte

We selected **rclone** because:

- Native multi-threaded transfers (parallel upload)
- Checksum-based change detection
- Resumable sync
- High reliability for deep directory trees
- Zero additional infrastructure required

This was critical given:

- ~90GB of nested JSON files
- Tens of thousands of small files

---

## 3️⃣ Why Store Raw Data in S3?

Amazon S3 acts as the **data lake landing zone**.

Reasons:

- Cheap and durable object storage
- Native integration with Glue & Snowflake
- Separation of storage from compute
- Supports raw → processed → curated layering

Layering strategy:

```

s3://bucket/statsbomb-raw/ (Raw Zone)
s3://bucket/warehouse/production/ (Processed Parquet)

```

This mirrors modern data lake architecture patterns.

---

## 4️⃣ Why AWS Glue (PySpark)?

The dataset:

- Deeply nested JSON
- Multi-level arrays
- Polymorphic event structures

Local Pandas processing would:

- Fail at scale
- Be memory-bound
- Lack distributed compute

Glue provides:

- Distributed Spark execution
- Serverless job management
- Direct S3 integration
- Scalable JSON flattening

Spark enables:

- Exploding nested arrays
- UDF-based subtype prioritization
- Efficient Parquet writing

---

## 5️⃣ Why Parquet Instead of JSON?

Raw JSON is inefficient for analytics.

Parquet advantages:

- Columnar storage
- Compression
- Predicate pushdown
- Faster warehouse loading
- Reduced Snowflake compute costs

This reduces both:

- Storage costs
- Query latency

---

## 6️⃣ Why Snowflake as the Warehouse?

Snowflake was selected because:

- Separation of storage and compute
- Automatic scaling
- External stage support
- Strong semi-structured data handling (VARIANT)
- Zero infrastructure maintenance

Alternative options considered:

- PostgreSQL (limited scaling)
- Redshift (cluster management overhead)
- BigQuery (vendor-specific trade-offs)

Snowflake allowed rapid development with minimal DevOps burden.

---

## 7️⃣ Why a Star Schema?

We adopted a Star Schema because:

- BI tools optimize for star joins
- Simplifies analytical queries
- Reduces duplication
- Improves query readability

Grain definition:

- FACT_EVENTS → atomic event level
- FACT_LINEUPS → match-player level

Dimensional modeling supports:

- Aggregation
- Drill-down
- Time-series analysis
- Tactical segmentation

---

## 8️⃣ Why a Unified FACT_EVENTS Table?

Alternative:

- Separate tables for Passes, Shots, Dribbles, etc.

We intentionally avoided this.

### Decision:

Use a **single sparse polymorphic fact table**.

### Why?

- Simplifies dbt modeling
- Avoids 20+ specialized tables
- Reduces cross-table joins
- Easier incremental loading

Tradeoff:

- Some NULL columns

But Snowflake compresses sparse columns efficiently, so storage cost is minimal.

This favors simplicity over hyper-normalization.

---

## 9️⃣ Why dbt for Analytics?

Transformation logic could have been:

- Raw SQL scripts
- Stored procedures

Instead we used **dbt** because:

- Modular modeling
- Built-in testing
- Dependency graph
- Documentation auto-generation
- Version control friendly

dbt enforces:

- Data contracts
- Reproducibility
- Clean separation of staging and marts

---

## 🔟 Why Dagster Over Airflow?

We experimented with both.

Dagster advantages:

- Python-native
- Clear dependency modeling
- Strong local dev experience
- Dagit UI for observability
- Simpler debugging

Airflow would require:

- More boilerplate
- External scheduler complexity
- Heavier infrastructure

Dagster aligned better with team skillset and development speed.

---

## 1️⃣1️⃣ Why K-Means for Tactical DNA?

We needed:

- Unsupervised grouping
- Interpretable clusters
- Fast computation
- Easy scaling

K-Means offers:

- Deterministic centroids
- Simple Euclidean similarity
- Clear tactical segmentation

Alternatives considered:

- DBSCAN (less interpretable for business users)
- Hierarchical clustering (harder to scale)
- PCA-only visualization (not actionable clustering)

K-Means balanced simplicity and value.

---

## 1️⃣2️⃣ Why StandardScaler Before Clustering?

Feature magnitudes vary significantly:

- Avg Passes (~2000)
- Shot Quality (~0.1)

Without scaling:

- Larger features dominate distance
- Clusters become biased

StandardScaler normalizes:

- Mean = 0
- Variance = 1

Ensures fair distance computation.

---

## 1️⃣3️⃣ Why Euclidean Distance for Manager Similarity?

We needed:

- Intuitive similarity metric
- Low compute cost
- Transparent logic

Euclidean distance:

- Simple
- Interpretable
- Efficient for moderate dataset size

More complex similarity measures were unnecessary for the use case.

---

## 1️⃣4️⃣ Trade-offs & Limitations

This system is:

- Batch-based (monthly)
- Not real-time
- Not fully incremental
- Dependent on Snowflake compute

Potential improvements:

- Partitioned incremental Glue jobs
- Change-data-capture ingestion
- Feature store abstraction
- CI/CD integration for dbt
- Cost monitoring automation

---

## 1️⃣5️⃣ Engineering Principles Applied

- Separation of storage and compute
- Layered architecture (Raw → Processed → Curated → Gold)
- Idempotent ingestion
- Reproducible modeling
- Security via least-privilege access
- Modularity over monolith scripts

---

## Conclusion

This pipeline was designed to reflect real-world modern data engineering architecture rather than a simple academic ETL exercise.

The focus was:

- Scalability
- Maintainability
- Analytical usability
- Business-facing value

It can serve as a foundation for:

- Advanced predictive modeling
- Scouting automation
- Tactical analytics platforms
- Football intelligence products

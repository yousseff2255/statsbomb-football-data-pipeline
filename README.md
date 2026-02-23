# ⚽ StatsBomb Football Data Pipeline

A fully automated, end-to-end big data pipeline that ingests raw nested football event data from StatsBomb Open Data, transforms it into a structured star-schema warehouse, builds analytical marts, and powers machine learning & BI applications.

---

## 📌 Project Overview

Modern football analytics requires scalable pipelines capable of processing large volumes of deeply nested JSON data.

The StatsBomb Open Dataset contains:

- 3,000+ matches
- Detailed event-level data (passes, shots, pressures, dribbles)
- Nested JSON structures across multiple folders

This project implements a **production-style ELT architecture** that:

1. Ingests raw data into AWS S3
2. Transforms and flattens nested JSON using AWS Glue (PySpark)
3. Loads structured data into Snowflake
4. Builds analytical marts using dbt
5. Orchestrates the pipeline using Dagster
6. Powers dashboards (Power BI) and a ML scouting engine

---

## 🏗 Architecture

```

StatsBomb GitHub
↓
rclone
↓
AWS S3 (Raw Zone)
↓
AWS Glue (PySpark)
↓
S3 (Parquet Production Zone)
↓
Snowflake (Warehouse)
↓
dbt (Analytics Layer)
↓
Power BI + ML + Streamlit

```

### Core Technologies

- rclone – High-performance ingestion
- AWS S3 – Scalable storage
- AWS Glue (PySpark) – Distributed transformation
- Snowflake – Cloud Data Warehouse
- dbt – Analytical modeling
- Dagster – Pipeline orchestration
- Power BI – Dashboarding
- Scikit-learn – ML clustering
- Streamlit – Scouting application

---

## 📂 Data Warehouse Design

### Star Schema

**Grain of Fact Tables:**

- Individual match events
- Lineup entries

### Dimensions

- DIM_COMPETITION
- DIM_SEASON
- DIM_TEAM
- DIM_PLAYER
- DIM_MATCH
- DIM_EVENT_TYPE
- DIM_STADIUM
- DIM_MANAGER
- DIM_REFEREE
- DIM_POSITION

### Fact Tables

- FACT_EVENTS (Atomic event stream)
- FACT_LINEUPS (Bridge table)

The FACT_EVENTS table uses a **sparse polymorphic schema** to unify multiple event types in a single structure.

---

## ⚙️ Data Processing (Glue – PySpark)

The transformation job:

- Reads nested JSON from S3
- Extracts and normalizes dimensions
- Explodes nested arrays (lineups, managers)
- Applies UDF logic for subtype prioritization
- Writes structured Parquet files back to S3

Key techniques:

- Array explosion
- Regex-based match_id extraction
- Sparse column handling
- Dynamic event subtype resolution

---

## 🧠 Analytical Modeling (dbt)

dbt models create analytics-ready marts:

### fct_player_match_stats

- Passes
- Progressive passes
- Goals
- xG
- Counterpress actions

### fct_team_match_stats

- Match results
- xG difference
- Possession %
- Pressing intensity

### fct_passing_network

- Passer → Recipient edges
- Node spatial positioning
- Pass count thresholds

### mart_player_season_summary

- Shot conversion %
- Pass completion %
- Goals - xG delta

Data quality is enforced using:

- not_null tests
- unique combination tests
- accepted range validations

---

## 🔄 Orchestration (Dagster)

The pipeline is fully automated:

1. Sync raw data to S3
2. Trigger Glue transformation job
3. Execute COPY INTO Snowflake
4. Run dbt models

Scheduled to run monthly:

```

0 3 1 \* \* (Africa/Cairo timezone)

```

Dagster provides:

- Observability
- Retry control
- Dependency chaining
- Run history

---

## 🤖 Tactical DNA Modeling (Machine Learning)

An unsupervised K-Means clustering model groups teams by playing style.

### Feature Engineering

- Avg Possession
- Passing Intensity
- Pressing Intensity
- Avg Shots
- Avg xG
- Shot Quality
- Team Width (Spatial stddev)

### Identified Tactical Archetypes

1. High-Possession Dominators
2. Counter-Attacking / Direct
3. Slow-Tempo Positional
4. Balanced

---

## 🔍 Manager Similarity Engine (Streamlit App)

The scouting app:

- Loads precomputed Gold-layer table
- Standardizes features
- Computes Euclidean distance
- Returns top 5 most tactically similar managers

Distance metric:

```

np.linalg.norm(target_vector - candidate_vector)

```

---

## 📊 Power BI Dashboard

Includes:

### 1️⃣ Season Overview

- Goals
- xG difference
- Possession trends
- Shot efficiency

### 2️⃣ Match Analysis

- Pass comparison
- Pressure comparison
- Player contributions

### 3️⃣ Passing Network

- Node centrality
- Edge frequency
- Tactical clusters

---

## 🔐 Security

- Least-privilege IAM roles
- Snowflake RBAC
- External Stages for secure S3 loading
- No credentials stored in repository

⚠️ All credentials must be supplied via environment variables.

---

## 🚀 How to Run (Local Dev Setup)

### 1️⃣ Create virtual environment

```bash
python -m venv venv
source venv/bin/activate
```

### 2️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 3️⃣ Run Dagster UI

```bash
dagster dev
```

Open:
[http://localhost:3000](http://localhost:3000)

---

## 📌 Future Improvements

- Incremental loading strategy
- Partition pruning optimization
- Real-time streaming ingestion
- Feature store integration
- CI/CD for dbt models

---

## 📚 Data Source

StatsBomb Open Data
[https://github.com/statsbomb/open-data](https://github.com/statsbomb/open-data)

---

## 📜 License

Educational / Research Purposes

# Real-Time Fintech Fraud Detection Pipeline (Delta Lakehouse)

## 📌 Project Overview
A high-performance, real-time data engineering pipeline designed to ingest, scrub, and analyze financial transactions for fraud patterns. Built with **PySpark**, **Kafka**, and **Delta Lake** using the Medallion Architecture.

## 🏗️ Technical Architecture (The 'Why')

### 1. Medallion Workflow
- **Bronze:** Raw landing zone capturing Kafka JSON streams. No data is filtered here to ensure re-playability.
- **Silver:** Scrubbed layer. Implemented **Salted SHA-256 Hashing** for PII (Customer ID, Card Numbers) to comply with GDPR/CCPA while maintaining join utility.
- **Gold:** Actionable layer. Uses **Broadcast Joins** with merchant metadata to prevent data shuffling, significantly reducing compute latency.

### 2. Engineering Decisions for Senior Defense
- **Idempotency:** Implemented `MERGE INTO` logic in the Gold layer's `foreachBatch` sink to handle 'at-least-once' Kafka delivery semantics without creating duplicate fraud alerts.
- **Performance:** Forced Broadcast Joins on static dimensions. In production, we monitoring the broadcast threshold to ensure it doesn't cross 2GB to avoid executor memory pressure.
- **Environment Agnostic:** The pipeline includes a polymorphic configuration that automatically adjusts paths and Spark settings between **Local Dev** and **Databricks Cloud**.

## 🚀 How to Run
### Local Dev (Windows/Linux)
1. Initialize virtual environment: `python -m venv venv`
2. Install dependencies: `pip install -r requirements.txt`
3. Run orchestrator: `python main.py`

### Databricks Deployment
1. Import this repository into **Databricks Repos**.
2. Run `main.py` on a cluster with Runtime 13.3+. 
3. *Note: Paths automatically switch to DBFS storage (dbfs:/fraud_pipeline) when cloud environment is detected.*

## 🛠️ Tech Stack
- **Engine:** PySpark (Structured Streaming)
- **Storage:** Delta Lake (ACID Transactions, Time Travel)
- **Security:** Hashing with cryptographic salts
- **DevOps:** Docker, Git-flow, Databricks Asset Bundles

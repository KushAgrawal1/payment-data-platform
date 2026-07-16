# Payment Data Platform

A production-grade data platform for processing payment streams, featuring automated testing, CI/CD pipelines, and robust data engineering patterns.

## 🚀 Pipeline Overview
- **Ingestion:** Kafka-based real-time ingestion.
- **Processing:** PySpark batch and stream processing using Medallion architecture.
- **Storage:** Data Lake implementation with optimized storage formats.
- **Orchestration:** Airflow DAGs for workflow management.

## 🛠 Tech Stack
- **Languages:** Python 3.11
- **Data Processing:** Apache Spark (PySpark)
- **Infrastructure:** Docker, Kafka, PostgreSQL
- **CI/CD:** GitHub Actions (Automated Linting & Testing)

## 🧪 Testing
This project uses `pytest` for unit and integration testing. Every push is validated automatically via GitHub Actions.
- To run tests locally: `pytest`
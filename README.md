# ultimate-data-engineering-project
The ultimate data engineering project that will cover data engineering concepts on the cloud; including batch processing, stream processing, simulating data streaming into database, medallion architecture, and many more.

## 📋 Phase 1: Foundation (Already Done ✅)

- [x]  OLTP data generation (customers, accounts, transactions)
- [x]  Airflow orchestration
- [x]  PostgreSQL database
- [x]  Realistic transaction patterns

![alt_text][https://github.com/jonuts100/ultimate-data-engineering-project/blob/main/Screenshot%20from%202025-10-10%2021-31-34.png?raw=true]
## Next Phases (2-5)
### Week 1: Streaming & Storage

- [ ]  **Set up Kafka/Debezium** for CDC from PostgreSQL
- [ ]  **Configure MinIO** (S3-compatible) for data lake
- [ ]  Stream changes to Bronze layer (raw data in Parquet)
- [ ]  Add Kafka monitoring with Kafka UI

**Skills Demonstrated:** Streaming, CDC, Data Lake, Event-driven architecture

### Week 2: Transformation Layer

- [ ]  **Install dbt** and create dimensional models
- [ ]  Build Silver layer (cleaned, deduplicated)
- [ ]  Build Gold layer (business metrics, aggregations)
- [ ]  Implement slowly changing dimensions (SCD Type 2)
- [ ]  Add data quality tests with dbt tests

**Skills Demonstrated:** Data modeling, dbt, Data warehouse design, SCD

### Week 3: Analytics & Visualization

- [ ]  **Deploy Metabase/Superset** for dashboards
- [ ]  Create 5-7 key dashboards:
    - Daily transaction volumes
    - Customer acquisition trends
    - Account balance distribution
    - Fraud detection alerts
    - Data quality metrics
- [ ]  Add basic ML model (fraud detection with scikit-learn)

**Skills Demonstrated:** BI tools, SQL analytics, Basic ML

### Week 4: Production Features

- [ ]  **Dockerize everything** (docker-compose)
- [ ]  Add Prometheus + Grafana for monitoring
- [ ]  Implement data lineage with OpenLineage
- [ ]  Write comprehensive README with architecture diagram
- [ ]  Deploy to cloud (AWS/GCP free tier)

**Skills Demonstrated:** DevOps, Monitoring, Documentation, Cloud deployment

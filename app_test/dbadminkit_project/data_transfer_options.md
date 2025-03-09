# Data Transfer Options for Databricks and Postgres in `dbadminkit.core.admin_operations.py`

This document summarizes recommended methods for moving data between Databricks and Postgres in the `dbadminkit` setup, specifically within the `AdminDBOps` class located in `dbadminkit.core.admin_operations.py`. It covers both **bulk transfers** (full 20M record loads) and **incremental loads** (SCD Type 2 syncs with daily batches). Each method includes a brief description, estimated speed, use case recommendation, and specific requirements beyond the existing `AdminDBOps`, `SparkEngine`, and `DBEngine` framework. Assumptions: Databricks uses Change Data Feed (CDF), and Postgres maintains an SCD Type 2 table (`emp_id`, `name`, `salary`, `is_active`, `on_date`, `off_date`).

---

## Bulk Transfer Options (Full 20M Records)

For initial loads or full table overwrites:

### 1. JDBC (Databricks → Postgres)
- **Summary**: Fastest and simplest bulk transfer, uses Spark-to-JDBC to write directly from Databricks to Postgres.
- **Speed**: ~5-10 minutes for 20M records.
- **Recommended When**: You need a one-time full load or schema overwrite with minimal setup.
- **Requirements**:
  - JDBC driver (`org.postgresql:postgresql:42.2.23`) installed on Databricks cluster.
  - Direct network connectivity between Databricks and Postgres.

### 2. CSV Export + COPY (Databricks → Postgres)
- **Summary**: Exports Databricks table to CSV (e.g., S3), then uses Postgres `COPY` for fast bulk loading.
- **Speed**: ~5-10 minutes for 20M records.
- **Recommended When**: You prefer file-based transfers or lack direct DB connectivity.
- **Requirements**:
  - S3 bucket (or equivalent storage) accessible by Databricks and Postgres host.
  - Ability to stage CSVs locally or mount S3 on Postgres host.
  - Postgres `COPY` command support (standard).

### 3. Spark Streaming (Databricks → Postgres)
- **Summary**: Streams full table in micro-batches from Databricks to Postgres via JDBC.
- **Speed**: ~20-30 minutes for 20M records.
- **Recommended When**: You want a streaming approach for full loads with incremental potential.
- **Requirements**:
  - JDBC driver (`org.postgresql:postgresql:42.2.23`) on Databricks cluster.
  - Running Spark cluster with sufficient resources (your Databricks cluster suffices).
  - Direct network connectivity between Databricks and Postgres.

---

## Incremental Load Options (SCD Type 2 Syncs)

For daily batch ETL (e.g., 3x daily) syncing changes:

### 1. Spark Streaming with CDF (Databricks → Postgres)
- **Summary**: Streams CDF changes from Databricks to Postgres in micro-batches, applying inserts/updates/deletes.
- **Speed**: ~1-5 minutes per batch (e.g., 100K changes).
- **Recommended When**: You need precise incremental syncs from Databricks with CDF, fitting your batch ETL schedule.
- **Requirements**:
  - Databricks Delta table with CDF enabled (`ALTER TABLE employees SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`).
  - Running Spark cluster with `SparkEngine` (your Databricks cluster works).
  - Postgres `SCDType2` handler implemented for `create`/`delete`.

### 2. CSV Export + MERGE INTO (Postgres → Databricks)
- **Summary**: Exports Postgres changes (via `last_modified`) to CSV, merges into Databricks Delta table.
- **Speed**: ~5-10 minutes per batch (e.g., 100K changes).
- **Recommended When**: You need a simple, file-based incremental sync from Postgres to Databricks.
- **Requirements**:
  - Postgres table with `last_modified` column and trigger (e.g., `CREATE TRIGGER update_last_modified ...`).
  - S3 bucket or local storage for CSV staging.
  - Databricks Delta table support (`USING DELTA`).
  - Running Spark cluster (your Databricks cluster suffices).

### 3. Kafka with CDF (Databricks → Postgres)
- **Summary**: Writes CDF changes to Kafka, consumes and applies to Postgres for near real-time sync (conceptual).
- **Speed**: ~30-60 minutes for 20M initial, near real-time for increments.
- **Recommended When**: You want near real-time updates instead of batch ETL, with Kafka infrastructure available.
- **Requirements**:
  - Databricks Delta table with CDF enabled.
  - External Kafka cluster (brokers, e.g., `localhost:9092`) and topic (`employees_changes`)—Databricks cluster can’t host Kafka, only produce/consume.
  - `kafka-python` library installed.
  - Direct network connectivity to Postgres.

### 4. Kafka with Trigger (Postgres → Databricks)
- **Summary**: Logs Postgres changes to Kafka via trigger, merges into Databricks Delta (conceptual).
- **Speed**: ~30-60 minutes for 20M initial, near real-time for increments.
- **Recommended When**: You need near real-time sync from Postgres to Databricks with Kafka setup.
- **Requirements**:
  - Postgres change log table (e.g., `employees_changes`) and trigger (e.g., `log_to_kafka()`).
  - External Kafka cluster and topic (`employees_changes`)—Databricks cluster can’t host Kafka, only consume.
  - `kafka-python` library installed.
  - Databricks Delta table support.
  - Running Spark cluster (your Databricks cluster works).

### 5. General SCD Type 2 Sync (Bidirectional)
- **Summary**: Generic sync using timestamps, works both ways (Postgres ↔ Databricks).
- **Speed**: ~1-5 minutes per batch (e.g., 100K changes).
- **Recommended When**: You need a flexible, fallback option without CDF or Kafka, suitable for smaller batches.
- **Requirements**:
  - Postgres table with `on_date`/`off_date` for SCD Type 2.
  - Databricks Delta table (if target) or Postgres SCD handler (if source).
  - `SparkEngine` for Databricks target (your Databricks cluster suffices).

---

## Summary of Recommendations

- **Bulk (20M Full Load)**:
  - **Top Choice**: **JDBC** – Fastest (~5-10 min), simplest, direct DB-to-DB transfer.
    - **Function**: `transfer_with_jdbc`
    - **Direction**: Unidirectional (Databricks → Postgres).
  - **Alternative**: **CSV Export + COPY** – Matches JDBC speed, file-based, good for disconnected environments.
    - **Function**: `transfer_with_csv_copy`
    - **Direction**: Unidirectional (Databricks → Postgres).
  - **Fallback**: **Spark Streaming** – Slower (~20-30 min), but streaming-ready for future increments.
    - **Function**: `transfer_with_streaming`
    - **Direction**: Unidirectional (Databricks → Postgres).

- **Incremental (Daily SCD Type 2 Sync)**:
  - **Databricks → Postgres**: **Spark Streaming with CDF** – Precise, scalable, fits batch ETL (~1-5 min/batch).
    - **Function**: `sync_databricks_to_postgres_stream`
    - **Direction**: Unidirectional (Databricks → Postgres).
  - **Postgres → Databricks**: **CSV Export + MERGE INTO** – Simple, fast for batches (~5-10 min/batch), no middleware.
    - **Function**: `sync_postgres_to_databricks_csv`
    - **Direction**: Unidirectional (Postgres → Databricks).
  - **Advanced Option**: **Kafka (Bidirectional)** – Near real-time, but complex (requires separate Kafka cluster).
    - **Functions**: 
      - `sync_databricks_to_postgres_kafka` (Databricks → Postgres).
      - `sync_postgres_to_databricks_kafka` (Postgres → Databricks).
    - **Direction**: Bidirectional (separate functions for each direction).
  - **General Fallback**: **SCD Type 2 Sync** – Flexible, no extra dependencies, good for smaller datasets.
    - **Function**: `sync_scd2_versions`
    - **Direction**: Bidirectional (works both ways with appropriate `source_ops` and target setup).

---

## Requirements Recap

- **JDBC**: JDBC driver on Databricks, network connectivity.
- **CSV + COPY**: S3 bucket or local storage, Postgres `COPY` support.
- **Spark Streaming**: Running Spark cluster (Databricks cluster OK), JDBC driver (if used), network connectivity.
- **Kafka**: External Kafka cluster (Databricks cluster can’t host, only produce/consume), `kafka-python`, Postgres trigger (for Postgres → Databricks), CDF (for Databricks → Postgres).
- **General SCD Sync**: Postgres `on_date`/`off_date`, Databricks Delta (optional).

These options address your 20M record needs and daily ETL requirements efficiently, leveraging your Databricks cluster as a Spark compute resource where applicable.
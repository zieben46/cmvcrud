import logging


from typing import List, Dict, Any
from sqlalchemy.sql import select, text
from sqlalchemy.orm import Session
from dbadminkit.core.engine import DBEngine
from dbadminkit.core.engine import SparkEngine
from dbadminkit.models.postgres.table_models import DBTable as PostgresDBTable
from dbadminkit.models.databricks.table_models import DBTable as DatabricksDBTable
from app_test.dbadminkit.core.crud_types import CRUDOperation
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import datetime
import csv
from pathlib import Path
import pandas as pd
from kafka import KafkaConsumer  # Requires kafka-python
from delta_sharing import SharingClient  # Requires delta-sharing library

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, config: DatabaseProfile):
        try:
            self.config = config
            self.engine = DBEngine(config)
            self.db_type = config.mode
            self.spark = None
            if "databricks" in self.db_type.value.lower():
                from dbadminkit.core.spark_engine import SparkEngine
                self.spark = SparkEngine(config).get_spark()
        except Exception as e:
            logger.error(f"Failed to initialize DBManager: {e}")
            raise

    def get_table(self, table_info: Dict[str, Any]):
        if "databricks" in self.db_type.value.lower():
            return DatabricksDBTable(self.engine.engine, self.spark, table_info)
        return PostgresDBTable(self.engine.engine, table_info)

    # Core CRUD and Schema Methods
    def perform_crud(self, table_info: Dict[str, Any], crud_type: CRUDOperation, data: Dict[str, Any]) -> Any:
        table = self.get_table(table_info)
        return table.perform_crud(crud_type, data)

    def process_cdc_logs(self, table_info: Dict[str, Any], cdc_records: List[Dict[str, Any]]) -> int:
        table = self.get_table(table_info)
        return table.process_cdc_logs(cdc_records)

    def process_dataframe_edits(self, table_info: Dict[str, Any], original_df: pd.DataFrame, edited_df: pd.DataFrame) -> int:
        table = self.get_table(table_info)
        return table.process_dataframe_edits(original_df, edited_df)

    def get_schema(self, table_info: Dict[str, Any]) -> Dict[str, str]:
        table = self.get_table(table_info)
        return table.get_db_table_schema()

    # Sync Metadata Management
    def get_last_sync_timestamp(self, table_name: str) -> str:
        """Fetch the last sync timestamp from metadata."""
        with self.engine.begin() as session:
            result = session.execute(
                select(text("last_sync_timestamp")).select_from(text("sync_metadata"))
                .where(text("table_name = :table_name")),
                {"table_name": table_name}
            ).scalar()
            return result or "1970-01-01 00:00:00"  # Default to epoch if no sync yet

    def update_last_sync_timestamp(self, table_name: str, timestamp: str) -> None:
        """Update the last sync timestamp in metadata."""
        with self.engine.begin() as session:
            session.execute(
                text("""
                    INSERT INTO sync_metadata (table_name, last_sync_timestamp)
                    VALUES (:table_name, :timestamp)
                    ON CONFLICT (table_name) DO UPDATE
                    SET last_sync_timestamp = :timestamp
                """),
                {"table_name": table_name, "timestamp": timestamp}
            )

    def get_last_sync_version(self, table_name: str) -> int:
        """Fetch the last synced Delta version from metadata."""
        with self.engine.begin() as session:
            result = session.execute(
                select(text("last_version")).select_from(text("sync_metadata"))
                .where(text("table_name = :table_name")),
                {"table_name": table_name}
            ).scalar()
            return result or 0  # Default to version 0 if no sync yet

    def update_last_sync_version(self, table_name: str, version: int) -> None:
        """Update the last synced Delta version in metadata."""
        with self.engine.begin() as session:
            session.execute(
                text("""
                    INSERT INTO sync_metadata (table_name, last_version)
                    VALUES (:table_name, :version)
                    ON CONFLICT (table_name) DO UPDATE
                    SET last_version = :version
                """),
                {"table_name": table_name, "version": version}
            )

    # Table Schema Utilities
    def create_table_if_not_exists(self, table_info: Dict[str, Any], schema: Dict[str, str]) -> None:
        """Create target table if it doesn’t exist."""
        table_name = table_info["table_name"]
        with self.engine.begin() as session:
            if not session.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table)"), 
                                  {"table": table_name}).scalar():
                columns = ", ".join(f"{col} {type}" for col, type in schema.items())
                session.execute(text(f"CREATE TABLE {table_name} ({columns})"))

    def overwrite_table(self, table_info: Dict[str, Any], source_df) -> None:
        """Drop and recreate target table with source data using JDBC."""
        table_name = table_info["table_name"]
        with self.engine.begin() as session:
            session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        jdbc_url = self.config.connection_string.replace("postgresql://", "jdbc:postgresql://").split("?")[0]
        user, password = self.config.connection_string.split("://")[1].split("@")[0].split(":")
        source_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    # (CLI APPROACH)
    def sync_table(self, target_ops, source_info, target_info):
        subprocess.run([
            "dbadminkit", "sync",
            "--source", f"postgres:{source_info['table_name']}",
            "--target", f"databricks:{target_info['table_name']}"
        ])

    # (API APPROACH)
    def sync_table(self, target_ops, source_info, target_info):
        requests.post(
            f"{self.base_url}/sync",
            json={"source": f"postgres:{source_info['table_name']}", "target": f"databricks:{target_info['table_name']}"}
        )

    def verify_sync(self, source_info, target_info):
        # Logic to query Postgres and Databricks, compare data
        # Could use SQL queries or `dbadminkit` commands
        return True  # Placeholder for actual verification

# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# DATA TRANSFER METHODS
# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Bulk Transfer Options (Full Data Movement)
    def transfer_with_jdbc(self, source_table_info: Dict[str, Any], target_config: DatabaseProfile) -> int:
        try:
            if not self.spark:
                raise ValueError("Spark session required")
            source_df = self.spark.table(source_table_info["table_name"])
            row_count = source_df.count()
            jdbc_url = target_config.connection_string.replace("postgresql://", "jdbc:postgresql://").split("?")[0]
            user, password = target_config.connection_string.split("://")[1].split("@")[0].split(":")
            source_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", source_table_info["table_name"]) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info(f"Transferred {row_count} rows from {source_table_info['table_name']} to JDBC target")
            return row_count
        except Exception as e:
            logger.error(f"Failed to transfer data with JDBC: {e}")
            raise

    def transfer_with_csv_copy(self, source_table_info: Dict[str, Any], target_ops: 'DBManager') -> int:
        """Bulk transfer using CSV export and Postgres COPY (Databricks → Postgres)."""
        if not self.spark:
            raise ValueError("Spark session required")
        source_df = self.spark.table(source_table_info["table_name"])
        export_path = "s3://my-bucket/employees_export"  # Requires S3 access
        source_df.write.csv(export_path, mode="overwrite", header=True)
        row_count = source_df.count()
        with target_ops.engine.connect() as conn:
            conn.execute(f"COPY {source_table_info['table_name']} FROM '{export_path}' WITH (FORMAT CSV, HEADER)")
        return row_count

    def transfer_with_streaming(self, source_table_info: Dict[str, Any], target_config: DBConfig) -> int:
        """Bulk transfer using Spark Streaming with JDBC (Databricks → Postgres)."""
        if not self.spark:
            raise ValueError("Spark session required")
        source_df = self.spark.readStream.table(source_table_info["table_name"])
        jdbc_url = target_config.connection_string.replace("postgresql://", "jdbc:postgresql://").split("?")[0]
        user, password = target_config.connection_string.split("://")[1].split("@")[0].split(":")
        
        def write_batch(batch_df, batch_id):
            batch_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", source_table_info["table_name"]) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

        query = source_df.writeStream \
            .foreachBatch(write_batch) \
            .trigger(processingTime="10 seconds") \
            .start()
        query.awaitTermination()  # Non-blocking in practice
        return source_df.count()  # Approximate

    # Incremental Load Options (SCD Type 2 with Change Detection)
    def sync_databricks_to_postgres_stream(self, target_ops: 'DBManager', source_table_info: Dict[str, Any], 
                                           target_table_info: Dict[str, Any], batch_size: int = 10000) -> int:
        """Incremental sync from Databricks to Postgres using Spark Streaming and CDF."""
        if not self.spark:
            raise ValueError("Spark session required")
        
        last_version = target_ops.get_last_sync_version(source_table_info["table_name"])
        changes_df = self.spark.readStream.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", last_version) \
            .table(source_table_info["table_name"])
        
        def process_batch(batch_df, batch_id):
            batch_list = [row.asDict() for row in batch_df.collect()]
            with target_ops.engine.begin() as session:
                target_table = target_ops.get_table(target_table_info)
                for i in range(0, len(batch_list), batch_size):
                    batch = batch_list[i:i + batch_size]
                    for row in batch:
                        change_type = row.pop("_change_type")
                        if change_type in ["insert", "update_postimage"]:
                            target_table.scd_handler.create([row], session)
                        elif change_type == "delete":
                            target_table.scd_handler.delete([row], session)
        
        query = changes_df.writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime="10 seconds") \
            .start()
        query.awaitTermination(timeout=600)  # 10-minute timeout
        
        current_version = self.spark.sql(f"DESCRIBE HISTORY {source_table_info['table_name']}") \
            .select("version").order_by(col("version").desc()).first()[0]
        target_ops.update_last_sync_version(source_table_info["table_name"], current_version)
        return changes_df.count()  # Approximate

    def sync_postgres_to_databricks_csv(self, target_ops: 'DBManager', source_table_info: Dict[str, Any], 
                                        target_table_info: Dict[str, Any], batch_size: int = 10000) -> int:
        """Incremental sync from Postgres to Databricks using CSV and MERGE INTO."""
        last_ts = self.get_last_sync_timestamp(source_table_info["table_name"])
        with self.engine.connect() as conn:
            result = conn.execute(
                select(self.get_table(source_table_info).table)
                .where(self.get_table(source_table_info).table.c.last_modified > last_ts)
            )
            total_synced = 0
            batch = []
            for row in result:
                batch.append(dict(row))
                if len(batch) >= batch_size:
                    changes_df = target_ops.spark.createDataFrame(batch)
                    delta_table = DeltaTable.forName(target_ops.spark, target_table_info["table_name"])
                    delta_table.merge(
                        changes_df.alias("source"),
                        f"target.{source_table_info['key']} = source.{source_table_info['key']} AND target.on_date = source.on_date"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                    total_synced += len(batch)
                    batch = []
            if batch:
                changes_df = target_ops.spark.createDataFrame(batch)
                delta_table = DeltaTable.forName(target_ops.spark, target_table_info["table_name"])
                delta_table.merge(
                    changes_df.alias("source"),
                    f"target.{source_table_info['key']} = source.{source_table_info['key']} AND target.on_date = source.on_date"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
                total_synced += len(batch)
        
        current_ts = datetime.datetime.utcnow().isoformat()
        self.update_last_sync_timestamp(source_table_info["table_name"], current_ts)
        return total_synced

    def sync_databricks_to_postgres_kafka(self, target_ops: 'DBManager', source_table_info: Dict[str, Any], 
                                          target_table_info: Dict[str, Any]) -> int:
        """Incremental sync from Databricks to Postgres using Kafka and CDF (Conceptual)."""
        if not self.spark:
            raise ValueError("Spark session required")
        
        last_version = target_ops.get_last_sync_version(source_table_info["table_name"])
        changes_df = self.spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", last_version) \
            .table(source_table_info["table_name"])
        changes_df.write.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "employees_changes") \
            .save()
        
        consumer = KafkaConsumer("employees_changes", bootstrap_servers="localhost:9092")
        total_applied = 0
        with target_ops.engine.connect() as conn:
            for msg in consumer:
                row = eval(msg.value.decode())  # Assumes JSON-like dict
                change_type = row.pop("_change_type")
                if change_type in ["insert", "update_postimage"]:
                    conn.execute(f"INSERT INTO {target_table_info['table_name']} (...) VALUES (...) ON CONFLICT DO UPDATE ...", row)
                elif change_type == "delete":
                    conn.execute(f"DELETE FROM {target_table_info['table_name']} WHERE emp_id = :id AND on_date = :on_date", 
                                 {"id": row["emp_id"], "on_date": row["on_date"]})
                total_applied += 1
        
        current_version = self.spark.sql(f"DESCRIBE HISTORY {source_table_info['table_name']}") \
            .select("version").order_by(col("version").desc()).first()[0]
        target_ops.update_last_sync_version(source_table_info["table_name"], current_version)
        return total_applied

    def sync_postgres_to_databricks_kafka(self, target_ops: 'DBManager', source_table_info: Dict[str, Any], 
                                          target_table_info: Dict[str, Any]) -> int:
        """Incremental sync from Postgres to Databricks using Kafka (Conceptual)."""
        # Requires Postgres trigger to log changes to Kafka topic 'employees_changes'
        consumer = KafkaConsumer("employees_changes", bootstrap_servers="localhost:9092")
        delta_table = DeltaTable.forName(target_ops.spark, target_table_info["table_name"])
        total_applied = 0
        
        for msg in consumer:
            change = eval(msg.value.decode())  # Assumes JSON-like dict
            row = change["data"]
            if change["change_type"] in ["INSERT", "UPDATE"]:
                changes_df = target_ops.spark.createDataFrame([row])
                delta_table.merge(
                    changes_df.alias("source"),
                    f"target.{source_table_info['key']} = source.{source_table_info['key']} AND target.on_date = source.on_date"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            elif change["change_type"] == "DELETE":
                delta_table.delete(f"{source_table_info['key']} = {row[source_table_info['key']]} AND on_date = '{row['on_date']}'")
            total_applied += 1
        
        current_ts = datetime.datetime.utcnow().isoformat()
        self.update_last_sync_timestamp(source_table_info["table_name"], current_ts)
        return total_applied

    def sync_scd2_versions(self, source_ops: 'DBManager', source_table_info: Dict[str, Any], 
                          target_table_info: Dict[str, Any], batch_size: int = 10000) -> int:
        """General SCD Type 2 sync (Postgres → Databricks or vice versa)."""
        source_table = source_ops.get_table(source_table_info)
        target_table = self.get_table(target_table_info)
        key = source_table_info.get("key", "id")

        last_sync_ts = source_ops.get_last_sync_timestamp(source_table_info["table_name"])
        current_ts = datetime.datetime.utcnow().isoformat()

        source_stmt = (
            select(source_table.table)
            .where(
                (source_table.table.c.on_date > last_sync_ts) |
                (source_table.table.c.off_date > last_sync_ts)
            )
            .order_by(source_table.table.c[key], source_table.table.c.on_date)
        )
        
        total_synced = 0
        with source_ops.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(source_stmt)
            batch = []
            for row in result:
                batch.append(dict(row))
                if len(batch) >= batch_size:
                    if "databricks" in self.db_type.value.lower():
                        updates_df = self.spark.createDataFrame(batch).alias("updates")
                        delta_table = DeltaTable.forName(self.spark, target_table_info["table_name"]).alias("target")
                        delta_table.merge(
                            updates_df,
                            f"target.{key} = updates.{key} AND target.on_date = updates.on_date"
                        ).whenMatchedUpdateAll() \
                         .whenNotMatchedInsertAll() \
                         .execute()
                    else:
                        with self.engine.begin() as session:
                            target_table.scd_handler.create(batch, session)
                    total_synced += len(batch)
                    batch = []
            if batch:
                if "databricks" in self.db_type.value.lower():
                    updates_df = self.spark.createDataFrame(batch).alias("updates")
                    delta_table = DeltaTable.forName(self.spark, target_table_info["table_name"]).alias("target")
                    delta_table.merge(
                        updates_df,
                        f"target.{key} = updates.{key} AND target.on_date = updates.on_date"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    with self.engine.begin() as session:
                        target_table.scd_handler.create(batch, session)
                total_synced += len(batch)

        source_ops.update_last_sync_timestamp(source_table_info["table_name"], current_ts)
        return total_synced

    # Additional Utility Methods
    def import_csvs_to_table(self, table_info: Dict[str, Any], csv_paths: List[str], batch_size: int = 10000) -> int:
        """Import CSVs into a table (Postgres or Databricks)."""
        total_records = 0
        table = self.get_table(table_info)
        if "databricks" in self.db_type.value.lower():
            if not self.spark:
                raise ValueError("Spark session required")
            df = self.spark.read.option("header", "true").csv(csv_paths)
            total_records = df.count()
            df.write.mode("append").saveAsTable(table_info["table_name"])
        else:
            for csv_path in csv_paths:
                file_path = Path(csv_path)
                if not file_path.exists():
                    continue
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    batch = []
                    for row in reader:
                        batch.append(dict(row))
                        if len(batch) >= batch_size:
                            with self.engine.begin() as session:
                                table.scd_handler.create(batch, session)
                            total_records += len(batch)
                            batch = []
                    if batch:
                        with self.engine.begin() as session:
                            table.scd_handler.create(batch, session)
                        total_records += len(batch)
        return total_records

    def import_s3_csv_to_postgres(self, target_ops: 'DBManager', s3_path: str, target_table_info: Dict[str, Any]) -> int:
        """Import S3 CSV to Postgres using Spark-to-JDBC (Databricks → Postgres)."""
        if not self.spark:
            raise ValueError("Spark session required")
        source_df = self.spark.read.option("header", "true").csv(s3_path)
        row_count = source_df.count()
        jdbc_url = target_ops.config.connection_string.replace("postgresql://", "jdbc:postgresql://").split("?")[0]
        user, password = target_ops.config.connection_string.split("://")[1].split("@")[0].split(":")
        source_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", target_table_info["table_name"]) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        return row_count



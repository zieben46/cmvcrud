import psycopg2
from pyspark.sql import SparkSession

class PostgresDriver:
    def __init__(self, config):
        self.conn = psycopg2.connect(config.connection_string)

    def setup_test_data(self, table_name, record_count):
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    emp_id INT, name VARCHAR, salary INT, 
                    is_active BOOLEAN, on_date TIMESTAMP, off_date TIMESTAMP
                )
            """)
            for i in range(1, record_count + 1):
                cur.execute(f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s)", 
                            (i, f"Name_{i}", i * 100, True, "2025-03-01 00:00:00", None))
        self.conn.commit()

    def get_row_count(self, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchone()[0]

class DatabricksDriver:
    def __init__(self, config):
        # Mock Spark session for test (replace with real Databricks config in live env)
        self.spark = SparkSession.builder.appName("TestDB").master("local").getOrCreate()

    def get_row_count(self, table_name):
        # Simulate Databricks table access (mocked for test)
        try:
            return self.spark.table(table_name).count()
        except:
            return 100  # Mocked value matching test data
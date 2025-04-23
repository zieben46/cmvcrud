import os
import logging
from sqlalchemy import create_engine, MetaData, Table, inspect
from databricks.sql import connect
from sqlalchemy.exc import OperationalError

# Configure logging to capture detailed errors
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

# Step 1: Set up Databricks connection parameters for Hive metastore
access_token = os.getenv("DATABRICKS_TOKEN")  # Replace with your token or hardcode for testing
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")  # e.g., adb-1234567890.azuredatabricks.net
http_path = os.getenv("DATABRICKS_HTTP_PATH")  # e.g., /sql/1.0/endpoints/1234567890
schema = "default"  # Replace with your Hive schema (database), e.g., default, my_db

# Step 2: Test basic connectivity with SQLAlchemy
try:
    engine = create_engine(
        f"databricks://token:{access_token}@{server_hostname}?"
        f"http_path={http_path}&schema={schema}"
    )
    with engine.connect() as conn:
        result = conn.execute("SELECT 1")
        print("Connectivity test passed:", result.fetchone())  # Should print (1,)
except Exception as e:
    print("Connectivity failed:", str(e))
    print("Check: Token, hostname, HTTP path, network, or firewall.")
    exit()

# Step 3: Test Hive metastore access with Databricks SQL Connector
try:
    db_conn = connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        schema=schema
    )
    cursor = db_conn.cursor()
    cursor.execute(f"SHOW TABLES IN {schema}")
    tables = [row[1] for row in cursor.fetchall()]
    print("Hive metastore access test passed. Tables:", tables)
    cursor.close()
    db_conn.close()
except Exception as e:
    print("Hive metastore access failed:", str(e))
    print("Likely permissions issue: Check token permissions for schema.")
    print("Ask Databricks admin for SELECT, USAGE permissions on schema.")

# Step 4: Test SQLAlchemy reflection with MetaData.reflect()
try:
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=schema)
    print("Reflection test passed. Tables:", list(metadata.tables.keys()))
except OperationalError as e:
    print("Reflection failed (OperationalError):", str(e))
    print("Likely permissions issue: Token may lack Hive metastore query permissions.")
    print("Check: SELECT, USAGE on schema, or dialect compatibility.")
except Exception as e:
    print("Reflection failed (Other error):", str(e))
    print("Check: Dialect support, Hive table formats (e.g., Delta), or network.")

# Step 5: Test reflection for a single known table
table_name = "my_table"  # Replace with a known table name from Step 3
try:
    table = Table(
        table_name,
        metadata,
        autoload_with=engine,
        schema=schema
    )
    print("Single table reflection passed. Columns:", [c.name for c in table.columns])
except OperationalError as e:
    print(f"Single table reflection failed for {table_name} (OperationalError):", str(e))
    print("Likely permissions issue: Token may lack DESCRIBE TABLE permission.")
except Exception as e:
    print(f"Single table reflection failed for {table_name} (Other error):", str(e))
    print("Check: Table existence, Hive table format, or dialect limitations.")

# Step 6: Test Inspector for table listing
try:
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema=schema)
    print("Inspector test passed. Tables:", tables)
except Exception as e:
    print("Inspector test failed:", str(e))
    print("Likely permissions issue: Token may lack Hive metastore query permissions.")

# Step 7: Interpret results
print("\n=== Diagnosis ===")
if tables:  # From Step 3
    print("Hive metastore accessible via Databricks SQL Connector.")
    print("If reflection failed (Steps 4/5/6), issue is likely:")
    print("- Permissions: Token lacks metastore query permissions (e.g., DESCRIBE, Hive metadata).")
    print("- Dialect: databricks-sqlalchemy may not support Hive reflection fully.")
    print("- Table Formats: Non-standard Hive tables (e.g., Delta, external) may break reflection.")
else:
    print("Hive metastore inaccessible. Issue is likely:")
    print("- Permissions: Token lacks SELECT, USAGE on schema.")
    print("- Configuration: Incorrect schema or network restrictions.")

print("\n=== Next Steps ===")
print("1. Verify token permissions with Databricks admin:")
print("   - SELECT, USAGE on schema.")
print("   - DESCRIBE TABLE, SHOW TABLES access to Hive metastore.")
print("2. Run in Databricks notebook to isolate external access issues:")
print("   - Use same code in a notebook to test reflection.")
print("3. If reflection fails, define tables manually:")
print("   from sqlalchemy import Table, Column, Integer, String")
print(f"   table = Table('{table_name}', metadata,")
print("       Column('id', Integer, primary_key=True),")
print("       Column('name', String),")
print(f"       schema='{schema}')")
print("4. Use databricks-sql-connector for metadata if SQLAlchemy fails:")
print("   See Step 3 for SHOW TABLES, DESCRIBE TABLE.")
print("5. Check databricks-sqlalchemy version and compatibility:")
print("   pip install databricks-sqlalchemy databricks-sql-connector")












# import os
# import logging
# from sqlalchemy import create_engine, MetaData, Table, inspect
# from databricks.sql import connect
# from sqlalchemy.exc import OperationalError

# # Configure logging to capture detailed errors
# logging.basicConfig()
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

# # Step 1: Set up Databricks connection parameters
# access_token = os.getenv("DATABRICKS_TOKEN")  # Replace with your token or hardcode for testing
# server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")  # e.g., adb-1234567890.azuredatabricks.net
# http_path = os.getenv("DATABRICKS_HTTP_PATH")  # e.g., /sql/1.0/endpoints/1234567890
# catalog = "main"  # Replace with your catalog
# schema = "default"  # Replace with your schema

# # Step 2: Test basic connectivity with SQLAlchemy
# try:
#     engine = create_engine(
#         f"databricks://token:{access_token}@{server_hostname}?"
#         f"http_path={http_path}&catalog={catalog}&schema={schema}"
#     )
#     with engine.connect() as conn:
#         result = conn.execute("SELECT 1")
#         print("Connectivity test passed:", result.fetchone())  # Should print (1,)
# except Exception as e:
#     print("Connectivity failed:", str(e))
#     print("Check: Token, hostname, HTTP path, network, or firewall.")
#     exit()

# # Step 3: Test metastore access with Databricks SQL Connector
# try:
#     db_conn = connect(
#         server_hostname=server_hostname,
#         http_path=http_path,
#         access_token=access_token,
#         catalog=catalog,
#         schema=schema
#     )
#     cursor = db_conn.cursor()
#     cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
#     tables = [row[1] for row in cursor.fetchall()]
#     print("Metastore access test passed. Tables:", tables)
#     cursor.close()
#     db_conn.close()
# except Exception as e:
#     print("Metastore access failed:", str(e))
#     print("Likely permissions issue: Check token permissions for catalog/schema.")
#     print("Ask Databricks admin for SELECT, USAGE permissions.")

# # Step 4: Test SQLAlchemy reflection with MetaData.reflect()
# try:
#     metadata = MetaData()
#     metadata.reflect(bind=engine, schema=f"{catalog}.{schema}")
#     print("Reflection test passed. Tables:", list(metadata.tables.keys()))
# except OperationalError as e:
#     print("Reflection failed (OperationalError):", str(e))
#     print("Likely permissions issue: Token may lack metastore query permissions.")
#     print("Check: SELECT, USAGE on catalog/schema, or dialect compatibility.")
# except Exception as e:
#     print("Reflection failed (Other error):", str(e))
#     print("Check: Dialect support, complex types (STRUCT/ARRAY), or network.")

# # Step 5: Test reflection for a single known table
# table_name = "my_table"  # Replace with a known table name from Step 3
# try:
#     table = Table(
#         table_name,
#         metadata,
#         autoload_with=engine,
#         schema=f"{catalog}.{schema}"
#     )
#     print("Single table reflection passed. Columns:", [c.name for c in table.columns])
# except OperationalError as e:
#     print(f"Single table reflection failed for {table_name} (OperationalError):", str(e))
#     print("Likely permissions issue: Token may lack DESCRIBE TABLE permission.")
# except Exception as e:
#     print(f"Single table reflection failed for {table_name} (Other error):", str(e))
#     print("Check: Table existence, complex types, or dialect limitations.")

# # Step 6: Test Inspector for table listing
# try:
#     inspector = inspect(engine)
#     tables = inspector.get_table_names(schema=f"{catalog}.{schema}")
#     print("Inspector test passed. Tables:", tables)
# except Exception as e:
#     print("Inspector test failed:", str(e))
#     print("Likely permissions issue: Token may lack metadata query permissions.")

# # Step 7: Interpret results
# print("\n=== Diagnosis ===")
# if tables:  # From Step 3
#     print("Metastore accessible via Databricks SQL Connector.")
#     print("If reflection failed (Steps 4/5/6), issue is likely:")
#     print("- Permissions: Token lacks metastore query permissions (e.g., DESCRIBE, information_schema).")
#     print("- Dialect: databricks-sqlalchemy may not support full reflection.")
#     print("- Complex Types: Tables with STRUCT/ARRAY may break reflection.")
# else:
#     print("Metastore inaccessible. Issue is likely:")
#     print("- Permissions: Token lacks SELECT, USAGE on catalog/schema.")
#     print("- Configuration: Incorrect catalog/schema or network restrictions.")

# print("\n=== Next Steps ===")
# print("1. Verify token permissions with Databricks admin:")
# print("   - SELECT, USAGE on catalog and schema.")
# print("   - DESCRIBE TABLE, SHOW TABLES access.")
# print("2. Run in Databricks notebook to isolate external access issues:")
# print("   - Use same code in a notebook to test reflection.")
# print("3. If reflection fails, define tables manually:")
# print("   from sqlalchemy import Table, Column, Integer, String")
# print(f"   table = Table('{table_name}', metadata,")
# print("       Column('id', Integer, primary_key=True),")
# print("       Column('name', String),")
# print(f"       schema='{catalog}.{schema}')")
# print("4. Use databricks-sql-connector for metadata if SQLAlchemy fails:")
# print("   See Step 3 for SHOW TABLES, DESCRIBE TABLE.")
# print("5. Check databricks-sqlalchemy version and compatibility:")
# print("   pip install databricks-sqlalchemy databricks-sql-connector")
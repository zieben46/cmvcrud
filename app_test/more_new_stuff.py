import os
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import UUID
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from databricks.sqlalchemy import TIMESTAMP, TINYINT

# Connection configuration
host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")
catalog = os.getenv("DATABRICKS_CATALOG")
schema = os.getenv("DATABRICKS_SCHEMA")

extra_connect_args = {
    "_tls_verify_hostname": True,
    "_user_agent_entry": "PySQL CRUD Functions Example",
}

# Create the engine
engine = create_engine(
    f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}",
    connect_args=extra_connect_args,
    echo=True,
)

# Sample data with multiple records
sample_data = [
    {
        "bigint_col": 9876543210987654321,
        "string_col": "bar",
        "tinyint_col": 50,
        "int_col": 1234,
        "numeric_col": Decimal("12345.67"),
        "boolean_col": False,
        "date_col": date(2023, 11, 15),
        "datetime_col": datetime(2022, 7, 20, 14, 30, tzinfo=timezone(timedelta(hours=-5))),
        "datetime_col_ntz": datetime(2021, 5, 10, 9, 15),
        "time_col": time(12, 30, 45),
        "uuid_col": UUID(int=512),
    },
    {
        "bigint_col": 1234567890123456789,
        "string_col": "foo",
        "tinyint_col": -25,
        "int_col": 5678,
        "numeric_col": Decimal("98765.43"),
        "boolean_col": True,
        "date_col": date(2024, 1, 20),
        "datetime_col": datetime(2023, 8, 15, 10, 0, tzinfo=timezone(timedelta(hours=-8))),
        "datetime_col_ntz": datetime(2022, 12, 1, 15, 45),
        "time_col": time(23, 59, 59),
        "uuid_col": UUID(int=256),
    },
    {
        "bigint_col": 1122334455667788990,
        "string_col": "baz",
        "tinyint_col": 75,
        "int_col": 9012,
        "numeric_col": Decimal("54321.89"),
        "boolean_col": False,
        "date_col": date(2023, 6, 30),
        "datetime_col": datetime(2021, 11, 5, 18, 20, tzinfo=timezone(timedelta(hours=-7))),
        "datetime_col_ntz": datetime(2020, 3, 14, 7, 30),
        "time_col": time(8, 15, 0),
        "uuid_col": UUID(int=128),
    },
]

# CRUD Functions
def create_records(session, SampleObject, data_list):
    """Create multiple records from a list of dictionaries."""
    créée_ids = []
    for data in data_list:
        record = SampleObject(**data)
        session.add(record)
        created_ids.append(data["bigint_col"])
    session.commit()
    print(f"Created {len(created_ids)} records with bigint_col: {created_ids}")
    return created_ids

def read_records(session, SampleObject, bigint_cols):
    """Read records matching the given bigint_col values."""
    stmt = select(SampleObject).where(SampleObject.bigint_col.in_(bigint_cols))
    records = session.execute(stmt).scalars().all()
    result = [{key: getattr(record, key) for key in sample_data[0].keys()} for record in records]
    print(f"Read {len(result)} records: {result}")
    return result

def update_records(session, SampleObject, bigint_cols, update_data):
    """Update records with the given bigint_col values using a dictionary of new values."""
    stmt = select(SampleObject).where(SampleObject.bigint_col.in_(bigint_cols))
    records = session.execute(stmt).scalars().all()
    updated_ids = []
    for record in records:
        for key, value in update_data.items():
            setattr(record, key, value)
        updated_ids.append(record.bigint_col)
    session.commit()
    print(f"Updated {len(updated_ids)} records with bigint_col: {updated_ids}")
    return updated_ids

def delete_records(session, SampleObject, bigint_cols):
    """Delete records with the given bigint_col values."""
    stmt = select(SampleObject).where(SampleObject.bigint_col.in_(bigint_cols))
    records = session.execute(stmt).scalars().all()
    deleted_ids = []
    for record in records:
        session.delete(record)
        deleted_ids.append(record.bigint_col)
    session.commit()
    print(f"Deleted {len(deleted_ids)} records with bigint_col: {deleted_ids}")
    return deleted_ids

def execute_crud_operations(table_name):
    """Execute CRUD operations on the specified table."""
    # Reflect the existing table
    Base = automap_base()
    Base.prepare(autoload_with=engine)
    
    try:
        SampleObject = Base.classes[table_name]  # Dynamically reflect the table
    except KeyError:
        raise ValueError(f"Table '{table_name}' not found in the database schema.")

    with Session(engine) as session:
        # CREATE: Insert multiple records
        bigint_cols = create_records(session, SampleObject, sample_data)

        # READ: Retrieve the created records
        read_records(session, SampleObject, bigint_cols)

        # UPDATE: Update specific fields for the records
        update_values = {
            "string_col": "updated",
            "int_col": 9999,
            "boolean_col": True,
        }
        update_records(session, SampleObject, bigint_cols, update_values)

        # READ: Verify the updated records
        read_records(session, SampleObject, bigint_cols)

        # DELETE: Remove the records
        delete_records(session, SampleObject, bigint_cols)

        # READ: Confirm deletion
        read_records(session, SampleObject, bigint_cols)

# Example usage
if __name__ == "__main__":
    table_name = "test_table"  # Replace with the desired table name
    execute_crud_operations(table_name)













# As a non-admin user in Databricks, finding the Unity Catalog your workspace is using can be straightforward if you have access to the workspace and basic permissions (e.g., USE CATALOG on at least one catalog). Below is the most effective way to identify the catalog, along with guidance on where to look in the Databricks interface and alternative methods using SQL. This assumes your workspace is enabled for Unity Catalog, as is common for workspaces created after November 8, 2023.

# Most Effective Way: Use Catalog Explorer in the Databricks Workspace
# Navigate to Catalog Explorer:
# Log in to your Databricks workspace.

# In the sidebar on the left, click the Catalog icon (it looks like a database or stack of layers).
# If you don’t see the Catalog icon, your workspace may not be enabled for Unity Catalog, or you lack the necessary permissions (e.g., USE CATALOG on any catalog). In this case, proceed to the SQL query method below or contact your workspace admin.

# This opens the Catalog Explorer, which displays the catalogs you have access to.

# View Available Catalogs:
# In Catalog Explorer, the left pane lists all catalogs you have the USE CATALOG privilege for. Common catalogs include:
# A workspace catalog (often named after your workspace, e.g., your_workspace_name), which all workspace users typically have access to by default.

# The main catalog or other custom catalogs created by admins.

# The hive_metastore (if your workspace uses the legacy Hive metastore alongside Unity Catalog).

# Click a catalog to see its schemas (databases) and objects (tables, views, etc.).

# If you see a catalog like __databricks_internal, note that this is a system catalog, and you may need admin intervention to gain USE CATALOG permission if you encounter errors like PERMISSION_DENIED.

# Identify the Default Catalog:
# The default catalog is used when you run queries without specifying a catalog name (e.g., SELECT * FROM my_table assumes default_catalog.my_schema.my_table).

# To check the default catalog, look at the top of the Catalog Explorer pane. The catalog marked as default (or assumed in queries) is often the workspace catalog or one set by an admin.

# Alternatively, use the SQL method below to confirm the default catalog programmatically.

# Alternative Method: Run a SQL Query
# If you have access to a notebook or the SQL Editor and permissions to query catalog metadata, you can use SQL to list catalogs or identify the default catalog. This is particularly useful if you can’t access Catalog Explorer or need to verify programmatically.
# Open a Notebook or SQL Editor:
# In the Databricks workspace, click New in the sidebar and select Notebook, or go to SQL > SQL Editor in the sidebar.

# Ensure you have access to a SQL warehouse or cluster with Unity Catalog enabled.

# Run SQL Commands:
# To list all catalogs you have access to:
# sql

# SHOW CATALOGS;

# This returns a list of catalog names you have the USE CATALOG privilege for. As a non-admin, you’ll only see catalogs you’re authorized to use.

# To check the default catalog:
# sql

# SELECT CURRENT_CATALOG();

# This returns the name of the current default catalog for your session. If your workspace was auto-enabled for Unity Catalog, this is likely the workspace catalog (named after your workspace).

# Example output:

# +---------------------+
# | current_catalog()   |
# +---------------------+
# | your_workspace_name |
# +---------------------+

# Check Permissions (Optional):
# If you suspect you lack access to catalogs, you can check your grants on a specific catalog (replace <catalog_name> with the catalog name, e.g., main, and <your_email> with your email):
# sql

# SHOW GRANTS `<your_email>` ON CATALOG <catalog_name>;

# This shows your privileges (e.g., USE CATALOG, SELECT). Non-admins can only view their own grants unless they have the MANAGE privilege or are the catalog owner.

# Where Is the Catalog Section Located?
# Primary Location: The Catalog section is in the Databricks workspace sidebar, labeled Catalog (with a database icon). Clicking it opens Catalog Explorer, where catalogs are listed in the left pane.

# Alternative Access:
# In a notebook or SQL Editor, you can interact with catalogs via SQL commands (e.g., SHOW CATALOGS).

# If you’re working in the SQL persona (accessible via the top navigation bar), the Catalog Explorer is also available under the Data tab.

# If You Can’t See the Catalog Section:
# Your workspace may not be Unity Catalog-enabled. Check with your workspace admin.

# You may lack USE CATALOG privileges. Run SHOW CATALOGS in a notebook to confirm, or ask an admin to grant access.

# The sidebar might be customized or restricted. Click your username in the top bar, select Settings > Preferences, and ensure the interface is set to show all features.

# Additional Tips for Non-Admin Users
# Workspace Catalog: If your workspace was automatically enabled for Unity Catalog, you likely have access to a workspace catalog (named after your workspace) with default privileges like USE CATALOG, USE SCHEMA, and CREATE TABLE on the default schema. This is a good starting point for exploring data.

# Permissions Limitations: As a non-admin, you can only see catalogs and objects you have privileges for. If SHOW CATALOGS returns nothing, or you get errors like PERMISSION_DENIED: User does not have USE CATALOG, contact your workspace admin to request access.

# Ask Your Admin: If you’re unsure which catalog to use or can’t access Catalog Explorer, ask your workspace admin to:
# Confirm the workspace’s Unity Catalog metastore.

# Grant you USE CATALOG on the relevant catalog (e.g., the workspace catalog or main).

# Share the default catalog name or set one explicitly.

# Check Documentation: If you have access to the workspace’s documentation or shared notebooks, admins may have documented the catalog structure (e.g., main for production data, workspace_name for user data).

# Why This Is the Most Effective Approach
# Catalog Explorer is user-friendly and visual, requiring no coding skills, making it ideal for non-admins. It directly shows accessible catalogs and their schemas.

# SQL Queries provide a programmatic fallback, confirming the default catalog or listing accessible catalogs, even if UI access is limited.

# Non-Admin Constraints: Since you lack admin privileges (e.g., metastore admin or MANAGE on catalogs), you’re limited to viewing your own grants and objects you have access to. These methods work within those constraints.

# If You Still Can’t Find the Catalog
# Verify Unity Catalog Enablement: Run SELECT CURRENT_CATALOG(); in a notebook. If it errors or returns hive_metastore, your workspace may not fully use Unity Catalog. Contact your admin to confirm.

# Reach Out to Admins: Workspace admins can view all catalogs in Catalog Explorer and grant you access. Provide your email and request USE CATALOG on the desired catalog.

# Check Workspace Settings: If you suspect UI issues, ensure your workspace language and preferences are set correctly (Settings > Preferences).

# By starting with Catalog Explorer and falling back to SQL queries, you can efficiently identify the catalog(s) available to you as a non-admin user. If you encounter access issues, your workspace admin is the best resource to resolve them. Let me know if you need help crafting a message to your admin or troubleshooting specific errors


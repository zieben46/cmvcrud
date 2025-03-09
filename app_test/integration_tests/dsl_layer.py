
# DSL Layer:

@Test
@channel("PostgresToDatabricks")
def should_sync_employees_table():
    db_ops = AdminDBOps(postgres_config)
    target_ops = AdminDBOps(databricks_config)
    db_ops.sync_table(target_ops, {"table_name": "employees"}, {"table_name": "employees_target"})
    assert target_ops.verify_sync({"table_name": "employees"}, {"table_name": "employees_target"})


# Protocol Driver (CLI implementation)
subprocess.run(["dbadminkit", "sync", "--source", "postgres:employees", "--target", "databricks:employees_target"])
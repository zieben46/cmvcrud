def test_scd_type2():
    env_path = os.path.join(".env", ".env.postgres_dev")
    assert Config.validate_env_file(env_path, "postgres")
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    table_info = {"table_name": "spend_plan_test", "scd_type": "type2", "key": "id"}
    table = orchestrator.get_table("spend_plan_test_db:safe_user", table_info)
    engine = create_engine(table.adapter.profile.connection_string)
    metadata = MetaData()
    test_table = Table(
        "spend_plan_test",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("category", String),
        Column("amount", Float),
        Column("start_date", DateTime, nullable=True),
        Column("end_date", DateTime, nullable=True),
        Column("is_active", Boolean, nullable=True),
        schema="safe_user"
    )
    metadata.create_all(engine)
    table.create([{"id": 1, "category": "Food", "amount": 50.0}])
    table.update([{"amount": 75.0}], {"id": 1})
    records = table.read({"id": 1})
    assert len(records) == 2  # Old and new versions
    metadata.drop_all(engine, tables=[test_table])
    engine.dispose()
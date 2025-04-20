

def test_table_info_storage():
    env_path = os.path.join(".env", ".env.postgres_dev")
    orchestrator = DataStoreOrchestrator(env_paths=[env_path])
    adapter = orchestrator.adapters["spend_plan_test_db:safe_user"]
    with adapter.session_factory() as session:
        table_info = TableInfo(
            table_name="test_table",
            keys="id",
            scd_type="type1",
            datastore_key="spend_plan_test_db:safe_user",
            schedule_frequency="daily",
            enabled=True
        )
        session.add(table_info)
        session.commit()
        result = session.query(TableInfo).filter_by(table_name="test_table").first()
        assert result is not None
        assert result.keys == "id"
        assert result.schedule_frequency == "daily"
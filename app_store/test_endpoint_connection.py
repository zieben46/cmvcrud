from sqlalchemy import create_engine
engine = create_engine("databricks://token:your_token@your_sql_endpoint_host?http_path=/sql/1.0/endpoints/your_endpoint_id&catalog=your_catalog&schema=your_schema")
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.fetchone())
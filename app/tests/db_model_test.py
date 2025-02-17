# from sqlalchemy.engine import Connection

# import pytest
# from app.models.db_model import DatabaseModel
# from app.models.base_model import CrudType, DatabaseType
# from app.models.db_scd_strategies import SCDType1Strategy



# class DatabaseFactory_SCDType1Strategy_new(DatabaseFactory):
#     def select_scdstrategy(self, scd_type, table):
#         """Returns an instance of SCD Type 1 strategy for handling updates."""
#         return SCDType1Strategy_new(table)  # Return an instance of the correct strategy

# class SCDType1Strategy_new:
#     def __init__(self, table):
#         self.table = table  # Store the reference to the table
    
#     def update(self, conn: Connection, updates: list[dict]):
#         if not updates:
#             return  # No updates provided
        
#         for update_data in updates:
#             entry_id = update_data.pop("id", None)
#             if not entry_id:
#                 raise ValueError("Each update must include an 'id' field.")

#             update_query = self.table.update().where(self.table.c.id == entry_id).values(**update_data)
#             conn.execute(update_query)

#         conn.commit()  # Commit once after all updates


#     def test_insert_using_NEW_scd_strategy_type1(self):
#         factory = DatabaseFactory_SCDType1Strategy_new()
#         db_model = DatabaseModel(DatabaseType.POSTGRES, "user1", factory=factory)

#         kwargs = {
#         "id": 4,  # Primary Key
#         "eoc_code": "NEW_VALUE",
#         "eoc_description": "Updated Description"
#         }
    


# # //EXAMPLE 2

# # class DatabaseFactory_SCDType1Strategy(DatabaseFactory):
# #     def new_select_scdstrategy(self, scd_type, table):
# #         def update(self, conn: Connection, **kwargs):
# #             update_query = self.table.update().where(self.table.c.id == kwargs["id"]).values(**kwargs)
# #             conn.execute(update_query)
# #             conn.commit()





# # //EXAMPLE 1

# class DatabaseFactory_SCDType1Strategy(DatabaseFactory):
#     def select_scdstrategy(self, scd_type, table):
#         return SCDType1Strategy(table)

# def test_insert_using_scd_strategy_type1():
#     factory = DatabaseFactory_SCDType1Strategy()
#     db_model = DatabaseModel(DatabaseType.POSTGRES, "user1", factory=factory)

#     kwargs = {
#     "id": 4,  # Primary Key
#     "eoc_code": "NEW_VALUE",
#     "eoc_description": "Updated Description"
# }


#     with db_model.engine.connect() as conn:
#         db_model.scdstrategy.update(conn, **kwargs)


# # //EXAMPLE 2



# class MockDatabaseFactory(DatabaseFactory):
#     """Mock Factory to replace the actual DB connection with PySpark DataFrame."""
#     def create_engine(self, database):
#         return "MockEngine"

#     def get_table(self, table_name):
#         return "MockTable"

#     def select_scdstrategy(self, scd_type, table):
#         return MockStrategy()  # Use a mock scdstrategy !!!!!!!!!!!!!!!!!!!!!!!!!

# class MockStrategy:
#     """Mock Strategy for CRUD operations."""
#     def create(self, conn, **kwargs):
#         return "Mock CREATE executed"

#     def read(self, conn):
#         return [{"id": 1, "value": "test"}]

#     def update(self, conn, **kwargs):
#         return "Mock UPDATE executed"

#     def delete(self, conn, **kwargs):
#         return "Mock DELETE executed"

# def test_database_model_create(spark):
#     """Test create operation using PySpark."""
#     factory = MockDatabaseFactory()
#     db_model = DatabaseModel(DatabaseType.POSTGRES, "users", factory=factory)
#     result = db_model.execute(CrudType.CREATE, id=1, value="new")
    
#     assert result == "Mock CREATE executed"

# def test_database_model_read(spark):
#     """Test read operation using PySpark."""
#     factory = MockDatabaseFactory()
#     db_model = DatabaseModel(DatabaseType.POSTGRES, "users", factory=factory)
#     result = db_model.execute(CrudType.READ)
    
#     assert result == [{"id": 1, "value": "test"}]







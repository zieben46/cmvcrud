from sqlalchemy.engine import Connection

import pytest
from app.models.db_model import DatabaseModel, DatabaseFactory
from app.models.base_model import CrudType, DatabaseType
from app.models.db_scd_strategies import SCDType1Strategy

# //EXAMPLE

class DatabaseFactory_SCDType1Strategy(DatabaseFactory):
    def select_scdstrategy(self, scd_type, table):
        return SCDType1Strategy(table)

def test_insert_using_scd_strategy_type1():
    factory = DatabaseFactory_SCDType1Strategy()
    db_model = DatabaseModel(DatabaseType.POSTGRES, "user1", factory=factory)

    kwargs = {
    "id": 4,  # Primary Key
    "eoc_code": "NEW_VALUE",
    "eoc_description": "Updated Description"
}


    with db_model.engine.connect() as conn:
        db_model.scdstrategy.update(conn, **kwargs)



class DatabaseFactory_SCDType1Strategy_new(DatabaseFactory):
    def select_scdstrategy(self, scd_type, table):
        """Returns an instance of SCD Type 1 strategy for handling updates."""
        return SCDType1Strategy_new(table)  # Return an instance of the correct strategy

class SCDType1Strategy_new:
    def __init__(self, table):
        self.table = table  # Store the reference to the table
    
    #t
    def update(self, conn: Connection, updates: list[dict]):
        if not updates:
            return  # No updates provided
        
        for update_data in updates:
            entry_id = update_data.pop("id", None)
            if not entry_id:
                raise ValueError("Each update must include an 'id' field.")

            update_query = self.table.update().where(self.table.c.id == entry_id).values(**update_data)
            conn.execute(update_query)

        conn.commit()  # Commit once after all updates

    # def update(self, conn: Connection, **kwargs):
    #     update_query = self.table.update().where(self.table.c.id == kwargs["id"]).values(**kwargs)
    #     conn.execute(update_query)
    #     conn.commit()


def test_insert_using_NEW_scd_strategy_type1():

    factory = DatabaseFactory_SCDType1Strategy_new()
    db_model = DatabaseModel(DatabaseType.POSTGRES, "user1", factory=factory)
    kwargs = {
    "id": 4,
    "eoc_code": "NEW_VALUE!!!!!!!!",
    "eoc_description": "Updated Description"
    }

    with db_model.engine.connect() as conn:
        db_model.scdstrategy.update(conn, **kwargs)
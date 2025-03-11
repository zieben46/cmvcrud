from abc import ABC, abstractmethod
from typing import Dict, Any, List
from dbadminkit.database_manager import DBManager
from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.core.crud_types import CRUDOperation

class DatabaseDriver(ABC):
    @abstractmethod
    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def update(self, table_info: Dict[str, str], data: Dict[str, Any], filters: Dict[str, Any]):
        pass

    @abstractmethod
    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        pass

    @abstractmethod
    def sync_to(self, source_table_info: Dict[str, str], target_manager: DBManager, target_table: str, method: str):
        pass

class PostgresDriver:
    def __init__(self, profile):
        self.manager = profile  # Assume profile provides an engine

    def create_table(self, table_name: str, schema: Dict[str, str]):
        metadata = MetaData()
        columns = [Column(col, eval(type_)) for col, type_ in schema.items()]  # Use proper types in practice
        table = Table(table_name, metadata, *columns)
        metadata.create_all(self.manager.engine)
        logger.info(f"Created table {table_name} in Postgres")

    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        with self.manager.engine.begin() as session:
            self.manager.get_table(table_info).scd_handler.create(data, session)

    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self.manager.perform_crud(table_info, CRUDOperation.READ, filters)

    def update(self, table_info: Dict[str, str], data: Dict[str, Any], filters: Dict[str, Any]):
        self.manager.perform_crud(table_info, CRUDOperation.UPDATE, data, filters)

    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        self.manager.perform_crud(table_info, CRUDOperation.DELETE, filters)

    def sync_to(self, source_table_info: Dict[str, str], target_manager: DBManager, target_table: str, method: str):
        if method == "jdbc":
            self.manager.transfer_with_jdbc(source_table_info, target_manager.config)
        elif method == "csv":
            self.manager.transfer_with_csv_copy(source_table_info, target_manager)
        else:
            raise ValueError(f"Unsupported sync method: {method}")

class DatabricksDriver:
    def __init__(self, profile):
        self.manager = profile  # Assume profile provides a Spark session

    def create_table(self, table_name: str, schema: Dict[str, str]):
        columns = ", ".join([f"{col} {type_}" for col, type_ in schema.items()])
        self.manager.spark.sql(f"CREATE TABLE {table_name} ({columns})")
        logger.info(f"Created table {table_name} in Databricks")

    def create(self, table_info: Dict[str, str], data: List[Dict[str, Any]]):
        df = self.manager.spark.createDataFrame(data)
        df.write.mode("append").saveAsTable(table_info["table_name"])

    def read(self, table_info: Dict[str, str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self.manager.perform_crud(table_info, CRUDOperation.READ, filters)

    def update(self, table_info: Dict[str, str], data: Dict[str, Any], filters: Dict[str, Any]):
        self.manager.perform_crud(table_info, CRUDOperation.UPDATE, data, filters)

    def delete(self, table_info: Dict[str, str], filters: Dict[str, Any]):
        self.manager.perform_crud(table_info, CRUDOperation.DELETE, filters)

    def sync_to(self, source_table_info: Dict[str, str], target_manager: DBManager, target_table: str, method: str):
        if method == "jdbc":
            self.manager.transfer_with_jdbc(source_table_info, target_manager.config)
        elif method == "csv":
            self.manager.transfer_with_csv_copy(source_table_info, target_manager)
        else:
            raise ValueError(f"Unsupported sync method: {method}")
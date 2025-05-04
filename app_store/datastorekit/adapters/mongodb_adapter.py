# datastorekit/adapters/mongodb_adapter.py
from pymongo.errors import DuplicateKeyError as MongoDuplicateKeyError
from datastorekit.adapters.base import DatastoreAdapter
from datastorekit.exceptions import DuplicateKeyError, DatastoreOperationError
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class MongoDBAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.connection = DatastoreConnection(profile)
        self.client = self.connection.get_mongo_client()
        self.db = self.client[profile.dbname]

    def get_reflected_keys(self, table_name: str) -> List[str]:
        return ["_id"]

    def create_table(self, table_name: str, schema: Dict[str, Any], schema_name: Optional[str] = None):
        """MongoDB collections are created on first insert, no action needed."""
        logger.info(f"MongoDB collection {table_name} will be created on first insert with schema {schema}")

    def get_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> Dict[str, str]:
        """Infer column names and types from collection data."""
        try:
            collection = self.db[table_name]
            sample_doc = collection.find_one()
            if not sample_doc:
                return {}
            return {k: type(v).__name__ for k, v in sample_doc.items() if k != "_id"}
        except Exception as e:
            logger.error(f"Failed to get columns for collection {table_name}: {e}")
            return {}

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        try:
            collection = self.db[table_name]
            collection.insert_many(data)
        except MongoDuplicateKeyError as e:
            logger.error(f"Duplicate key error in MongoDB insert for {table_name}: {e}")
            raise DuplicateKeyError(f"Duplicate key error during insert on {table_name}: {e}")
        except Exception as e:
            logger.error(f"Insert failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during insert on {table_name}: {e}")

    def select(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        collection = self.db[table_name]
        query = filters or {}
        return list(collection.find(query))

    def select_chunks(self, table_name: str, filters: Optional[Dict[str, Any]] = None, chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        collection = self.db[table_name]
        query = filters or {}
        cursor = collection.find(query).batch_size(chunk_size)
        chunk = []
        for doc in cursor:
            chunk.append(doc)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None):
        collection = self.db[table_name]
        try:
            for update_data in data:
                query = filters or {"_id": update_data.get("_id")}
                collection.update_one(query, {"$set": update_data})
        except Exception as e:
            logger.error(f"Update failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during update on {table_name}: {e}")

    def delete(self, table_name: str, filters: Optional[Dict[str, Any]] = None):
        collection = self.db[table_name]
        query = filters or {}
        try:
            collection.delete_many(query)
        except Exception as e:
            logger.error(f"Delete failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Error during delete on {table_name}: {e}")

    def apply_changes(self, table_name: str, inserts: List[Dict[str, Any]], 
                      updates: List[Dict[str, Any]], deletes: List[Dict[str, Any]]):
        key_columns = self.get_reflected_keys(table_name)
        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    collection = self.db[table_name]
                    if inserts:
                        collection.insert_many(inserts, session=session)
                        logger.debug(f"Applied {len(inserts)} inserts to {table_name}")
                    if updates:
                        for update_data in updates:
                            key_filter = {key: update_data[key] for key in key_columns if key in update_data}
                            update_values = {k: v for k, v in update_data.items() if k not in key_columns}
                            if update_values:
                                collection.update_one(key_filter, {"$set": update_values}, session=session)
                        logger.debug(f"Applied {len(updates)} updates to {table_name}")
                    if deletes:
                        key_filters = [{key: d[key] for key in key_columns if key in d} for d in deletes]
                        if key_filters:
                            collection.delete_many({"$or": key_filters}, session=session)
                        logger.debug(f"Applied {len(deletes)} deletes to {table_name}")
        except MongoDuplicateKeyError as e:
            logger.error(f"Duplicate key error in MongoDB apply_changes for {table_name}: {e}")
            raise DuplicateKeyError(f"Duplicate key error during apply_changes on {table_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to apply changes to {table_name}: {e}")
            raise DatastoreOperationError(f"Error during apply_changes on {table_name}: {e}")

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError("SQL execution is not supported for MongoDBAdapter")

    def list_tables(self, schema: str) -> List[str]:
        try:
            return self.db.list_collection_names()
        except Exception as e:
            logger.error(f"Failed to list collections: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        metadata = {}
        for table_name in self.list_tables(schema):
            columns = self.get_table_columns(table_name)
            metadata[table_name] = {
                "columns": columns,
                "primary_keys": ["_id"]
            }
        return metadata
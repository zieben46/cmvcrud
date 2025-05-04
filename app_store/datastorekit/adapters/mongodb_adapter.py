from pymongo import MongoClient
from typing import Dict, Any, List, Iterator, Optional
from bson import json_util
import json
from datastorekit.adapters.base import DatastoreAdapter
import logging

logger = logging.getLogger(__name__)

class MongoDBAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        client = MongoClient(profile.connection_string)
        self.db = client[profile.dbname]

    def get_reflected_keys(self, table_name: str) -> List[str]:
        """MongoDB uses '_id' as the default key unless overridden."""
        return ["_id"]  # Default for MongoDB collection

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys are present in the collection."""
        collection = self.db[table_name]
        sample_doc = collection.find_one()
        if sample_doc:
            doc_keys = set(sample_doc.keys())
            if not all(key in doc_keys for key in table_info_keys):
                raise ValueError(
                    f"Collection {table_name} keys {doc_keys} do not include all TableInfo keys {table_info_keys}"
                )

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

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(","))
        collection = self.db[table_name]
        cursor = collection.find(filters).batch_size(chunk_size)
        chunk = []
        for record in cursor:
            chunk.append(json.loads(json_util.dumps(record)))
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

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query (not supported for MongoDB). Use MongoDB query syntax instead."""
        raise NotImplementedError("SQL execution is not supported for MongoDBAdapter")

    def list_tables(self, schema: str) -> List[str]:
        """List collections in the MongoDB database."""
        try:
            return self.db.list_collection_names()
        except Exception as e:
            logger.error(f"Failed to list collections: {e}")
            return []

    def get_table_metadata(self, schema: str) -> Dict[str, Dict]:
        """Get metadata for all collections in the MongoDB database."""
        metadata = {}
        for collection_name in self.list_tables(schema):
            collection = self.db[collection_name]
            sample_doc = collection.find_one()
            if sample_doc:
                columns = {key: str(type(value).__name__) for key, value in sample_doc.items()}
                pk_columns = self.table_info.keys.split(",") if self.table_info else ["_id"]
                metadata[collection_name] = {
                    "columns": columns,
                    "primary_keys": pk_columns
                }
        return metadata
    

    from pymongo import ClientSession

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
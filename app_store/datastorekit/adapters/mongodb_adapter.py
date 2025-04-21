# datastorekit/adapters/mongodb_adapter.py
from pymongo import MongoClient
from typing import Dict, Any, List, Iterator, Optional
from bson import json_util
import json
from datastorekit.adapters.base import DatastoreAdapter
import logging

logger = logging.getLogger(__name__)

class MongoDBAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        client = MongoClient(profile.connection_string)
        self.db = client[profile.dbname]

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
        collection = self.db[table_name]
        collection.insert_many(data)

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        collection = self.db[table_name]
        records = collection.find(filters)
        return [json.loads(json_util.dumps(record)) for record in records]

    def select_chunks(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
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

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        collection = self.db[table_name]
        for update_data in data:
            collection.update_many(filters, {"$set": update_data})

    def delete(self, table_name: str, filters: Dict[str, Any]):
        collection = self.db[table_name]
        collection.delete_many(filters)

    def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query (not supported for MongoDB)."""
        raise NotImplementedError("SQL execution is not supported for MongoDBAdapter")
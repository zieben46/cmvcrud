# datastorekit/adapters/mongodb_adapter.py
import pymongo
from datastorekit.adapters.base import DatastoreAdapter
from typing import List, Dict, Any

class MongoDBAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.client = pymongo.MongoClient(profile.connection_string)
        self.db = self.client[profile.dbname]

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        if data:
            self.db[table_name].insert_many(data)

    def select(self, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(self.db[table_name].find(filters))

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        for update_data in data:
            self.db[table_name].update_many(filters, {"$set": update_data})

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.db[table_name].delete_many(filters)
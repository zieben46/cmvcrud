# datastorekit/scd/type0.py
from datastorekit.scd.base import SCDHandler
from typing import List, Dict, Any, Optional
from datastorekit.exceptions import DatastoreOperationError
import logging

logger = logging.getLogger(__name__)

class Type0Handler(SCDHandler):
    def create(self, records: List[Dict]) -> int:
        try:
            if not records:
                return 0
            logger.debug(f"Creating {len(records)} records in {self.table_name} with SCD Type 0")
            return self.adapter.create(self.table_name, records)
        except Exception as e:
            logger.error(f"Failed to create records in {self.table_name}: {e}")
            raise DatastoreOperationError(f"Failed to create records: {e}")

    def read(self, filters: Optional[Dict] = None) -> List[Dict]:
        try:
            logger.debug(f"Reading records from {self.table_name} with filters: {filters}")
            return self.adapter.read(self.table_name, filters)
        except Exception as e:
            logger.error(f"Failed to read records from {self.table_name}: {e}")
            raise DatastoreOperationError(f"Failed to read records: {e}")

    def update(self, updates: List[Dict]) -> int:
        try:
            logger.error(f"Update operation attempted on {self.table_name} with SCD Type 0")
            raise NotImplementedError("SCD Type 0 does not support updates")
        except NotImplementedError as e:
            logger.error(f"Update operation failed: {e}")
            raise

    def delete(self, conditions: List[Dict]) -> int:
        try:
            logger.error(f"Delete operation attempted on {self.table_name} with SCD Type 0")
            raise NotImplementedError("SCD Type 0 does not support deletes")
        except NotImplementedError as e:
            logger.error(f"Delete operation failed: {e}")
            raise
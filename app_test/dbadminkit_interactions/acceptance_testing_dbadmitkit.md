# Acceptance Testing for `dbadminkit` Using Dave Farley’s TDD Practices

This document outlines how to implement acceptance testing for the `dbadminkit` project, following Dave Farley’s best practices from Test-Driven Development (TDD). Farley’s approach tests **what** the system does (its behavior) rather than **how** it does it (its implementation), evaluating the system in life-like scenarios from an external user’s perspective in production-like environments. His four-layer acceptance testing model—**Test Case**, **Domain-Specific Language (DSL)**, **Protocol Driver**, and **System Under Test**—is adapted here for `dbadminkit`, which manages data transfers between Databricks and Postgres (e.g., bulk 20M record loads, incremental SCD Type 2 syncs).

We’ll use Docker to simulate a production-like Postgres environment and leverage Python’s `pytest` framework to execute these tests, integrating with the CLI entry point in `dbadminkit.cli.py`.


## Farley’s Four-Layer Acceptance Testing Model

### 1. Test Case (Executable Specification)
This layer contains the actual test cases written using the DSL, executed by the testing framework using the drivers. It defines the scenarios to be tested and verifies results against expected outcomes, ensuring the tests are both reliable and repeatable. This layer bridges the gap between the high-level specifications and the practical execution against the SUT.

### 2. Domain-Specific Language (DSL) Layer
The Domain Specific Language (DSL) layer involves creating a high-level language for writing acceptance tests. This language is designed to be close to the problem domain, making tests readable and understandable by business stakeholders. It helps in specifying test cases in a way that aligns with user requirements, enhancing communication between technical and non-technical teams.

### 3. Protocol Driver
The drivers layer consists of components or libraries that handle low-level interactions with the system under test (SUT). These drivers, such as Selenium for web UI or HTTP clients for APIs, abstract away the complexities of direct system interaction, ensuring the test implementation remains clean and resilient to changes in the SUT's interface.

### 4. System Under Test
The system under test (SUT) is the software application or system being tested. It is the target of all acceptance tests, and the other layers work together to interact with and verify its behavior, ensuring it meets user expectations in a production-like environment.

---

## Acceptance Testing Goals for `dbadminkit`
- **Life-Like Scenarios**: Test table syncing (e.g., Postgres to Databricks).
- **External User Perspective**: Simulate a data engineer running CLI commands.
- **Production-Like Environment**: Use Docker for Postgres and a mock Databricks setup.
- **Test WHAT, Not HOW**: Verify outcomes (e.g., “data is synced”) without checking internals (e.g., Spark execution details).

---

## Example Acceptance Test Scenarios

### Scenario 1: Sync `employees` Table from Postgres to Databricks
- **Description**: A data engineer syncs the `employees` table from Postgres to Databricks in a test environment and verifies the data matches.
- **Test Case**: "Should sync the `employees` table from Postgres to Databricks with the CLI."

### Scenario 2: Bulk Transfer from Databricks to Postgres
- **Description**: A data engineer performs a bulk transfer of the `customers` table from Databricks to Postgres and verifies the row count.
- **Test Case**: "Should bulk transfer the `customers` table from Databricks to Postgres with the CLI."

---

## Implementation in `dbadminkit`

### Directory Structure

dbadminkit/
├── core/
│   ├── admin_operations.py  # DBManager class
│   ├── config.py           # DBConfig class
│   └── init.py
├── cli.py                  # CLI entry point
├── main.py             # Makes package executable
├── tests/
│   ├── acceptance/
│   │   ├── dsl.py         # DSL layer (DataOpsDSL)
│   │   ├── drivers.py     # Protocol driver implementations
│   │   └── test_cases.py  # Test cases (executable specs)
│   └── conftest.py        # Pytest fixtures (Docker setup)
├── Dockerfile             # Postgres container setup
├── setup.py               # Package definition
└── requirements.txt       # Dependencies



### 1. Test Case Layer
**File**: `tests/acceptance/test_crud.py`  
High-level executable specifications.

```python
import pytest
from dbadminkit.core.database_profile import DatabaseProfile
from dbadminkit.core.crud_types import CRUDOperation
from dsl import CrudDSL, PostgresDriver, DatabricksDriver

class TestCrud:
    def setUp(self, database_type):
        """Initialize the DSL and driver based on database type."""
        if database_type == "postgres":
            self.driver = PostgresDriver(DatabaseProfile.in_memory())
        elif database_type == "databricks":
            self.driver = DatabricksDriver(DatabaseProfile.test_databricks())
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        self.dsl = CrudDSL(self.driver)

    def test_crud_operations_postgres(self):
        self.setUp("postgres")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.CREATE, {"emp_id": 2, "name": "Bob"}) \
                .assert_table_has([{"emp_id": 1, "name": "Alice"}, {"emp_id": 2, "name": "Bob"}])

    def test_crud_operations_databricks(self):
        self.setUp("databricks")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.CREATE, {"emp_id": 2, "name": "Bob"}) \
                .assert_table_has([{"emp_id": 1, "name": "Alice"}, {"emp_id": 2, "name": "Bob"}])

    def test_update_postgres(self):
        self.setUp("postgres")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.UPDATE, {"name": "Updated"}, {"emp_id": 1}) \
                .assert_table_has([{"emp_id": 1, "name": "Updated"}])

    def test_update_databricks(self):
        self.setUp("databricks")
        schema = {"emp_id": "Integer", "name": "String"}
        self.dsl.create_table("employees", schema, key="emp_id") \
                .setup_data([{"emp_id": 1, "name": "Alice"}]) \
                .execute_crud(CRUDOperation.UPDATE, {"name": "Updated"}, {"emp_id": 1}) \
                .assert_table_has([{"emp_id": 1, "name": "Updated"}])
```


### 2. Domain-Specific Language (DSL) Layer
**File**: ` tests/acceptance/dsl.py`  
Readable abstraction for test steps.

```python
from typing import Dict, Any, List
import pandas as pd

import logging
from typing import Dict, Any, List
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class DatabaseDSL:
    def __init__(self, driver):
        self.driver = driver
        self.selected_table = None

    def create_table(self, table_name: str, schema: Dict[str, str], key: str = "id"):
        """Create a table with the given schema."""
        if not schema:
            raise ValueError("Schema must be provided")
        self.selected_table = {"table_name": table_name, "key": key}
        self.driver.create_table(table_name, schema)
        logger.info(f"Created table {table_name} with schema {schema}")
        return self

    def select_table(self, table_name: str, key: str = "id"):
        self.selected_table = {"table_name": table_name, "key": key}
        return self

    def setup_data(self, records: List[Dict[str, Any]]):
        if not self.selected_table:
            raise ValueError("No table selected")
        self.driver.create(self.selected_table, records)
        logger.info(f"Inserted {len(records)} records into {self.selected_table['table_name']}")
        return self

    def assert_table_has(self, expected_records: List[Dict[str, Any]]):
        if not self.selected_table:
            raise ValueError("No table selected")
        actual_data = self.driver.read(self.selected_table, {})
        actual_df = pd.DataFrame(actual_data)
        expected_df = pd.DataFrame(expected_records)
        pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)
        logger.info(f"Assertion passed for table {self.selected_table['table_name']}")
        return self

    def assert_record_exists(self, filters: Dict[str, Any], expected_data: Dict[str, Any]):
        if not self.selected_table:
            raise ValueError("No table selected")
        data = self.driver.read(self.selected_table, filters)
        assert len(data) == 1, "Record not found"
        for key, value in expected_data.items():
            assert data[0][key] == value, f"Mismatch in {key}"
        return self

class CrudDSL(DatabaseDSL):
    def execute_crud(self, operation: CRUDOperation, data: Dict[str, Any], filters: Dict[str, Any] = None):
        if not self.selected_table:
            raise ValueError("No table selected")
        if operation == CRUDOperation.CREATE:
            self.driver.create(self.selected_table, [data])
        elif operation == CRUDOperation.READ:
            return self.driver.read(self.selected_table, data or {})
        elif operation == CRUDOperation.UPDATE:
            self.driver.update(self.selected_table, data, filters or data)
        elif operation == CRUDOperation.DELETE:
            self.driver.delete(self.selected_table, filters or data)
        return self

class SyncDSL(DatabaseDSL):
    def __init__(self, source_driver: DatabaseDriver, target_driver: DatabaseDriver):
        super().__init__(source_driver)
        self.target_driver = target_driver
        self.target_table = None

    def sync_to_target(self, target_table: str, sync_method: str = "jdbc"):
        if not self.selected_table:
            raise ValueError("No source table selected")
        self.target_table = {"table_name": target_table, "key": self.selected_table["key"]}
        self.driver.sync_to(self.selected_table, self.target_driver.manager, target_table, sync_method)
        return self

    def assert_tables_synced(self):
        if not self.selected_table or not self.target_table:
            raise ValueError("Tables not selected")
        source_data = self.driver.read(self.selected_table, {})
        target_data = self.target_driver.read(self.target_table, {})
        source_df = pd.DataFrame(source_data)
        target_df = pd.DataFrame(target_data)
        pd.testing.assert_frame_equal(source_df, target_df, check_like=True)
        return self
```



### 3. Protocol Driver Layer
**File**: `tests/acceptance/drivers.py`  
Implements DSL actions with real system interactions.

```python

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
```

### 4. System Under Test
Tested from the perspective of a python developer.
```

Running the Tests


1.  Install Package:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

2. Install Dependencies:
```bash
pip install -r requirements.txt
```

3. Build Docker Image:
```bash
docker build -t dbadminkit-postgres-test .
```

4. Run Tests:
```bash
pytest tests/acceptance/ -v --disable-warnings
```

Requirements (requirements.txt)


```text
requirements.txt
pytest==7.4.0
psycopg2-binary==2.9.6
pyspark==3.4.0
docker==6.1.3
```

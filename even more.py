from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging
import os
import pandas as pd
from sqlalchemy import create_engine, Table
from copy import deepcopy
from datastorekit.exceptions import DatastoreOperationError
from datastorekit.adapters.base import DatastoreAdapter, DatabaseProfile

# Configure logging
logger = logging.getLogger(__name__)

# Define TableInfo dataclass
@dataclass
class TableInfo:
    table_name: str
    keys: List[str]  # List of column names used for conflict/merge condition
    columns: List[str]  # List of all column names in the table

# Define DBTable for CSV and InMemory adapters
@dataclass
class DBTable:
    table_name: str
    file_path: Optional[str] = None

# Adapter Interface
class DatabaseAdapter(ABC):
    @abstractmethod
    def upsert(self, table_info: TableInfo, data: List[Dict]):
        pass

# PostgreSQL Adapter
class PostgresAdapter(DatabaseAdapter):
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()

    def _get_table(self, table_name: str) -> Table:
        return Table(table_name, self.metadata, autoload_with=self.engine)

    def upsert(self, table_info: TableInfo, data: List[Dict]):
        if not data:
            return
        table = self._get_table(table_info.table_name)
        with self.engine.connect() as conn:
            insert_stmt = sa.insert(table).values(data)
            update_dict = {
                col: insert_stmt.excluded[col]
                for col in table_info.columns
                if col not in table_info.keys
            }
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=table_info.keys,
                set_=update_dict
            )
            conn.execute(upsert_stmt)
            conn.commit()

# Databricks Adapter
class DatabricksAdapter(DatabaseAdapter):
    def __init__(self, conn):
        self.conn = conn

    def execute_sql(self, sql: str, parameters: Optional[Dict] = None) -> List[Dict]:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, parameters or {})
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
        except Exception as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {e}")
            raise

    def upsert(self, table_info: TableInfo, data: List[Dict]):
        if not data:
            return
        value_placeholders = []
        parameters = {}
        for i, row in enumerate(data):
            row_placeholders = [f"%({col}_{i})s" for col in table_info.columns]
            value_placeholders.append(f"({', '.join(row_placeholders)})")
            for col in table_info.columns:
                parameters[f"{col}_{i}"] = row.get(col)
        values_clause = ', '.join(value_placeholders)
        merge_condition = ' AND '.join(f"target.{key} = source.{key}" for key in table_info.keys)
        update_clause = ', '.join(f"target.{col} = source.{col}" for col in table_info.columns if col not in table_info.keys)
        insert_columns = ', '.join(table_info.columns)
        insert_values = ', '.join(f"source.{col}" for col in table_info.columns)
        sql = f"""
        MERGE INTO {table_info.table_name} AS target
        USING (VALUES {values_clause}) AS source({insert_columns})
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
        """
        self.execute_sql(sql, parameters)

# CSV Adapter
class CSVAdapter(DatabaseAdapter, DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.base_dir = profile.connection_string or "./data"
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, table_name: str) -> str:
        return os.path.join(self.base_dir, f"{table_name}.csv")

    def get_reflected_keys(self, table_name: str) -> List[str]:
        return self.profile.keys.split(",") if self.profile.keys else []

    def upsert(self, table_info: TableInfo, data: List[Dict]):
        try:
            if not data:
                return
            dbtable = DBTable(table_info.table_name, self._get_file_path(table_info.table_name))
            primary_keys = table_info.keys or self.get_reflected_keys(table_info.table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for upsert operations.")
            file_path = dbtable.file_path
            df = pd.read_csv(file_path) if os.path.exists(file_path) else pd.DataFrame(columns=table_info.columns)
            new_df = pd.DataFrame(data)
            if not all(col in new_df.columns for col in table_info.columns):
                raise ValueError(f"Input data must contain all columns: {table_info.columns}")
            if not all(pk in new_df.columns for pk in primary_keys):
                raise ValueError(f"Input data missing primary key(s): {primary_keys}")
            if not df.empty:
                mask = df[primary_keys].apply(tuple, axis=1).isin(new_df[primary_keys].apply(tuple, axis=1))
                for _, row in new_df.iterrows():
                    row_keys = tuple(row[pk] for pk in primary_keys)
                    df_mask = df[primary_keys].apply(tuple, axis=1) == row_keys
                    if df_mask.any():
                        for col in table_info.columns:
                            if col not in primary_keys:
                                df.loc[df_mask, col] = row[col]
                new_df = new_df[~new_df[primary_keys].apply(tuple, axis=1).isin(df[primary_keys].apply(tuple, axis=1))]
            result_df = pd.concat([df, new_df], ignore_index=True)
            temp_path = file_path + ".tmp"
            result_df.to_csv(temp_path, index=False)
            os.replace(temp_path, file_path)
            logger.info(f"Upserted {len(data)} records into {table_info.table_name}")
        except Exception as e:
            logger.error(f"Upsert failed for {table_info.table_name}: {e}")
            raise DatastoreOperationError(f"Upsert failed for {table_info.table_name}: {e}")

# InMemory Adapter
class InMemoryAdapter(DatabaseAdapter, DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        super().__init__(profile)
        self.data = {}

    def get_reflected_keys(self, table_name: str) -> List[str]:
        return self.profile.keys.split(",") if self.profile.keys else []

    def upsert(self, table_info: TableInfo, data: List[Dict]):
        try:
            if not data:
                return
            table_name = table_info.table_name
            primary_keys = table_info.keys or self.get_reflected_keys(table_name)
            if not primary_keys:
                raise ValueError("Table must have a primary key for upsert operations.")
            if table_name not in self.data:
                self.data[table_name] = []
            if not all(all(col in record for col in table_info.columns) for record in data):
                raise ValueError(f"Input data must contain all columns: {table_info.columns}")
            if not all(all(pk in record for pk in primary_keys) for record in data):
                raise ValueError(f"Input data missing primary key(s): {primary_keys}")
            existing_records = self.data[table_name]
            new_records = []
            updated_count = 0
            for new_record in data:
                new_key = tuple(new_record[pk] for pk in primary_keys)
                matched = False
                for record in existing_records:
                    if tuple(record[pk] for pk in primary_keys) == new_key:
                        for col in table_info.columns:
                            if col not in primary_keys:
                                record[col] = deepcopy(new_record[col])
                        updated_count += 1
                        matched = True
                        break
                if not matched:
                    new_records.append(deepcopy(new_record))
            self.data[table_name].extend(new_records)
            logger.info(f"Upserted {len(data)} records into {table_name} ({updated_count} updated, {len(new_records)} inserted)")
        except Exception as e:
            logger.error(f"Upsert failed for {table_name}: {e}")
            raise DatastoreOperationError(f"Upsert failed for {table_name}: {e}")


ghgfdhgfhgfhfgh












import streamlit as st
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Optional
import os
import logging
from datastorekit.exceptions import DatastoreOperationError
from datastorekit.adapters.base import DatastoreAdapter, DatabaseProfile
from copy import deepcopy

# Configure logging
logger = logging.getLogger(__name__)


# Streamlit App
def main():
    st.title("CSV UPSERT App")
    st.write("Upload a CSV file and specify table info to perform an UPSERT operation.")

    # Input fields for TableInfo
    table_name = st.text_input("Table Name", "users")
    keys = st.text_input("Primary Keys (comma-separated)", "id")
    columns = st.text_input("Columns (comma-separated)", "id,name,email")
    
    # File uploader
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if st.button("Perform UPSERT"):
        if not uploaded_file:
            st.error("Please upload a CSV file.")
            return
        if not table_name or not keys or not columns:
            st.error("Please provide table name, primary keys, and columns.")
            return

        try:
            # Read CSV into DataFrame and convert to List[Dict]
            df = pd.read_csv(uploaded_file)
            data = df.to_dict("records")
            
            # Create TableInfo
            table_info = TableInfo(
                table_name=table_name,
                keys=keys.split(","),
                columns=columns.split(",")
            )
            
            # Initialize CSVAdapter
            profile = DatabaseProfile(connection_string="./data", keys=keys)
            adapter = CSVAdapter(profile)
            
            # Perform UPSERT
            adapter.upsert(table_info, data)
            
            st.success(f"Successfully upserted {len(data)} records into {table_name}.csv")
            
            # Display the resulting CSV content
            result_df = pd.read_csv(os.path.join("./data", f"{table_name}.csv"))
            st.write("Current content of the CSV file:")
            st.dataframe(result_df)
            
        except Exception as e:
            st.error(f"Error during UPSERT: {str(e)}")

if __name__ == "__main__":
    main()
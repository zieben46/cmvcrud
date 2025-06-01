import pandas as pd
from typing import List, Dict, Any, Optional
import logging
import random
import string
import uuid

logger = logging.getLogger(__name__)

def generate_sync_operations(source_df: pd.DataFrame, target_df: pd.DataFrame, key_columns: List[str]) -> List[Dict[str, Any]]:
    """
    Generate synchronization operations (create, update, delete) between source and target DataFrames based on key columns.

    Args:
        source_df: DataFrame containing the source data.
        target_df: DataFrame containing the target data.
        key_columns: List of column names used as keys (e.g., ["key1", "key2"]).

    Returns:
        List of dictionaries, each with an 'operation' key ('create', 'update', 'delete') and record data.
        Example: [{"operation": "create", "key1": 1, "value": "a"}, {"operation": "delete", "key1": 2}, ...]

    Raises:
        ValueError: If key columns are not present in both DataFrames.
    """
    try:
        # Validate inputs
        if not all(col in source_df.columns and col in target_df.columns for col in key_columns):
            raise ValueError(f"Key columns {key_columns} must exist in both source and target DataFrames")

        # Convert key columns to tuples for comparison
        source_key = source_df[key_columns].apply(tuple, axis=1)
        target_key = target_df[key_columns].apply(tuple, axis=1)

        # Inserts: rows in source not in target
        inserts = source_df[~source_key.isin(target_key)].to_dict("records")
        insert_changes = [{"operation": "create", **record} for record in inserts]

        # Updates: rows in both with different non-key values
        merged = source_df.merge(target_df, on=key_columns, how="inner", suffixes=("_source", "_target"))
        update_changes = []
        for _, row in merged.iterrows():
            update_data = {col: row[f"{col}_source"] for col in source_df.columns if col not in key_columns}
            # Check if any non-key column differs
            if any(row[f"{col}_source"] != row[f"{col}_target"] for col in source_df.columns if col not in key_columns):
                update_changes.append({
                    "operation": "update",
                    **{key: row[key] for key in key_columns},
                    **update_data
                })

        # Deletes: rows in target not in source
        deletes = target_df[~target_key.isin(source_key)][key_columns].to_dict("records")
        delete_changes = [{"operation": "delete", **record} for record in deletes]

        # Combine all changes
        changes = insert_changes + update_changes + delete_changes
        logger.debug(f"Generated {len(insert_changes)} create, {len(update_changes)} update, {len(delete_changes)} delete operations for key columns {key_columns}")
        return changes
    except Exception as e:
        logger.error(f"Failed to generate sync operations: {e}")
        raise ValueError(f"Failed to generate sync operations: {e}")

def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, key_columns: List[str], ignore_columns: Optional[List[str]] = None) -> bool:
    """Compare two DataFrames for equality based on key columns, ignoring specified columns.
    
    Args:
        df1: First DataFrame to compare.
        df2: Second DataFrame to compare.
        key_columns: List of column names used as keys for matching rows.
        ignore_columns: Optional list of column names to ignore in comparison (e.g., timestamps).
    
    Returns:
        True if DataFrames are equal for key columns and non-ignored columns, False otherwise.
    
    Raises:
        ValueError: If key columns are not present in both DataFrames.
    """
    try:
        if not all(col in df1.columns and col in df2.columns for col in key_columns):
            raise ValueError(f"Key columns {key_columns} must exist in both DataFrames")

        # Sort by key columns
        df1_sorted = df1.sort_values(by=key_columns).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=key_columns).reset_index(drop=True)

        # Select columns to compare
        compare_columns = [col for col in df1.columns if col not in (ignore_columns or []) and col not in key_columns]
        compare_columns = [col for col in compare_columns if col in df2.columns]

        # Merge on key columns
        merged = df1_sorted.merge(df2_sorted, on=key_columns, how="outer", suffixes=("_df1", "_df2"))
        if len(merged) != len(df1_sorted) or len(merged) != len(df2_sorted):
            logger.debug("DataFrames have different key sets")
            return False

        # Compare non-key columns
        for col in compare_columns:
            if not merged[f"{col}_df1"].equals(merged[f"{col}_df2"]):
                logger.debug(f"Difference found in column {col}")
                return False

        logger.debug("DataFrames are equal for key columns and non-ignored columns")
        return True
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {e}")
        raise ValueError(f"Failed to compare DataFrames: {e}")

def generate_mock_changes(num_records: int, key_columns: List[str], data_columns: List[str], operation_weights: Optional[Dict[str, float]] = None) -> List[Dict]:
    """Generate mock change dictionaries for testing.
    
    Args:
        num_records: Number of change records to generate.
        key_columns: List of column names used as keys.
        data_columns: List of non-key column names for data.
        operation_weights: Optional dictionary of operation weights (e.g., {"create": 0.5, "update": 0.3, "delete": 0.2}).
                          Defaults to equal weights.
    
    Returns:
        List of change dictionaries with 'operation' and data.
    
    Raises:
        ValueError: If num_records is non-positive or columns are empty.
    """
    try:
        if num_records <= 0:
            raise ValueError("num_records must be positive")
        if not key_columns or not data_columns:
            raise ValueError("key_columns and data_columns cannot be empty")

        operations = ["create", "update", "delete"]
        weights = operation_weights or {"create": 1/3, "update": 1/3, "delete": 1/3}
        weights = [weights.get(op, 0) for op in operations]
        if sum(weights) == 0:
            raise ValueError("Operation weights must sum to a positive value")

        changes = []
        for _ in range(num_records):
            operation = random.choices(operations, weights=weights, k=1)[0]
            record = {}
            for col in key_columns:
                record[col] = random.randint(1, 1000)  # Simple integer keys
            for col in data_columns:
                record[col] = ''.join(random.choices(string.ascii_letters, k=10))  # Random string data
            change = {"operation": operation, **record}
            changes.append(change)

        logger.debug(f"Generated {num_records} mock changes: {sum(1 for c in changes if c['operation'] == 'create')} creates, "
                     f"{sum(1 for c in changes if c['operation'] == 'update')} updates, "
                     f"{sum(1 for c in changes if c['operation'] == 'delete')} deletes")
        return changes
    except Exception as e:
        logger.error(f"Failed to generate mock changes: {e}")
        raise ValueError(f"Failed to generate mock changes: {e}")

def validate_changes(changes: List[Dict], expected_operations: Dict[str, int], key_columns: List[str]) -> bool:
    """Validate a list of changes against expected operation counts and primary key presence.
    
    Args:
        changes: List of change dictionaries to validate.
        expected_operations: Dictionary of expected operation counts (e.g., {"create": 2, "update": 1, "delete": 0}).
        key_columns: List of column names used as keys.
    
    Returns:
        True if changes match expected counts and have valid keys, False otherwise.
    
    Raises:
        ValueError: If expected_operations contains invalid operations.
    """
    try:
        valid_operations = {"create", "update", "delete"}
        if not all(op in valid_operations for op in expected_operations):
            raise ValueError(f"Invalid operations in expected_operations: {set(expected_operations) - valid_operations}")

        # Count actual operations
        actual_counts = {"create": 0, "update": 0, "delete": 0}
        for change in changes:
            operation = change.get("operation")
            if operation not in valid_operations:
                logger.debug(f"Invalid operation found: {operation}")
                return False
            actual_counts[operation] += 1

            # Validate primary keys
            if not all(k in change for k in key_columns):
                logger.debug(f"Missing key columns {key_columns} in change: {change}")
                return False

        # Compare counts
        for op in valid_operations:
            if actual_counts[op] != expected_operations.get(op, 0):
                logger.debug(f"Operation {op} count mismatch: expected {expected_operations.get(op, 0)}, got {actual_counts[op]}")
                return False

        logger.debug("Changes validated successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to validate changes: {e}")
        raise ValueError(f"Failed to validate changes: {e}")
    
def compare_saved_tables(
    source_adapter: DatastoreAdapter,
    source_table: str,
    source_schema: Optional[str],
    target_adapter: DatastoreAdapter,
    target_table: str,
    target_schema: Optional[str],
    key_columns: List[str],
    ignore_columns: Optional[List[str]] = None,
    chunk_size: int = 100000
) -> Tuple[bool, List[Dict]]:
    """
    Compare two saved tables for equality based on key columns, streaming data in chunks for memory efficiency.
    
    Args:
        source_adapter: DatastoreAdapter for the source table.
        source_table: Name of the source table.
        source_schema: Schema of the source table (e.g., 'default' for Databricks, 'public' for PostgreSQL).
        target_adapter: DatastoreAdapter for the target table.
        target_table: Name of the target table.
        target_schema: Schema of the target table.
        key_columns: List of column names used as keys for matching rows.
        ignore_columns: Optional list of column names to ignore in comparison (e.g., timestamps).
        chunk_size: Number of records to process per chunk (default: 100000).
    
    Returns:
        Tuple of (bool, List[Dict]):
        - bool: True if tables are equal for key columns and non-ignored columns, False otherwise.
        - List[Dict]: List of mismatch details, each with 'key', 'source_row', 'target_row', 'differences'.
    
    Raises:
        ValueError: If key columns are invalid or tables cannot be accessed.
    """
    try:
        # Validate key columns
        source_columns = source_adapter.get_table_columns(source_table, source_schema)
        target_columns = target_adapter.get_table_columns(target_table, target_schema)
        if not all(col in source_columns and col in target_columns for col in key_columns):
            raise ValueError(f"Key columns {key_columns} must exist in both tables")

        mismatches = []
        is_equal = True

        # Get common columns to compare
        compare_columns = [col for col in source_columns if col in target_columns and col not in key_columns and col not in (ignore_columns or [])]

        # Stream source table
        source_chunks = source_adapter.select_chunks(source_table, chunk_size=chunk_size)
        target_chunks = target_adapter.select_chunks(target_table, chunk_size=chunk_size)

        # Process chunks
        source_key_set = set()
        target_key_set = set()

        for source_chunk in source_chunks:
            source_df = pd.DataFrame(source_chunk)
            if source_df.empty:
                continue
            source_keys = source_df[key_columns].apply(tuple, axis=1)
            source_key_set.update(source_keys)

            # Compare against target chunks
            target_chunks.seek(0)  # Reset target iterator
            for target_chunk in target_chunks:
                target_df = pd.DataFrame(target_chunk)
                if target_df.empty:
                    continue
                target_keys = target_df[key_columns].apply(tuple, axis=1)
                target_key_set.update(target_keys)

                # Merge on key columns
                merged = source_df.merge(target_df, on=key_columns, how="inner", suffixes=("_source", "_target"))
                for _, row in merged.iterrows():
                    key = {k: row[k] for k in key_columns}
                    differences = []
                    for col in compare_columns:
                        source_val = row[f"{col}_source"]
                        target_val = row[f"{col}_target"]
                        if pd.isna(source_val) and pd.isna(target_val):
                            continue
                        if source_val != target_val:
                            differences.append({
                                "column": col,
                                "source_value": source_val,
                                "target_value": target_val
                            })
                    if differences:
                        is_equal = False
                        mismatches.append({
                            "key": key,
                            "source_row": {k: row[f"{k}_source"] for k in key_columns + compare_columns},
                            "target_row": {k: row[f"{k}_target"] for k in key_columns + compare_columns},
                            "differences": differences
                        })

                # Free memory
                del target_df

            # Free memory
            del source_df

        # Check for missing or extra keys
        missing_in_target = source_key_set - target_key_set
        missing_in_source = target_key_set - source_key_set

        if missing_in_target:
            is_equal = False
            for key in missing_in_target:
                mismatches.append({
                    "key": dict(zip(key_columns, key)),
                    "source_row": None,
                    "target_row": None,
                    "differences": [{"column": None, "source_value": "present", "target_value": "missing"}]
                })

        if missing_in_source:
            is_equal = False
            for key in missing_in_source:
                mismatches.append({
                    "key": dict(zip(key_columns, key)),
                    "source_row": None,
                    "target_row": None,
                    "differences": [{"column": None, "source_value": "missing", "target_value": "present"}]
                })

        logger.debug(f"Table comparison completed: equal={is_equal}, {len(mismatches)} mismatches found")
        return is_equal, mismatches

    except Exception as e:
        logger.error(f"Failed to compare saved tables {source_table} and {target_table}: {e}")
        raise ValueError(f"Failed to compare saved tables: {e}")
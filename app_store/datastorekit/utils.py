# datastorekit/utils.py
import pandas as pd
from typing import List, Dict, Any
import logging

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
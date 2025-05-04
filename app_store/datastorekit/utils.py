# datastorekit/utils.py
import pandas as pd
from typing import List, Dict, Any, Tuple

def detect_changes(source_df: pd.DataFrame, target_df: pd.DataFrame, key_columns: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Compute inserts, updates, and deletes between source and target DataFrames based on key columns.

    Args:
        source_df: DataFrame containing the source data.
        target_df: DataFrame containing the target data.
        key_columns: List of column names used as keys (e.g., ["key1", "key2"]).

    Returns:
        Tuple of (inserts, updates, deletes), where each is a list of dictionaries.
    """
    # Validate inputs
    if not all(col in source_df.columns and col in target_df.columns for col in key_columns):
        raise ValueError(f"Key columns {key_columns} must exist in both source and target DataFrames")

    # Convert key columns to tuples for comparison
    source_key = source_df[key_columns].apply(tuple, axis=1)
    target_key = target_df[key_columns].apply(tuple, axis=1)

    # Inserts: rows in source not in target
    inserts = source_df[~source_key.isin(target_key)].to_dict("records")

    # Updates: rows in both with different non-key values
    merged = source_df.merge(target_df, on=key_columns, how="inner", suffixes=("_source", "_target"))
    updates = []
    for _, row in merged.iterrows():
        update_data = {col: row[f"{col}_source"] for col in source_df.columns if col not in key_columns}
        # Check if any non-key column differs
        if any(row[f"{col}_source"] != row[f"{col}_target"] for col in source_df.columns if col not in key_columns):
            updates.append({**{key: row[key] for key in key_columns}, **update_data})

    # Deletes: rows in target not in source
    deletes = target_df[~target_key.isin(source_key)][key_columns].to_dict("records")

    return inserts, updates, deletes
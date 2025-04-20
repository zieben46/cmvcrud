# datastorekit/adapters/csv_adapter.py
import os
import pandas as pd
from datastorekit.adapters.base import DatastoreAdapter
from typing import List, Dict, Any, Iterator
import boto3
import logging

logger = logging.getLogger(__name__)

class CSVAdapter(DatastoreAdapter):
    def __init__(self, profile: DatabaseProfile):
        self.base_dir = profile.connection_string or "./data"
        self.db_type = profile.db_type
        self.s3_client = None
        if self.base_dir.startswith("s3://"):
            self.bucket, self.prefix = self._parse_s3_path(self.base_dir)
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
        else:
            os.makedirs(self.base_dir, exist_ok=True)

    def _parse_s3_path(self, s3_path: str) -> tuple:
        """Parse S3 path into bucket and prefix."""
        path = s3_path.replace("s3://", "")
        parts = path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix

    def _get_file_path(self, table_name: str) -> str:
        """Get the file path for a table."""
        if self.s3_client:
            return f"{self.prefix}/{table_name}.csv" if self.prefix else f"{table_name}.csv"
        return os.path.join(self.base_dir, f"{table_name}.csv")

    def validate_keys(self, table_name: str, table_info_keys: List[str]):
        """Validate that table_info.keys are present in the CSV."""
        file_path = self._get_file_path(table_name)
        if self.s3_client:
            try:
                self.s3_client.head_object(Bucket=self.bucket, Key=file_path)
                # Read first chunk to get columns
                response = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
                df = pd.read_csv(response['Body'], nrows=1)
                columns = set(df.columns)
            except self.s3_client.exceptions.NoSuchKey:
                return
        else:
            if not os.path.exists(file_path):
                return
            df = pd.read_csv(file_path, nrows=1)
            columns = set(df.columns)
        
        if not all(key in columns for key in table_info_keys):
            raise ValueError(
                f"Table {table_name} CSV columns {columns} do not include all TableInfo keys {table_info_keys}"
            )

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        df = pd.DataFrame(data)
        
        if self.s3_client:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
                existing_df = pd.read_csv(response['Body'])
                df = pd.concat([existing_df, df], ignore_index=True)
            except self.s3_client.exceptions.NoSuchKey:
                pass
            csv_buffer = df.to_csv(index=False)
            self.s3_client.put_object(Bucket=self.bucket, Key=file_path, Body=csv_buffer)
        else:
            if os.path.exists(file_path):
                existing_df = pd.read_csv(file_path)
                df = pd.concat([existing_df, df], ignore_index=True)
            df.to_csv(file_path, index=False)

    def select(self, table_name: str, filters: Dict[str, Any], chunk_size: int = 100000) -> Iterator[List[Dict[str, Any]]]:
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        if self.s3_client:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
                for chunk in pd.read_csv(response['Body'], chunksize=chunk_size):
                    for key, value in filters.items():
                        chunk = chunk[chunk[key] == value]
                    yield chunk.to_dict('records')
            except self.s3_client.exceptions.NoSuchKey:
                return
        else:
            if not os.path.exists(file_path):
                return
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                for key, value in filters.items():
                    chunk = chunk[chunk[key] == value]
                yield chunk.to_dict('records')

    def update(self, table_name: str, data: List[Dict[str, Any]], filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        if self.s3_client:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
                df = pd.read_csv(response['Body'])
            except self.s3_client.exceptions.NoSuchKey:
                return
        else:
            if not os.path.exists(file_path):
                return
            df = pd.read_csv(file_path)
        
        mask = True
        for key, value in filters.items():
            mask &= df[key] == value
        for update_data in data:
            for key, value in update_data.items():
                df.loc[mask, key] = value
        
        if self.s3_client:
            csv_buffer = df.to_csv(index=False)
            self.s3_client.put_object(Bucket=self.bucket, Key=file_path, Body=csv_buffer)
        else:
            df.to_csv(file_path, index=False)

    def delete(self, table_name: str, filters: Dict[str, Any]):
        self.validate_keys(table_name, self.table_info.keys.split(",") if hasattr(self, 'table_info') else ["unique_id"])
        file_path = self._get_file_path(table_name)
        if self.s3_client:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=file_path)
                df = pd.read_csv(response['Body'])
            except self.s3_client.exceptions.NoSuchKey:
                return
        else:
            if not os.path.exists(file_path):
                return
            df = pd.read_csv(file_path)
        
        mask = True
        for key, value in filters.items():
            mask &= df[key] == value
        df = df[~mask]
        
        if self.s3_client:
            csv_buffer = df.to_csv(index=False)
            self.s3_client.put_object(Bucket=self.bucket, Key=file_path, Body=csv_buffer)
        else:
            df.to_csv(file_path, index=False)
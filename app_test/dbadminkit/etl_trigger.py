import requests
from typing import Dict, Any

class ETLTrigger:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def trigger_job(self, job_name: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = f"{self.endpoint}/{job_name}"
        payload = params or {}
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to trigger ETL job '{job_name}': {str(e)}") from e
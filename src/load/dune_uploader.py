"""
Dune Analytics Uploader - Load Layer

Handles uploading data to Dune Analytics.
Pure I/O operations for external system integration.
"""

import requests
import json
import os
from typing import Dict, Any, Optional
from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""

    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


class DuneUploader:
    """Pure I/O operations for Dune Analytics API"""

    def __init__(self, api_key: Optional[str] = None, namespace: str = "uniswap_fnd"):
        self.api_key = api_key or os.getenv("DUNE_API_KEY")
        if not self.api_key:
            raise ValueError("DUNE_API_KEY environment variable is not set")

        self.namespace = namespace
        self.base_url = "https://api.dune.com/api/v1"
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with headers"""
        session = requests.Session()
        session.headers.update(
            {"X-Dune-API-Key": self.api_key, "Content-Type": "application/json"}
        )
        return session

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """
        Create table in Dune Analytics

        Args:
            table_name: Name of table to create
            schema: Table schema definition

        Returns:
            bool: True if successful
        """
        logger.info(f"Creating table: {table_name}")

        url = f"{self.base_url}/table/{self.namespace}/{table_name}/create"

        payload = {"table_name": table_name, "schema": schema}

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()

            logger.info(
                f"✅ Table created successfully: dune.{self.namespace}.{table_name}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to create table {table_name}: {e}")
            raise

    def clear_table(self, table_name: str) -> bool:
        """
        Clear all data from table

        Args:
            table_name: Name of table to clear

        Returns:
            bool: True if successful
        """
        logger.info(f"Clearing table: {table_name}")

        url = f"{self.base_url}/table/{self.namespace}/{table_name}/clear"

        try:
            response = self.session.post(url)
            response.raise_for_status()

            logger.info(f"✅ Cleared table: {table_name} cleared successfully.")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to clear table {table_name}: {e}")
            raise

    def upload_data(self, table_name: str, data: list) -> bool:
        """
        Upload data to table

        Args:
            table_name: Name of table to upload to
            data: List of records to upload

        Returns:
            bool: True if successful
        """
        logger.info(f"Uploading {len(data)} rows to table: {table_name}")

        # Clear table first for idempotency
        self.clear_table(table_name)

        url = f"{self.base_url}/table/{self.namespace}/{table_name}/insert"

        # Prepare data for Dune (convert lists to JSON strings)
        prepared_data = self._prepare_data_for_dune(data)

        payload = {"data": prepared_data}

        try:
            response = self.session.post(url, json=payload, cls=DateTimeEncoder)
            response.raise_for_status()

            logger.info(
                f"✅ Uploaded {len(data)} rows successfully to table: {table_name}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to upload data to {table_name}: {e}")
            raise

    def _prepare_data_for_dune(self, data: list) -> list:
        """
        Prepare data for Dune upload by converting lists to JSON strings

        Args:
            data: Raw data list

        Returns:
            list: Prepared data for Dune
        """
        prepared = []

        for record in data:
            prepared_record = {}
            for key, value in record.items():
                if isinstance(value, list):
                    # Convert lists to JSON strings for Dune
                    prepared_record[key] = json.dumps(value)
                else:
                    prepared_record[key] = value
            prepared.append(prepared_record)

        return prepared

    def delete_table(self, table_name: str) -> bool:
        """
        Delete table from Dune Analytics

        Args:
            table_name: Name of table to delete

        Returns:
            bool: True if successful
        """
        logger.info(f"Deleting table: {table_name}")

        url = f"{self.base_url}/table/{self.namespace}/{table_name}/delete"

        try:
            response = self.session.delete(url)
            response.raise_for_status()

            logger.info(f"✅ Deleted table: {table_name}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to delete table {table_name}: {e}")
            raise

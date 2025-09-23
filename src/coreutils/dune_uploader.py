import requests
import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from src.coreutils.logging import setup_logging


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle date and datetime objects"""

    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


from src.datasources.defillama.yieldpools.schemas import (
    CURRENT_STATE_SCHEMA,
    HISTORICAL_TVL_SCHEMA,
    POOL_DIM_SCD2_SCHEMA,
    HISTORICAL_FACTS_SCHEMA,
)
import polars as pl


class DuneUploader:
    """Handles Dune table creation and data uploads"""

    def __init__(self, namespace: str = "uniswap_fnd"):
        self.api_key = os.getenv("DUNE_API_KEY")
        self.base_url = "https://api.dune.com/api/v1"
        self.namespace = namespace
        self.logger = setup_logging()

        if not self.api_key:
            raise ValueError("DUNE_API_KEY environment variable is not set")

    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for Dune API requests"""
        return {
            "X-Dune-Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def _polars_to_dune_schema(self, polars_schema: pl.Schema) -> List[Dict[str, Any]]:
        """Convert Polars schema to Dune API schema format"""
        dune_schema = []

        for field_name, field_type in polars_schema.items():
            # Map Polars types to Dune types
            if field_type == pl.String():
                dune_type = "varchar"
            elif field_type == pl.Float64():
                dune_type = "double"
            elif field_type == pl.Int64():
                dune_type = "bigint"
            elif field_type == pl.Boolean():
                dune_type = "boolean"
            elif field_type == pl.Date():
                dune_type = "date"
            elif field_type == pl.Datetime():
                dune_type = "timestamp"
            elif field_type == pl.List(pl.String()):
                dune_type = "varchar"  # Store as JSON string
            else:
                dune_type = "varchar"  # Default fallback

            # Determine if nullable (all our fields are nullable by default)
            nullable = True

            dune_schema.append(
                {"name": field_name, "type": dune_type, "nullable": nullable}
            )

        return dune_schema

    def _prepare_data_for_dune(self, data: List[Dict]) -> List[Dict]:
        """Prepare data for Dune upload by converting list fields to JSON strings"""
        prepared_data = []

        for row in data:
            prepared_row = {}
            for key, value in row.items():
                if isinstance(value, list):
                    # convert list to JSON string for Dune
                    prepared_row[key] = json.dumps(value) if value else None
                else:
                    prepared_row[key] = value
            prepared_data.append(prepared_row)

        return prepared_data

    def create_table(
        self,
        table_name: str,
        schema: List[Dict],
        description: str,
        is_private: bool = True,
    ) -> Dict[str, Any]:
        """Create a new table in Dune"""
        self.logger.info(f"Creating table: {table_name}")

        url = f"{self.base_url}/table/create"

        payload = {
            "namespace": self.namespace,
            "table_name": table_name,
            "description": description,
            "is_private": is_private,
            "schema": schema,
        }

        try:
            response = requests.post(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()

            result = response.json()
            self.logger.info(f"✅ Table created successfully: {result['full_name']}")
            return result

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Failed to create table {table_name}: {e}")
            raise

    def create_dimension_table(self) -> Dict[str, Any]:
        """Create the dimension table (current_state) using CURRENT_STATE_SCHEMA"""
        schema = self._polars_to_dune_schema(CURRENT_STATE_SCHEMA)

        return self.create_table(
            table_name="defillama_current_state",
            schema=schema,
            description="Current state of Defillama yield pools - updated weekly",
        )

    def create_scd2_dimension_table(self) -> Dict[str, Any]:
        """Create the SCD2 dimension table (pool_dim_scd2) using POOL_DIM_SCD2_SCHEMA"""
        from src.datasources.defillama.yieldpools.schemas import POOL_DIM_SCD2_SCHEMA

        schema = self._polars_to_dune_schema(POOL_DIM_SCD2_SCHEMA)
        return self.create_table(
            table_name="pool_dim_scd2",
            schema=schema,
            description="SCD2 dimension table for DeFiLlama yield pools - updated weekly",
        )

    def create_fact_table(self) -> Dict[str, Any]:
        """Create the fact table (tvl_data) using HISTORICAL_TVL_SCHEMA"""
        schema = self._polars_to_dune_schema(HISTORICAL_TVL_SCHEMA)

        return self.create_table(
            table_name="defillama_tvl_data",
            schema=schema,
            description="Historical TVL and APY data - updated daily",
        )

    def create_historical_facts_table(self) -> Dict[str, Any]:
        """Create the historical facts table (defillama_historical_facts) using HISTORICAL_FACTS_SCHEMA"""
        from src.datasources.defillama.yieldpools.schemas import HISTORICAL_FACTS_SCHEMA

        schema = self._polars_to_dune_schema(HISTORICAL_FACTS_SCHEMA)
        return self.create_table(
            table_name="defillama_historical_facts",
            schema=schema,
            description="Historical facts table with SCD2 dimensions for DeFiLlama yield pools - updated daily",
        )

    def upload_data(
        self,
        table_name: str,
        data: List[Dict],
        content_type: str = "application/x-ndjson",
    ) -> Dict[str, Any]:
        """Upload data to Dune table"""
        self.logger.info(f"Uploading {len(data)} rows to table: {table_name}")

        # Clear table first to ensure full refresh
        self.logger.info(f"Clearing table {table_name} before upload...")
        self.clear_table(table_name)

        url = f"{self.base_url}/table/{self.namespace}/{table_name}/insert"

        # Prepare data for Dune (convert lists to JSON strings)
        prepared_data = self._prepare_data_for_dune(data)
        ndjson_data = "\n".join(
            json.dumps(row, cls=DateTimeEncoder) for row in prepared_data
        )

        headers = {"X-DUNE-API-KEY": self.api_key, "Content-Type": content_type}

        try:
            response = requests.post(url, headers=headers, data=ndjson_data)
            response.raise_for_status()

            result = response.json()
            self.logger.info(
                f"✅ Uploaded {result['rows_written']} rows successfully to table: {table_name}"
            )
            return result

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Failed to upload data to table {table_name}: {e}")
            raise

    def upload_dimension_data(self, current_state_data) -> Dict[str, Any]:
        """Upload current state (dimension) data to Dune"""
        self.logger.info("Uploading dimension data (current_state)")

        # convert polars dataframe to list of dicts
        if hasattr(current_state_data, "to_dicts"):
            data = current_state_data.to_dicts()
        else:
            data = current_state_data

        # converrt list to JSON string
        prepared_data = self._prepare_data_for_dune(data)

        return self.upload_data(
            table_name="defillama_current_state", data=prepared_data
        )

    def upload_scd2_dimension_data(self, scd2_df: pl.DataFrame) -> Dict[str, Any]:
        """Upload SCD2 dimension data to Dune"""
        self.logger.info("Uploading SCD2 dimension data (pool_dim_scd2)")
        data = scd2_df.to_dicts() if hasattr(scd2_df, "to_dicts") else scd2_df
        prepared_data = self._prepare_data_for_dune(data)
        return self.upload_data("pool_dim_scd2", prepared_data)

    def upload_fact_data(self, tvl_data) -> Dict[str, Any]:
        """Upload historical TVL (fact) data to Dune"""
        self.logger.info("Uploading fact data (tvl_data)")

        # convert polars dataframe to list of dicts
        if hasattr(tvl_data, "to_dicts"):
            data = tvl_data.to_dicts()
        else:
            data = tvl_data

        # convert list to JSON string
        prepared_data = self._prepare_data_for_dune(data)

        return self.upload_data(table_name="defillama_tvl_data", data=prepared_data)

    def upload_historical_facts_data(
        self, historical_facts_df: pl.DataFrame
    ) -> Dict[str, Any]:
        """Upload historical facts data to Dune"""
        self.logger.info("Uploading historical facts data (defillama_historical_facts)")
        data = (
            historical_facts_df.to_dicts()
            if hasattr(historical_facts_df, "to_dicts")
            else historical_facts_df
        )
        prepared_data = self._prepare_data_for_dune(data)
        return self.upload_data(
            table_name="defillama_historical_facts", data=prepared_data
        )

    def clear_table(self, table_name: str) -> Dict[str, Any]:
        """Clear all data from a table"""
        self.logger.info(f"Clearing table: {table_name}")

        url = f"{self.base_url}/table/{self.namespace}/{table_name}/clear"

        try:
            response = requests.post(url, headers=self._get_headers())
            response.raise_for_status()

            result = response.json()
            self.logger.info(f"✅ Cleared table: {table_name} cleared successfully.")
            return result

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Failed to clear table {table_name}: {e}")
            raise

    def delete_table(self, table_name: str) -> Dict[str, Any]:
        """Delete a table"""
        self.logger.info(f"Deleting table: {table_name}")

        url = f"{self.base_url}/table/{self.namespace}/{table_name}"

        try:
            response = requests.delete(url, headers=self._get_headers())
            response.raise_for_status()

            result = response.json()
            self.logger.info(f"✅ Deleted table: {table_name} successfully.")
            return result

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Failed to delete table {table_name}: {e}")
            raise

    def upload_daily_metrics_partition(
        self, daily_metrics_df: pl.DataFrame, date_range: tuple
    ) -> Dict[str, Any]:
        """Upload daily metrics partition to Dune"""
        start_date, end_date = date_range
        self.logger.info(
            f"Uploading daily metrics partition for {start_date} to {end_date}"
        )

        # convert to list of dicts
        data = daily_metrics_df.to_dicts()

        # upload to Dune
        return self.upload_data(table_name="defillama_historical_facts", data=data)

    def delete_daily_metrics_partition(self, date_range: tuple) -> Dict[str, Any]:
        """Delete daily metrics partition from Dune"""
        start_date, end_date = date_range
        self.logger.info(
            f"Deleting daily metrics partition for {start_date} to {end_date}"
        )

        # This would require a DELETE query in Dune
        # for now, we'll clear and re-upload

        return self.clear_table(table_name="defillama_historical_facts")

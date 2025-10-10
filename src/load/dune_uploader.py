"""
Dune Analytics Uploader - Simplified Load Layer

Handles uploading historical facts data to Dune Analytics.
Focuses only on facts table with daily upsert functionality.
"""

import requests
import json
import os
import time
from typing import Dict, Any, Optional, List
from datetime import date, datetime
import polars as pl
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""

    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


class DuneUploader:
    """Simplified Dune uploader for historical facts only"""

    def __init__(
        self,
        api_key: Optional[str] = None,
        namespace: str = "uniswap_fnd",
    ):
        self.api_key = api_key or os.getenv("DUNE_API_KEY")
        if not self.api_key:
            raise ValueError("DUNE_API_KEY environment variable is not set")

        self.namespace = namespace
        self.facts_table = "prod_defillama_historical_facts"
        self.base_url = "https://api.dune.com/api/v1"
        self.session = self._create_session()

        # Initialize Dune Client for SDK-based operations
        try:
            from dune_client.client import DuneClient

            self.dune_client = DuneClient.from_env()
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize Dune Client: {e}")
            self.dune_client = None

    def _create_session(self) -> requests.Session:
        """Create HTTP session with enhanced retry logic for daily pipeline reliability"""
        session = requests.Session()

        # Enhanced retry strategy for daily pipeline
        retry_strategy = Retry(
            total=5,  # Increased retries for daily pipeline reliability
            backoff_factor=2,  # Exponential backoff
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on server errors
            allowed_methods=[
                "HEAD",
                "GET",
                "POST",
                "PUT",
                "DELETE",
                "OPTIONS",
                "TRACE",
            ],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update(
            {"X-Dune-API-Key": self.api_key, "Content-Type": "application/json"}
        )
        return session

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
            elif field_type == pl.Binary():
                dune_type = "varbinary"  # Binary data type for pool_id
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

    def create_historical_facts_table(self) -> bool:
        """
        Create the historical facts table using HISTORICAL_FACTS_SCHEMA (legacy method for compatibility)

        Returns:
            bool: True if successful
        """
        from src.transformation.schemas import HISTORICAL_FACTS_SCHEMA

        logger.info(f"Creating historical facts table: {self.facts_table}")

        url = f"{self.base_url}/table/create"

        # Convert Polars schema to Dune format
        schema = self._polars_to_dune_schema(HISTORICAL_FACTS_SCHEMA)

        payload = {
            "namespace": self.namespace,
            "table_name": self.facts_table,
            "description": "Historical facts table for DeFiLlama yield pools - updated daily",
            "is_private": True,
            "schema": schema,
        }

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()

            result = response.json()
            logger.info(
                f"✅ Historical facts table created successfully: {result['full_name']}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to create historical facts table: {e}")
            raise

    def upload_full_historical_facts(self, facts_df: pl.DataFrame) -> bool:
        """
        Upload full historical facts dataset (initial load)

        Args:
            facts_df: Complete historical facts DataFrame

        Returns:
            bool: True if successful
        """
        logger.info(f"Uploading full historical facts: {facts_df.height} records")

        # Create table if it doesn't exist
        self.create_historical_facts_table()

        # Convert DataFrame to list of records
        data = facts_df.to_dicts()

        # Upload data using NDJSON format
        return self._upload_data_to_table_ndjson(data)

    def _upload_data_to_table_append(self, data: List[Dict[str, Any]]) -> bool:
        """
        Upload data to facts table without clearing (for appending)

        Args:
            data: List of records to upload

        Returns:
            bool: True if successful
        """
        logger.info(f"Appending {len(data)} rows to facts table")

        url = f"{self.base_url}/table/{self.namespace}/{self.facts_table}/insert"

        # Convert to NDJSON format, handling binary data
        processed_data = []
        for row in data:
            processed_row = row.copy()
            # Convert binary pool_id to hex for JSON serialization
            if "pool_id" in processed_row and isinstance(
                processed_row["pool_id"], bytes
            ):
                processed_row["pool_id"] = "0x" + processed_row["pool_id"].hex()
            processed_data.append(processed_row)

        ndjson_data = "\n".join(
            json.dumps(row, cls=DateTimeEncoder) for row in processed_data
        )

        headers = {
            "X-DUNE-API-KEY": self.api_key,
            "Content-Type": "application/x-ndjson",
        }

        try:
            response = self.session.post(url, data=ndjson_data, headers=headers)
            response.raise_for_status()

            logger.info(
                f"✅ Successfully appended {len(data)} rows to {self.facts_table}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to append data to {self.facts_table}: {e}")
            logger.error(f"❌ Response status: {response.status_code}")
            logger.error(f"❌ Response text: {response.text}")
            raise

    def _create_upload_record(self, target_date: date, record_count: int) -> None:
        """Create upload record for successful upload"""
        try:
            # Ensure cache directory exists
            os.makedirs("output/cache", exist_ok=True)

            date_str = target_date.strftime("%Y-%m-%d")
            upload_record = f"output/cache/uploaded_{date_str}.txt"

            with open(upload_record, "w") as f:
                f.write(
                    f"Uploaded {record_count} records for {target_date} at {datetime.now()}"
                )

            logger.info(f"✅ Created upload record: {upload_record}")
        except Exception as e:
            logger.warning(f"Could not create upload record: {e}")

    def append_daily_facts(self, facts_df: pl.DataFrame, target_date: date) -> bool:
        """
        Append daily facts data with enhanced duplicate detection and error handling

        Args:
            facts_df: Historical facts DataFrame
            target_date: Target date for append

        Returns:
            bool: True if successful
        """
        logger.info(f"🔄 Appending daily facts for date: {target_date}")

        # Filter data for target date
        daily_data = facts_df.filter(pl.col("timestamp") == target_date)

        if daily_data.height == 0:
            logger.warning(f"⚠️ No data found for date: {target_date}")
            return True

        logger.info(f"📊 Found {daily_data.height} records for {target_date}")

        # Enhanced duplicate detection
        try:
            if self._data_exists_for_date(target_date):
                logger.info(
                    f"✅ Data for {target_date} already exists, skipping append"
                )
                return True
        except Exception as e:
            logger.warning(f"⚠️ Could not check for existing data: {e}")
            logger.info("🔄 Proceeding with append (may create duplicates)")

        # Validate data quality before upload
        if not self._validate_daily_data_quality(daily_data, target_date):
            logger.error(f"❌ Data quality validation failed for {target_date}")
            return False

        # Append new data for target date (don't clear table)
        try:
            data = daily_data.to_dicts()
            success = self._upload_data_to_table_append(data)

            if success:
                self._create_upload_record(target_date, daily_data.height)
                logger.info(
                    f"✅ Successfully appended {daily_data.height} records for {target_date}"
                )
            else:
                logger.error(f"❌ Failed to append data for {target_date}")

            return success

        except Exception as e:
            logger.error(f"❌ Error during append for {target_date}: {e}")
            raise

    def _validate_daily_data_quality(
        self, daily_data: pl.DataFrame, target_date: date
    ) -> bool:
        """
        Validate data quality for daily append

        Args:
            daily_data: DataFrame to validate
            target_date: Expected date for validation

        Returns:
            bool: True if data quality is acceptable
        """
        try:
            # Check row count (should be reasonable for daily data)
            row_count = daily_data.height
            if row_count < 100:
                logger.warning(f"⚠️ Low row count for daily data: {row_count}")
            elif row_count > 10000:
                logger.warning(f"⚠️ High row count for daily data: {row_count}")

            # Check for null values in critical columns
            null_counts = daily_data.null_count()
            critical_columns = ["pool_id", "pool_id_defillama", "timestamp", "tvl_usd"]

            for col in critical_columns:
                if col in null_counts.columns and null_counts[col].item() > 0:
                    logger.error(
                        f"❌ Found {null_counts[col].item()} null values in {col}"
                    )
                    return False

            # Check that all timestamps match target date
            unique_dates = daily_data.select("timestamp").unique().to_series().to_list()
            if len(unique_dates) != 1 or unique_dates[0] != target_date:
                logger.error(
                    f"❌ Date mismatch: expected {target_date}, got {unique_dates}"
                )
                return False

            # Check for duplicate records
            duplicate_count = daily_data.height - daily_data.unique().height
            if duplicate_count > 0:
                logger.error(
                    f"❌ Found {duplicate_count} duplicate records - upload rejected"
                )
                return False

            logger.info(f"✅ Data quality validation passed for {target_date}")
            return True

        except Exception as e:
            logger.error(f"❌ Data quality validation error: {e}")
            return False

    def _execute_duplicate_detection_query(self, target_date: date) -> bool:
        """
        Execute a pre-created query to check if data exists for a specific date using Dune Client SDK

        Args:
            target_date: Date to check

        Returns:
            bool: True if data exists for this date
        """
        try:
            from dune_client.query import QueryBase, QueryParameter

            query_id = self._get_duplicate_detection_query_id()

            if not query_id:
                logger.warning("⚠️ No duplicate detection query configured")
                return False

            # Set up query parameters with dynamic date
            date_str = target_date.strftime("%Y-%m-%d")

            logger.info(f"🔍 Dynamic date parameter: {date_str}")
            logger.info(f"🔍 Target date object: {target_date}")
            logger.info(f"🔍 Date format: YYYY-MM-DD")

            # Create QueryBase with proper QueryParameter using Dune Client SDK
            query = QueryBase(
                name="DeFiLlama Duplicate Detection",
                query_id=query_id,
                params=[QueryParameter.text_type(name="date", value=date_str)],
            )

            logger.info(f"🔍 Executing duplicate detection query for {target_date}")
            logger.info(f"🔍 Query object: {query}")
            logger.info(f"🔍 Query URL: {query.url()}")

            # Execute using Dune Client SDK
            results = self.dune_client.run_query(query)

            logger.info(f"🔍 Query execution completed")
            logger.info(f"🔍 Results state: {results.state}")

            if results.state.value == "QUERY_STATE_COMPLETED":
                # Get the row count from results
                rows = results.get_rows()
                logger.info(f"🔍 Query rows: {rows}")

                if rows and len(rows) > 0:
                    # Handle both dictionary and list formats
                    if isinstance(rows[0], dict):
                        row_count = rows[0].get("row_count", 0)
                    else:
                        row_count = rows[0][0] if rows[0] else 0
                    logger.info(f"🔍 Row count: {row_count}")

                    exists = row_count > 0

                    if exists:
                        logger.info(
                            f"✅ Found {row_count} existing records for {target_date}"
                        )
                    else:
                        logger.info(f"✅ No existing records for {target_date}")

                    return exists
                else:
                    logger.info(f"✅ No rows in results for {target_date}")
                    return False
            else:
                logger.warning(f"⚠️ Query execution failed with state: {results.state}")
                return None  # Return None to trigger fallback to file-based detection

        except Exception as e:
            logger.warning(f"⚠️ Could not execute duplicate detection query: {e}")
            return None  # Return None to trigger fallback to file-based detection

    def _get_duplicate_detection_query_id(self) -> Optional[int]:
        """
        Get the query ID for duplicate detection query

        Returns:
            int: Query ID if configured, None otherwise
        """
        # Query ID for DeFiLlama Duplicate Detection query
        # Created via scripts/create_duplicate_detection_query.py
        # Query URL: https://dune.com/queries/5917236
        #
        # For production
        # return int(os.getenv("DUNE_DUPLICATE_DETECTION_QUERY_ID", "5917236"))
        return 5917236

    def _get_query_results(self, execution_id: str, target_date: date) -> bool:
        """
        Get results from query execution to determine if data exists

        Args:
            execution_id: Execution ID from query execution
            target_date: Date being checked

        Returns:
            bool: True if data exists
        """
        try:
            # Wait for query completion with timeout
            max_attempts = 10  # 10 seconds max wait
            attempt = 0

            while attempt < max_attempts:
                # Get execution status
                status_url = f"{self.base_url}/execution/{execution_id}/status"
                response = self.session.get(status_url)
                response.raise_for_status()

                status = response.json()
                state = status.get("state")

                if state == "QUERY_STATE_COMPLETE":
                    logger.info(f"✅ Query completed successfully")
                    break
                elif state == "QUERY_STATE_FAILED":
                    logger.warning(f"⚠️ Query failed: {status.get('error')}")
                    return None  # Return None to trigger fallback
                elif state == "QUERY_STATE_PENDING":
                    logger.info(
                        f"⏳ Query still pending, waiting... (attempt {attempt + 1}/{max_attempts})"
                    )
                    time.sleep(1)  # Wait 1 second before retry
                    attempt += 1
                else:
                    logger.warning(f"⚠️ Unknown query state: {state}")
                    return None  # Return None to trigger fallback

            if attempt >= max_attempts:
                logger.warning(f"⚠️ Query timeout after {max_attempts} seconds")
                return None  # Return None to trigger fallback

            # Get results
            results_url = f"{self.base_url}/execution/{execution_id}/results"
            response = self.session.get(results_url)
            response.raise_for_status()

            results = response.json()
            logger.info(f"🔍 Query results: {results}")

            rows = results.get("result", {}).get("rows", [])
            logger.info(f"🔍 Query rows: {rows}")

            if not rows:
                logger.info(f"✅ No data found for {target_date}")
                return False

            # Check if any rows exist (row count > 0)
            row_count = rows[0][0] if rows else 0
            exists = row_count > 0

            if exists:
                logger.info(f"✅ Found {row_count} existing records for {target_date}")
            else:
                logger.info(f"✅ No existing records for {target_date}")

            return exists

        except Exception as e:
            logger.warning(f"⚠️ Could not get query results: {e}")
            return False

    def _data_exists_for_date(self, target_date: date) -> bool:
        """
        Check if data already exists for a specific date

        Uses different strategies based on environment:
        - GitHub Actions: Dune Client SDK only (ephemeral environment)
        - Local runs: File-based detection first, then Dune Client SDK fallback

        Args:
            target_date: Date to check

        Returns:
            bool: True if data exists for this date
        """
        # Check if running in GitHub Actions
        is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"

        if is_github_actions:
            # GitHub Actions: Use Dune Client SDK only
            logger.info(
                f"🔍 GitHub Actions detected - using Dune Client SDK for {target_date}"
            )
            try:
                result = self._execute_duplicate_detection_query(target_date)
                if result is not None:
                    return result
                else:
                    logger.warning(
                        f"⚠️ Dune query failed, assuming no data exists for {target_date}"
                    )
                    return False
            except Exception as e:
                logger.warning(f"⚠️ Could not check for existing data: {e}")
                return False
        else:
            # Local runs: Use file-based detection first, then Dune Client SDK fallback
            logger.info(
                f"🔍 Local run detected - using file-based detection for {target_date}"
            )
            try:
                # Try file-based detection first (faster for local runs)
                file_result = self._data_exists_for_date_file_based(target_date)
                if file_result:
                    return True

                # If no file found, try Dune Client SDK as fallback
                logger.info(
                    f"🔄 No local file found, trying Dune Client SDK for {target_date}"
                )
                dune_result = self._execute_duplicate_detection_query(target_date)
                if dune_result is not None:
                    return dune_result
                else:
                    logger.warning(
                        f"⚠️ Dune query failed, assuming no data exists for {target_date}"
                    )
                    return False

            except Exception as e:
                logger.warning(f"⚠️ Could not check for existing data: {e}")
                return False

    def _data_exists_for_date_file_based(self, target_date: date) -> bool:
        """
        Check if data already exists using file-based tracking (local runs only)

        This method is used for local development and testing where files persist.
        Not used in GitHub Actions due to ephemeral nature.

        Args:
            target_date: Date to check

        Returns:
            bool: True if data exists for this date
        """
        try:
            # Check for upload record file
            date_str = target_date.strftime("%Y-%m-%d")
            upload_record = f"output/cache/uploaded_{date_str}.txt"

            if os.path.exists(upload_record):
                logger.info(
                    f"✅ Found upload record for {target_date}: {upload_record}"
                )
                return True
            else:
                logger.info(f"✅ No upload record found for {target_date}")
                return False

        except Exception as e:
            logger.warning(f"⚠️ Could not check for existing data: {e}")
            return False

    def clear_table(self) -> bool:
        """
        Clear all data from the facts table (matching working version)

        Returns:
            bool: True if successful
        """
        logger.info(f"Clearing table: {self.facts_table}")

        url = f"{self.base_url}/table/{self.namespace}/{self.facts_table}/clear"

        try:
            response = self.session.post(url)
            response.raise_for_status()

            result = response.json()
            logger.info(f"✅ Cleared table: {self.facts_table} cleared successfully.")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to clear table {self.facts_table}: {e}")
            raise

    def _upload_data_to_table_ndjson(self, data: List[Dict[str, Any]]) -> bool:
        """
        Upload data to facts table using NDJSON format (matching working version)

        Args:
            data: List of records to upload

        Returns:
            bool: True if successful
        """
        logger.info(f"Uploading {len(data)} rows to facts table using NDJSON format")

        url = f"{self.base_url}/table/{self.namespace}/{self.facts_table}/insert"

        # Convert to NDJSON format, handling binary data
        processed_data = []
        for row in data:
            processed_row = row.copy()
            # Convert binary pool_id to hex for JSON serialization
            if "pool_id" in processed_row and isinstance(
                processed_row["pool_id"], bytes
            ):
                processed_row["pool_id"] = "0x" + processed_row["pool_id"].hex()
            processed_data.append(processed_row)

        ndjson_data = "\n".join(
            json.dumps(row, cls=DateTimeEncoder) for row in processed_data
        )

        headers = {
            "X-DUNE-API-KEY": self.api_key,
            "Content-Type": "application/x-ndjson",
        }

        try:
            response = requests.post(url, headers=headers, data=ndjson_data)
            response.raise_for_status()

            result = response.json()
            logger.info(
                f"✅ Uploaded {result['rows_written']} rows successfully to table: {self.facts_table}"
            )
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to upload data to table {self.facts_table}: {e}")
            raise

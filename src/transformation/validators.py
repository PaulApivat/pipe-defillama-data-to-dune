"""
Data Validators - Transform Layer

Pure functions for validating data transformations.
Ensures data quality and schema compliance.
"""

import polars as pl
from typing import Dict, Any
from .schemas import (
    CURRENT_STATE_SCHEMA,
    HISTORICAL_TVL_SCHEMA,
    POOL_DIM_SCD2_SCHEMA,
    HISTORICAL_FACTS_SCHEMA,
)
import logging

logger = logging.getLogger(__name__)


def validate_current_state_schema(df: pl.DataFrame) -> bool:
    """
    Validate current state data matches expected schema

    Args:
        df: Current state DataFrame

    Returns:
        bool: True if valid, raises exception if invalid
    """
    if df.schema != CURRENT_STATE_SCHEMA:
        raise ValueError(
            f"Schema mismatch: expected {CURRENT_STATE_SCHEMA}, got {df.schema}"
        )

    # Check for null values in required fields
    required_fields = ["pool", "protocol_slug", "chain", "symbol"]
    for field in required_fields:
        null_count = df.select(pl.col(field).is_null().sum()).item()
        if null_count > 0:
            raise ValueError(
                f"Null values found in required field '{field}': {null_count}"
            )

    logger.info(f"Current state validation passed: {df.height} records")
    return True


def validate_historical_tvl_schema(df: pl.DataFrame) -> bool:
    """
    Validate historical TVL data matches expected schema

    Args:
        df: Historical TVL DataFrame

    Returns:
        bool: True if valid, raises exception if invalid
    """
    if df.schema != HISTORICAL_TVL_SCHEMA:
        raise ValueError(
            f"Schema mismatch: expected {HISTORICAL_TVL_SCHEMA}, got {df.schema}"
        )

    # Check for null values in required fields
    required_fields = ["timestamp", "pool_id", "tvl_usd", "apy"]
    for field in required_fields:
        null_count = df.select(pl.col(field).is_null().sum()).item()
        if null_count > 0:
            raise ValueError(
                f"Null values found in required field '{field}': {null_count}"
            )

    # Check numeric fields are non-negative
    numeric_fields = ["tvl_usd", "apy", "apy_base", "apy_reward"]
    for field in numeric_fields:
        negative_count = df.select((pl.col(field) < 0).sum()).item()
        if negative_count > 0:
            logger.warning(f"Negative values found in '{field}': {negative_count}")

    logger.info(f"Historical TVL validation passed: {df.height} records")
    return True


def validate_scd2_schema(df: pl.DataFrame) -> bool:
    """
    Validate SCD2 data matches expected schema

    Args:
        df: SCD2 DataFrame

    Returns:
        bool: True if valid, raises exception if invalid
    """
    if df.schema != POOL_DIM_SCD2_SCHEMA:
        raise ValueError(
            f"Schema mismatch: expected {POOL_DIM_SCD2_SCHEMA}, got {df.schema}"
        )

    # Check SCD2 specific validations
    # All records should be current
    is_current_count = df.select(pl.col("is_current").sum()).item()
    if is_current_count != df.height:
        raise ValueError(f"Not all records are current: {is_current_count}/{df.height}")

    # valid_from should be consistent
    valid_from_values = df.select(pl.col("valid_from").unique()).to_series().to_list()
    if len(valid_from_values) > 1:
        raise ValueError(f"Multiple valid_from values found: {valid_from_values}")

    logger.info(f"SCD2 validation passed: {df.height} records")
    return True


def validate_historical_facts_schema(df: pl.DataFrame) -> bool:
    """
    Validate historical facts data matches expected schema

    Args:
        df: Historical facts DataFrame

    Returns:
        bool: True if valid, raises exception if invalid
    """
    if df.schema != HISTORICAL_FACTS_SCHEMA:
        raise ValueError(
            f"Schema mismatch: expected {HISTORICAL_FACTS_SCHEMA}, got {df.schema}"
        )

    # Check for null values in key fields
    key_fields = ["protocol_slug", "chain", "symbol", "pool_id"]
    for field in key_fields:
        null_count = df.select(pl.col(field).is_null().sum()).item()
        if null_count > 0:
            raise ValueError(f"Null values found in key field '{field}': {null_count}")

    # Check date consistency
    timestamps = df.select(pl.col("timestamp")).to_series().to_list()
    if not timestamps:
        raise ValueError("No timestamps found in historical facts")

    logger.info(f"Historical facts validation passed: {df.height} records")
    return True


def validate_data_quality(df: pl.DataFrame, data_type: str) -> Dict[str, Any]:
    """
    Comprehensive data quality validation

    Args:
        df: DataFrame to validate
        data_type: Type of data for specific validations

    Returns:
        Dict: Validation results and statistics
    """
    results = {
        "record_count": df.height,
        "column_count": len(df.columns),
        "data_type": data_type,
        "issues": [],
    }

    # Check for empty DataFrame
    if df.height == 0:
        results["issues"].append("Empty DataFrame")
        return results

    # Check for duplicate records
    if data_type in ["current_state", "scd2"]:
        duplicate_count = df.height - df.unique().height
        if duplicate_count > 0:
            results["issues"].append(f"Duplicate records: {duplicate_count}")

    # Check for null values in key fields
    key_fields = (
        ["pool", "protocol_slug", "chain", "symbol"]
        if data_type == "current_state"
        else ["pool_id", "protocol_slug", "chain", "symbol"]
    )
    for field in key_fields:
        if field in df.columns:
            null_count = df.select(pl.col(field).is_null().sum()).item()
            if null_count > 0:
                results["issues"].append(f"Null values in '{field}': {null_count}")

    # Check date ranges for time series data
    if (
        data_type in ["historical_tvl", "historical_facts"]
        and "timestamp" in df.columns
    ):
        timestamps = df.select(pl.col("timestamp")).to_series().to_list()
        if timestamps:
            min_date = min(timestamps)
            max_date = max(timestamps)
            results["date_range"] = {"min": min_date, "max": max_date}

    logger.info(
        f"Data quality validation for {data_type}: {len(results['issues'])} issues found"
    )
    return results

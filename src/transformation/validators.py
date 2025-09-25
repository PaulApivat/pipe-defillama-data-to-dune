"""
Data Validators - Simplified Transform Layer

Pure functions for validating data transformations.
Ensures data quality and schema compliance.
"""

import polars as pl
from typing import Dict, Any
from .schemas import (
    POOL_DIM_SCHEMA,
    HISTORICAL_FACTS_SCHEMA,
)
import logging

logger = logging.getLogger(__name__)


def validate_pool_dim_schema(df: pl.DataFrame) -> bool:
    """
    Validate pool dimensions data matches expected schema

    Args:
        df: Pool dimensions DataFrame

    Returns:
        bool: True if valid, raises exception if invalid
    """
    if df.schema != POOL_DIM_SCHEMA:
        raise ValueError(
            f"Schema mismatch: expected {POOL_DIM_SCHEMA}, got {df.schema}"
        )

    # Check for null values in required fields
    required_fields = ["pool_id", "protocol_slug", "chain", "symbol"]
    for field in required_fields:
        null_count = df.select(pl.col(field).is_null().sum()).item()
        if null_count > 0:
            raise ValueError(
                f"Null values found in required field '{field}': {null_count}"
            )

    logger.info(f"Pool dimensions validation passed: {df.height} records")
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

    # Check for null values in required fields
    required_fields = ["timestamp", "pool_id", "protocol_slug", "chain", "symbol"]
    for field in required_fields:
        null_count = df.select(pl.col(field).is_null().sum()).item()
        if null_count > 0:
            raise ValueError(
                f"Null values found in required field '{field}': {null_count}"
            )

    logger.info(f"Historical facts validation passed: {df.height} records")
    return True


def validate_data_quality(df: pl.DataFrame, data_type: str) -> Dict[str, Any]:
    """
    Validate data quality and return quality metrics

    Args:
        df: DataFrame to validate
        data_type: Type of data for specific validation

    Returns:
        Dict: Quality metrics and validation results
    """
    logger.info(f"Validating data quality for {data_type}")

    quality_metrics = {
        "total_records": df.height,
        "null_counts": {},
        "duplicate_counts": {},
        "data_types": df.schema,
    }

    # Check for null values in all columns
    for column in df.columns:
        null_count = df.select(pl.col(column).is_null().sum()).item()
        quality_metrics["null_counts"][column] = null_count

    # Check for duplicates
    if data_type == "pool_dimensions":
        duplicate_count = df.height - df.select("pool_id").n_unique().item()
        quality_metrics["duplicate_counts"]["pool_id"] = duplicate_count
    elif data_type == "historical_facts":
        duplicate_count = (
            df.height - df.select(["timestamp", "pool_id"]).n_unique().item()
        )
        quality_metrics["duplicate_counts"]["timestamp_pool_id"] = duplicate_count

    # Log quality issues
    for column, null_count in quality_metrics["null_counts"].items():
        if null_count > 0:
            logger.warning(f"Column '{column}' has {null_count} null values")

    for key, duplicate_count in quality_metrics["duplicate_counts"].items():
        if duplicate_count > 0:
            logger.warning(f"Duplicate records found for '{key}': {duplicate_count}")

    logger.info(f"Data quality validation completed for {data_type}")
    return quality_metrics


def validate_business_rules(df: pl.DataFrame, data_type: str) -> bool:
    """
    Validate business rules specific to the data type

    Args:
        df: DataFrame to validate
        data_type: Type of data for specific rules

    Returns:
        bool: True if all business rules pass
    """
    logger.info(f"Validating business rules for {data_type}")

    if data_type == "pool_dimensions":
        # Check that all pools have valid protocol slugs
        invalid_protocols = df.filter(
            pl.col("protocol_slug").is_null() | (pl.col("protocol_slug") == "")
        ).height

        if invalid_protocols > 0:
            raise ValueError(
                f"Found {invalid_protocols} pools with invalid protocol slugs"
            )

        # Check that all pools have valid chains
        invalid_chains = df.filter(
            pl.col("chain").is_null() | (pl.col("chain") == "")
        ).height

        if invalid_chains > 0:
            raise ValueError(f"Found {invalid_chains} pools with invalid chains")

    elif data_type == "historical_facts":
        # Check that TVL values are non-negative
        negative_tvl = df.filter(pl.col("tvl_usd") < 0).height
        if negative_tvl > 0:
            logger.warning(f"Found {negative_tvl} records with negative TVL values")

        # Check that APY values are reasonable (between -100% and 1000%)
        extreme_apy = df.filter((pl.col("apy") < -100) | (pl.col("apy") > 1000)).height
        if extreme_apy > 0:
            logger.warning(f"Found {extreme_apy} records with extreme APY values")

    logger.info(f"Business rules validation passed for {data_type}")
    return True


def validate_complete_pipeline(
    dimensions_df: pl.DataFrame, facts_df: pl.DataFrame
) -> Dict[str, Any]:
    """
    Validate the complete transformation pipeline

    Args:
        dimensions_df: Pool dimensions DataFrame
        facts_df: Historical facts DataFrame

    Returns:
        Dict: Complete validation results
    """
    logger.info("Validating complete transformation pipeline")

    # Validate schemas
    validate_pool_dim_schema(dimensions_df)
    validate_historical_facts_schema(facts_df)

    # Validate data quality
    dimensions_quality = validate_data_quality(dimensions_df, "pool_dimensions")
    facts_quality = validate_data_quality(facts_df, "historical_facts")

    # Validate business rules
    validate_business_rules(dimensions_df, "pool_dimensions")
    validate_business_rules(facts_df, "historical_facts")

    # Check referential integrity
    dimension_pools = set(dimensions_df.select("pool_id").to_series().to_list())
    facts_pools = set(facts_df.select("pool_id").to_series().to_list())

    orphaned_facts = facts_pools - dimension_pools
    if orphaned_facts:
        logger.warning(f"Found {len(orphaned_facts)} orphaned fact records")

    validation_results = {
        "dimensions_quality": dimensions_quality,
        "facts_quality": facts_quality,
        "orphaned_facts_count": len(orphaned_facts),
        "pipeline_valid": len(orphaned_facts) == 0,
    }

    logger.info("Complete pipeline validation completed")
    return validation_results

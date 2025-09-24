"""
Data Transformers - Transform Layer

Pure functions for data transformations including SCD2 logic.
All functions are deterministic and unit testable.
No I/O operations, just data manipulation.
"""

import polars as pl
import hashlib
import duckdb
from datetime import date
from typing import List, Dict, Any, Optional
from .schemas import (
    CURRENT_STATE_SCHEMA,
    HISTORICAL_TVL_SCHEMA,
    POOL_DIM_SCD2_SCHEMA,
    HISTORICAL_FACTS_SCHEMA,
)
from .validators import (
    validate_current_state_schema,
    validate_historical_tvl_schema,
    validate_scd2_schema,
    validate_historical_facts_schema,
)
import logging

logger = logging.getLogger(__name__)


def transform_raw_pools_to_current_state(raw_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform raw pools data to current state format

    Args:
        raw_df: Raw pools data from API

    Returns:
        pl.DataFrame: Transformed current state data
    """
    logger.info("Transforming raw pools data to current state format")

    try:
        # Clean and standardize the data
        transformed_df = (
            raw_df.with_columns(
                [
                    # Convert Int64 to Float64 for consistency
                    pl.col("tvl_usd").cast(pl.Float64),
                    # Handle pool_old field (convert from Float64 to String)
                    pl.col("pool_old").cast(pl.String),
                    # Ensure timestamp is properly formatted
                    pl.col("timestamp").cast(pl.String),
                ]
            )
            # Select only the fields we need for current state
            .select(
                [
                    "pool",
                    "protocol_slug",
                    "chain",
                    "symbol",
                    "underlying_tokens",
                    "reward_tokens",
                    "timestamp",
                    "tvl_usd",
                    "apy",
                    "apy_base",
                    "apy_reward",
                    "pool_old",
                ]
            )
        )

        # Validate schema
        if transformed_df.schema != CURRENT_STATE_SCHEMA:
            logger.warning(
                f"Schema mismatch: expected {CURRENT_STATE_SCHEMA}, got {transformed_df.schema}"
            )

        logger.info(f"Transformed {transformed_df.height} current state records")
        return transformed_df

    except Exception as e:
        logger.error(f"❌ Error transforming raw pools data: {e}")
        raise


def transform_raw_tvl_to_historical_tvl(raw_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform raw TVL data to historical TVL format

    Args:
        raw_df: Raw TVL data from API

    Returns:
        pl.DataFrame: Transformed historical TVL data
    """
    logger.info("Transforming raw TVL data to historical TVL format")

    try:
        # Clean and standardize the data
        transformed_df = (
            raw_df.with_columns(
                [
                    # Convert Int64 to Float64 for consistency
                    pl.col("tvl_usd").cast(pl.Float64),
                    # Ensure timestamp is properly formatted
                    pl.col("timestamp").cast(pl.String),
                ]
            )
            # Select only the fields we need for historical TVL
            .select(
                ["timestamp", "pool_id", "tvl_usd", "apy", "apy_base", "apy_reward"]
            )
        )

        # Validate schema
        if transformed_df.schema != HISTORICAL_TVL_SCHEMA:
            logger.warning(
                f"Schema mismatch: expected {HISTORICAL_TVL_SCHEMA}, got {transformed_df.schema}"
            )

        logger.info(f"Transformed {transformed_df.height} historical TVL records")
        return transformed_df

    except Exception as e:
        logger.error(f"❌ Error transforming raw TVL data: {e}")
        raise


def create_scd2_dimensions(
    current_state_df: pl.DataFrame,
    snap_date: date,
    existing_scd2_df: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """
    Create SCD2 dimensions using SQL approach

    Args:
        current_state_df: Current state DataFrame
        snap_date: Snapshot date for SCD2
        existing_scd2_df: Existing SCD2 data (optional)

    Returns:
        pl.DataFrame: SCD2 dimensions with historical tracking
    """
    logger.info(f"Creating SCD2 dimensions for snapshot date: {snap_date}")

    try:
        # Create DuckDB connection
        conn = duckdb.connect()

        # Register DataFrames
        conn.register("current_state", current_state_df)

        if existing_scd2_df is not None:
            conn.register("existing_scd2", existing_scd2_df)
        else:
            # Create empty SCD2 table if none exists
            empty_scd2 = pl.DataFrame(
                {
                    "pool_id": [],
                    "protocol_slug": [],
                    "chain": [],
                    "symbol": [],
                    "underlying_tokens": [],
                    "reward_tokens": [],
                    "timestamp": [],
                    "tvl_usd": [],
                    "apy": [],
                    "apy_base": [],
                    "apy_reward": [],
                    "pool_old": [],
                    "valid_from": [],
                    "valid_to": [],
                    "is_current": [],
                    "attrib_hash": [],
                    "is_active": [],
                }
            )
            conn.register("existing_scd2", empty_scd2)

        # SQL for SCD2 update (from scd2_manager.py)
        sql = f"""
        WITH new_dims AS (
            SELECT
                pool as pool_id 
                , protocol_slug 
                , chain 
                , symbol 
                , underlying_tokens 
                , reward_tokens
                , timestamp
                , tvl_usd
                , apy 
                , apy_base
                , apy_reward
                , pool_old 
                , '2022-01-01'::DATE as valid_from 
                , '9999-12-31'::DATE as valid_to 
                , true as is_current 
                , md5(protocol_slug || '|' || chain || '|' || symbol || '|' || 
                    array_to_string(underlying_tokens, '|') || '|' || 
                    array_to_string(reward_tokens, '|') || '|' || 
                    coalesce(tvl_usd::text, '') || '|' || 
                    coalesce(apy::text, '') || '|' || 
                    coalesce(apy_base::text, '') || '|' || 
                    coalesce(apy_reward::text, '') || '|' || 
                    coalesce(pool_old, '')
                ) as attrib_hash
                , true as is_active
            FROM current_state
        ),

        changed_records AS (
            SELECT n.pool_id
            FROM new_dims n 
            JOIN existing_scd2 e ON n.pool_id = e.pool_id AND e.is_current = true
            WHERE n.attrib_hash != e.attrib_hash
        ),

        closed_existing AS (
            SELECT 
                pool_id, protocol_slug, chain, symbol, underlying_tokens, 
                reward_tokens, timestamp, tvl_usd, apy, apy_base, apy_reward, 
                pool_old, valid_from,
                CASE 
                    WHEN pool_id IN (SELECT pool_id FROM changed_records)
                    THEN '{snap_date}'::DATE 
                    ELSE valid_to 
                END as valid_to,
                CASE
                    WHEN pool_id IN (SELECT pool_id FROM changed_records) 
                    THEN false 
                    ELSE is_current 
                END as is_current,
                attrib_hash, 
                is_active
            FROM existing_scd2 
        )
        SELECT * FROM closed_existing
        UNION ALL
        SELECT * FROM new_dims 
        ORDER BY pool_id, valid_from 
        """

        # Execute SQL and get result
        result_df = conn.execute(sql).pl()

        # Close connection
        conn.close()

        logger.info(f"Created {result_df.height} SCD2 dimension records")
        return result_df

    except Exception as e:
        logger.error(f"❌ Error creating SCD2 dimensions: {e}")
        raise


def create_historical_facts(
    tvl_df: pl.DataFrame, scd2_df: pl.DataFrame, target_date: Optional[date] = None
) -> pl.DataFrame:
    """
    Create historical facts using SQL as-of join approach

    Args:
        tvl_df: Historical TVL DataFrame
        scd2_df: SCD2 dimensions DataFrame
        target_date: Optional target date for filtering

    Returns:
        pl.DataFrame: Historical facts with denormalized data
    """
    logger.info("Creating historical facts with as-of join")

    try:
        # Create DuckDB connection
        conn = duckdb.connect()

        # Register DataFrames
        conn.register("tvl_data", tvl_df)
        conn.register("existing_scd2", scd2_df)

        # Build date filter if target_date provided
        date_filter = ""
        if target_date:
            date_filter = f"WHERE timestamp::DATE = '{target_date}'"

        # SQL for as-of join (from scd2_manager.py)
        sql = f"""
            WITH tvl_with_date AS (
                SELECT
                    pool_id
                    , tvl_usd 
                    , apy
                    , apy_base
                    , apy_reward
                    , timestamp::DATE as date
                FROM tvl_data
                {date_filter}
            )
            SELECT
                t.date as timestamp 
                , SPLIT_PART(d.pool_old, '-', 1) as pool_old_clean
                , t.pool_id 
                , d.protocol_slug 
                , d.chain
                , d.symbol
                , t.tvl_usd
                , t.apy 
                , t.apy_base
                , t.apy_reward
                , d.valid_from
                , d.valid_to
                , d.is_current
                , d.attrib_hash
                , d.is_active
            FROM tvl_with_date t
            JOIN existing_scd2 d ON (
                t.pool_id = d.pool_id AND 
                t.date >= d.valid_from AND 
                t.date < d.valid_to
            )
            ORDER BY t.date, t.pool_id
        """

        # Execute SQL and get result
        result_df = conn.execute(sql).pl()

        # Close connection
        conn.close()

        logger.info(f"Created {result_df.height} historical facts records")
        return result_df

    except Exception as e:
        logger.error(f"❌ Error creating historical facts: {e}")
        raise


def upsert_historical_facts_for_date(
    tvl_df: pl.DataFrame, scd2_df: pl.DataFrame, target_date: date
) -> pl.DataFrame:
    """
    Create incremental historical facts for a specific date

    Args:
        tvl_df: Historical TVL DataFrame
        scd2_df: SCD2 dimensions DataFrame
        target_date: Target date for incremental update

    Returns:
        pl.DataFrame: Historical facts for the target date
    """
    logger.info(f"Creating incremental historical facts for date: {target_date}")

    try:
        # Create DuckDB connection
        conn = duckdb.connect()

        # Register DataFrames
        conn.register("tvl_data", tvl_df)
        conn.register("existing_scd2", scd2_df)

        # SQL for incremental historical facts
        sql = f"""
            WITH tvl_with_date AS (
                SELECT
                    pool_id
                    , tvl_usd 
                    , apy
                    , apy_base
                    , apy_reward
                    , timestamp::DATE as date
                FROM tvl_data
                WHERE timestamp::DATE = '{target_date}'
            )
            SELECT
                t.date as timestamp 
                , SPLIT_PART(d.pool_old, '-', 1) as pool_old_clean
                , t.pool_id 
                , d.protocol_slug 
                , d.chain
                , d.symbol
                , t.tvl_usd
                , t.apy 
                , t.apy_base
                , t.apy_reward
                , d.valid_from
                , d.valid_to
                , d.is_current
                , d.attrib_hash
                , d.is_active
            FROM tvl_with_date t
            JOIN existing_scd2 d ON (
                t.pool_id = d.pool_id AND 
                t.date >= d.valid_from AND 
                t.date < d.valid_to
            )
            ORDER BY t.date, t.pool_id
        """

        # Execute SQL and get result
        result_df = conn.execute(sql).pl()

        # Close connection
        conn.close()

        logger.info(f"Created {result_df.height} incremental historical facts records")
        return result_df

    except Exception as e:
        logger.error(f"❌ Error creating incremental historical facts: {e}")
        raise


def filter_pools_by_projects(
    df: pl.DataFrame, target_projects: set[str]
) -> pl.DataFrame:
    """
    Filter pools by target projects

    Args:
        df: Current state DataFrame
        target_projects: Set of project slugs to include

    Returns:
        pl.DataFrame: Filtered pools data
    """
    logger.info(f"Filtering pools by projects: {target_projects}")

    filtered_df = df.filter(pl.col("protocol_slug").is_in(list(target_projects)))

    logger.info(f"Filtered to {filtered_df.height} pools")
    return filtered_df


def sort_by_timestamp(df: pl.DataFrame, descending: bool = False) -> pl.DataFrame:
    """
    Sort data by timestamp

    Args:
        df: DataFrame to sort
        descending: Sort in descending order

    Returns:
        pl.DataFrame: Sorted data
    """
    logger.info(f"Sorting data by timestamp (descending={descending})")

    sorted_df = df.sort("timestamp", descending=descending)

    logger.info(f"Sorted {sorted_df.height} records")
    return sorted_df


def sort_by_tvl(df: pl.DataFrame, descending: bool = True) -> pl.DataFrame:
    """
    Sort data by TVL

    Args:
        df: DataFrame to sort
        descending: Sort in descending order

    Returns:
        pl.DataFrame: Sorted data
    """
    logger.info(f"Sorting data by TVL (descending={descending})")

    sorted_df = df.sort("tvl_usd", descending=descending)

    logger.info(f"Sorted {sorted_df.height} records")
    return sorted_df


def get_summary_stats(df: pl.DataFrame, data_type: str) -> Dict[str, Any]:
    """
    Get summary statistics for data

    Args:
        df: DataFrame to analyze
        data_type: Type of data for specific statistics

    Returns:
        Dict: Summary statistics
    """
    logger.info(f"Generating summary stats for {data_type}")

    if data_type == "current_state":
        stats = {
            "total_pools": df.height,
            "protocol_slug_unique": df.select("protocol_slug").n_unique().item(),
            "chain_unique": df.select("chain").n_unique().item(),
            "symbol_unique": df.select("symbol").n_unique().item(),
            "underlying_tokens_avg_count": df.select("underlying_tokens")
            .with_columns(pl.col("underlying_tokens").list.len())
            .mean()
            .item(),
            "underlying_tokens_max_count": df.select("underlying_tokens")
            .with_columns(pl.col("underlying_tokens").list.len())
            .max()
            .item(),
            "reward_tokens_avg_count": df.select("reward_tokens")
            .with_columns(pl.col("reward_tokens").list.len())
            .mean()
            .item(),
            "reward_tokens_max_count": df.select("reward_tokens")
            .with_columns(pl.col("reward_tokens").list.len())
            .max()
            .item(),
            "timestamp_unique": df.select("timestamp").n_unique().item(),
            "tvl_usd_sum": df.select("tvl_usd").sum().item(),
            "apy_mean": df.select("apy").mean().item(),
            "apy_base_mean": df.select("apy_base").mean().item(),
            "apy_reward_mean": df.select("apy_reward").mean().item(),
            "pool_old_unique": df.select("pool_old").n_unique().item(),
        }
    elif data_type == "historical_tvl":
        stats = {
            "total_records": df.height,
            "unique_pools": df.select("pool_id").n_unique().item(),
            "date_range": f"{df.select('timestamp').min().item()} to {df.select('timestamp').max().item()}",
            "tvl_usd_sum": df.select("tvl_usd").sum().item(),
            "apy_mean": df.select("apy").mean().item(),
            "apy_base_mean": df.select("apy_base").mean().item(),
            "apy_reward_mean": df.select("apy_reward").mean().item(),
        }
    else:
        raise ValueError(f"Unknown data_type: {data_type}")

    logger.info(f"Generated summary stats: {list(stats.keys())}")
    return stats

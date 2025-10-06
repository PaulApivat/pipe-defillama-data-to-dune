"""
Data Transformers - Simplified Transform Layer

Pure functions for data transformations without SCD2 complexity.
Since DeFiLlama dimensions are not slowly changing, we use simple schemas.
"""

import polars as pl
import duckdb
from datetime import date
from typing import List, Dict, Any, Optional
from .schemas import (
    POOL_DIM_SCHEMA,
    HISTORICAL_FACTS_SCHEMA,
)
import logging

logger = logging.getLogger(__name__)


def create_pool_dimensions(raw_pools_df: pl.DataFrame) -> pl.DataFrame:
    """
    Create simple pool dimensions from raw pools data

    Args:
        raw_pools_df: Raw pools data from API

    Returns:
        pl.DataFrame: Pool dimensions data
    """
    logger.info("Creating pool dimensions from raw pools data")

    try:
        # Clean and standardize the data
        dimensions_df = (
            raw_pools_df.with_columns(
                [
                    # Convert pool to pool_id for consistency
                    pl.col("pool").alias("pool_id"),
                    # Ensure pool_old is string
                    pl.col("pool_old").cast(pl.String()),
                ]
            )
            # Select all fields to match POOL_DIM_SCHEMA (same as RAW_POOLS_SCHEMA)
            .select(
                [
                    "pool_id",
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
        if dimensions_df.schema != POOL_DIM_SCHEMA:
            logger.warning(
                f"Schema mismatch: expected {POOL_DIM_SCHEMA}, got {dimensions_df.schema}"
            )

        logger.info(f"Created {dimensions_df.height} pool dimension records")
        return dimensions_df

    except Exception as e:
        logger.error(f"❌ Error creating pool dimensions: {e}")
        raise


def create_historical_facts(
    tvl_df: pl.DataFrame,
    dimensions_df: pl.DataFrame,
    target_date: Optional[date] = None,
) -> pl.DataFrame:
    """
    Create historical facts by joining TVL data with dimensions

    Args:
        tvl_df: Historical TVL DataFrame
        dimensions_df: Pool dimensions DataFrame
        target_date: Optional target date for filtering

    Returns:
        pl.DataFrame: Historical facts with denormalized data
    """
    logger.info("Creating historical facts with simple join")

    try:
        # Create DuckDB connection
        conn = duckdb.connect()

        # Register DataFrames
        conn.register("tvl_data", tvl_df)
        conn.register("pool_dimensions", dimensions_df)

        # Build date filter if target_date provided
        date_filter = ""
        if target_date:
            date_filter = f"WHERE timestamp::DATE = '{target_date}'"

        # SQL for simple join
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
                , CASE 
                    WHEN SPLIT_PART(d.pool_old, '-', 1) LIKE '0x%' 
                    THEN SPLIT_PART(d.pool_old, '-', 1)
                    WHEN SPLIT_PART(d.pool_old, '-', 2) LIKE '0x%' 
                    THEN SPLIT_PART(d.pool_old, '-', 2)
                    ELSE d.pool_old
                  END as pool_id
                , t.pool_id as pool_id_defillama
                , d.protocol_slug 
                , d.chain
                , d.symbol
                , t.tvl_usd
                , t.apy 
                , t.apy_base
                , t.apy_reward
            FROM tvl_with_date t
            JOIN pool_dimensions d ON t.pool_id = d.pool_id
            ORDER BY t.date, t.pool_id
        """

        # Execute SQL and get result
        result_df = conn.execute(sql).pl()

        # Close connection
        conn.close()

        # Convert hex string to actual binary data
        def hex_string_to_binary(hex_string: str) -> bytes:
            """Convert hex string to binary data"""
            if hex_string.startswith("0x"):
                hex_string = hex_string[2:]  # Remove 0x prefix
            return bytes.fromhex(hex_string)

        # Apply binary conversion to pool_id column
        result_df = result_df.with_columns(
            [
                pl.col("pool_id").map_elements(
                    hex_string_to_binary, return_dtype=pl.Binary()
                )
            ]
        )

        # Validate schema
        if result_df.schema != HISTORICAL_FACTS_SCHEMA:
            logger.warning(
                f"Schema mismatch: expected {HISTORICAL_FACTS_SCHEMA}, got {result_df.schema}"
            )

        logger.info(f"Created {result_df.height} historical facts records")
        return result_df

    except Exception as e:
        logger.error(f"❌ Error creating historical facts: {e}")
        raise


def filter_pools_by_projects(
    df: pl.DataFrame, target_projects: set[str]
) -> pl.DataFrame:
    """
    Filter pools by target projects

    Args:
        df: Pools DataFrame
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

    if data_type == "pool_dimensions":
        stats = {
            "total_pools": df.height,
            "protocol_slug_unique": df.select("protocol_slug").n_unique(),
            "chain_unique": df.select("chain").n_unique(),
            "symbol_unique": df.select("symbol").n_unique(),
            "underlying_tokens_avg_count": df.select("underlying_tokens")
            .with_columns(pl.col("underlying_tokens").list.len())
            .mean()
            .item(),
            "reward_tokens_avg_count": df.select("reward_tokens")
            .with_columns(pl.col("reward_tokens").list.len())
            .mean()
            .item(),
            "timestamp_unique": df.select("timestamp").n_unique(),
            "tvl_usd_sum": df.select("tvl_usd").sum().item(),
            "apy_mean": df.select("apy").mean().item(),
            "apy_base_mean": df.select("apy_base").mean().item(),
            "apy_reward_mean": df.select("apy_reward").mean().item(),
            "pool_old_unique": df.select("pool_old").n_unique(),
        }
    elif data_type == "historical_facts":
        stats = {
            "total_records": df.height,
            "unique_pools": df.select("pool_id_defillama").n_unique(),
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


def save_transformed_data(
    dimensions_df: pl.DataFrame,
    historical_facts_df: pl.DataFrame,
    today: str,
) -> None:
    """
    Save transformed data to output directory

    Args:
        dimensions_df: Pool dimensions data
        historical_facts_df: Historical facts data
        today: Date string for file naming
    """
    logger.info("Saving transformed data to output directory")

    try:
        # Save pool dimensions
        dimensions_file = "output/pool_dimensions.parquet"
        dimensions_df.write_parquet(dimensions_file)
        logger.info(f"✅ Saved pool dimensions to {dimensions_file}")

        # Save historical facts
        historical_facts_file = f"output/historical_facts_{today}.parquet"
        historical_facts_df.write_parquet(historical_facts_file)
        logger.info(f"✅ Saved historical facts to {historical_facts_file}")

        logger.info("🎉 All transformed data saved successfully!")

    except Exception as e:
        logger.error(f"❌ Error saving transformed data: {e}")
        raise


def main():
    """
    Main function to run the complete transformation pipeline
    """
    from datetime import date
    import glob
    import os

    logger.info("🚀 Starting Simplified Transform Layer Pipeline")

    try:
        # Get today's date
        today = date.today().strftime("%Y-%m-%d")

        # Load extracted data
        logger.info("📁 Loading extracted data...")

        # Find the most recent raw_pools parquet file
        raw_pools_files = glob.glob("output/raw_pools_*.parquet")
        if not raw_pools_files:
            raise FileNotFoundError(
                "No raw_pools parquet files found. Run extract layer first."
            )

        latest_raw_pools = max(raw_pools_files, key=os.path.getctime)
        logger.info(f"Loading raw pools from: {latest_raw_pools}")

        # Find the most recent raw_tvl parquet file
        raw_tvl_files = glob.glob("output/raw_tvl_*.parquet")
        if not raw_tvl_files:
            raise FileNotFoundError(
                "No raw_tvl parquet files found. Run extract layer first."
            )

        latest_raw_tvl = max(raw_tvl_files, key=os.path.getctime)
        logger.info(f"Loading raw TVL from: {latest_raw_tvl}")

        # Load the data
        raw_pools_df = pl.read_parquet(latest_raw_pools)
        raw_tvl_df = pl.read_parquet(latest_raw_tvl)

        logger.info(f"Loaded {raw_pools_df.height} raw pool records")
        logger.info(f"Loaded {raw_tvl_df.height} raw TVL records")

        # Step 1: Create pool dimensions
        logger.info("🔄 Step 1: Creating pool dimensions...")
        dimensions_df = create_pool_dimensions(raw_pools_df)

        # Step 2: Filter by target projects
        logger.info("🔄 Step 2: Filtering by target projects...")
        target_projects = {
            "curve-dex",
            "pancakeswap-amm",
            "pancakeswap-amm-v3",
            "aerodrome-slipstream",
            "aerodrome-v1",
            "uniswap-v2",
            "uniswap-v3",
            "fluid-dex",
        }
        filtered_dimensions_df = filter_pools_by_projects(
            dimensions_df, target_projects
        )

        # Step 3: Create historical facts (join TVL + dimensions)
        logger.info("🔄 Step 3: Creating historical facts...")
        historical_facts_df = create_historical_facts(
            raw_tvl_df, filtered_dimensions_df
        )

        # Step 4: Save all transformed data
        logger.info("🔄 Step 4: Saving transformed data...")
        save_transformed_data(filtered_dimensions_df, historical_facts_df, today)

        # Step 5: Generate summary statistics
        logger.info("🔄 Step 5: Generating summary statistics...")

        # Dimensions stats
        dimensions_stats = get_summary_stats(filtered_dimensions_df, "pool_dimensions")
        logger.info(
            f"Pool Dimensions Stats: {dimensions_stats['total_pools']} pools, "
            f"{dimensions_stats['protocol_slug_unique']} protocols"
        )

        # Historical facts stats
        facts_stats = get_summary_stats(historical_facts_df, "historical_facts")
        logger.info(
            f"Historical Facts Stats: {facts_stats['total_records']} records, "
            f"{facts_stats['unique_pools']} unique pools, "
            f"Date range: {facts_stats['date_range']}"
        )

        logger.info("🎉 Simplified Transform Layer Pipeline completed successfully!")

    except Exception as e:
        logger.error(f"❌ Transform Layer Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()

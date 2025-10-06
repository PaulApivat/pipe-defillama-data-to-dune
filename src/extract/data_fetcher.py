"""
Data Fetcher - Extract Layer

Pure functions for fetching data from external APIs.
No business logic, just I/O operations that return raw data.
"""

import polars as pl
import os
from datetime import date
from typing import List, Dict, Any, Set
from .defillama_api import get_pools_old, get_chart_data_batch
from .schemas import RAW_POOLS_SCHEMA, RAW_TVL_SCHEMA
import logging

logger = logging.getLogger(__name__)

# Target projects for filtering
TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
    "uniswap-v2",
    "uniswap-v3",
    "fluid-dex",
}


def fetch_raw_pools_data() -> pl.DataFrame:
    """
    Fetch raw pools data from DeFiLlama API and save to output directory

    Returns:
        pl.DataFrame: Raw pools data with RAW_POOLS_SCHEMA
    """
    logger.info("Fetching raw pools data from DeFiLlama PoolsOld API")

    try:
        # Get raw data from API - PURE I/O ONLY
        raw_data = get_pools_old()

        # Convert to Polars DataFrame with strict=False to handle mixed types
        df = pl.DataFrame(raw_data, strict=False, infer_schema_length=10000)

        # Log the actual schema we got
        logger.info(f"Raw data schema: {df.schema}")

        # The API returns nested data, so we need to extract the 'data' field
        # and flatten it to get the actual pool records
        if "data" in df.columns:
            # Extract the nested data field and map API fields to our schema
            # This is the correct way - map actual API field names to our schema
            flattened_df = df.select(
                pl.col("data").struct.rename_fields(
                    [
                        "pool",
                        "timestamp",
                        "project",
                        "chain",
                        "symbol",
                        "poolMeta",
                        "underlyingTokens",
                        "rewardTokens",
                        "tvlUsd",
                        "apy",
                        "apyBase",
                        "apyReward",
                        "il7d",
                        "apyBase7d",
                        "volumeUsd1d",
                        "volumeUsd7d",
                        "apyBaseInception",
                        "url",
                        "apyPct1D",
                        "apyPct7D",
                        "apyPct30D",
                        "apyMean30d",
                        "stablecoin",
                        "ilRisk",
                        "exposure",
                        "return",
                        "count",
                        "apyMeanExpanding",
                        "apyStdExpanding",
                        "mu",
                        "sigma",
                        "outlier",
                        "project_factorized",
                        "chain_factorized",
                        "predictions",
                        "pool_old",
                    ]
                )
            )

            # Now we need to actually flatten the struct to get individual columns
            flattened_df = flattened_df.unnest("data")

            # Now map the API field names to our schema field names
            # This is the key step that was missing!
            mapped_df = flattened_df.select(
                [
                    pl.col("pool"),
                    pl.col("project").alias(
                        "protocol_slug"
                    ),  # Map project -> protocol_slug
                    pl.col("chain"),
                    pl.col("symbol"),
                    pl.col("underlyingTokens").alias(
                        "underlying_tokens"
                    ),  # Keep as List
                    pl.col("rewardTokens").alias("reward_tokens"),  # Keep as List
                    pl.col("timestamp"),
                    pl.col("tvlUsd").alias("tvl_usd"),  # Map tvlUsd -> tvl_usd
                    pl.col("apy"),
                    pl.col("apyBase").alias("apy_base"),  # Map apyBase -> apy_base
                    pl.col("apyReward").alias(
                        "apy_reward"
                    ),  # Map apyReward -> apy_reward
                    pl.col("pool_old"),
                ]
            )

            # Debug: Log the schema after mapping
            logger.info(f"Mapped data schema: {mapped_df.schema}")

            # Apply filtering by target projects
            logger.info(f"Filtering by target projects: {TARGET_PROJECTS}")
            filtered_df = filter_pools_by_projects(mapped_df, TARGET_PROJECTS)

            # Debug: Log the schema after filtering
            logger.info(f"Filtered data schema: {filtered_df.schema}")

            # Handle data type conversion more carefully
            # Check what we actually have before trying to cast
            tvl_usd_type = filtered_df.select("tvl_usd").dtypes[0]
            pool_old_type = filtered_df.select("pool_old").dtypes[0]
            underlying_tokens_type = filtered_df.select("underlying_tokens").dtypes[0]

            logger.info(f"tvl_usd type: {tvl_usd_type}")
            logger.info(f"pool_old type: {pool_old_type}")
            logger.info(f"underlying_tokens type: {underlying_tokens_type}")

            # Handle List types properly - use a more robust approach
            corrected_df = filtered_df

            # If tvl_usd is a List, we need to handle it carefully
            if tvl_usd_type == pl.List(pl.String):
                logger.info("tvl_usd is a List of Strings - using try-catch approach")
                try:
                    # Try to extract first element, but handle errors gracefully
                    corrected_df = corrected_df.with_columns(
                        [
                            pl.col("tvl_usd")
                            .map_elements(
                                lambda x: (
                                    float(x[0])
                                    if isinstance(x, list) and len(x) > 0
                                    else None
                                ),
                                return_dtype=pl.Float64,
                            )
                            .alias("tvl_usd")
                        ]
                    )
                    logger.info("Successfully converted tvl_usd from List to Float64")
                except Exception as e:
                    logger.warning(f"Failed to convert tvl_usd from List: {e}")
                    # Fallback: set all to null
                    corrected_df = corrected_df.with_columns(
                        [pl.lit(None).cast(pl.Float64).alias("tvl_usd")]
                    )
            elif tvl_usd_type != pl.Float64:
                logger.info("Casting tvl_usd to Float64")
                corrected_df = corrected_df.with_columns(
                    [pl.col("tvl_usd").cast(pl.Float64)]
                )

            # Handle pool_old type
            if pool_old_type != pl.String:
                logger.info("Casting pool_old to String")
                corrected_df = corrected_df.with_columns(
                    [pl.col("pool_old").cast(pl.String)]
                )

            # Handle underlying_tokens type - should be List(String)
            if underlying_tokens_type != pl.List(pl.String):
                logger.info("Converting underlying_tokens to List(String)")
                corrected_df = corrected_df.with_columns(
                    [pl.col("underlying_tokens").cast(pl.List(pl.String))]
                )

            # Ensure other numeric fields are correct types
            corrected_df = corrected_df.with_columns(
                [
                    pl.col("apy").cast(pl.Float64),
                    pl.col("apy_base").cast(pl.Float64),
                    pl.col("apy_reward").cast(pl.Float64),
                ]
            )

            logger.info(f"Final corrected schema: {corrected_df.schema}")
            logger.info(f"Fetched {corrected_df.height} raw pool records")

            # Save to output directory
            today = date.today().strftime("%Y-%m-%d")
            output_file = f"output/raw_pools_{today}.parquet"
            corrected_df.write_parquet(output_file)
            logger.info(f"✅ Saved raw pools data to {output_file}")

            return corrected_df
        else:
            # If no nested structure, return as-is
            logger.info(f"Fetched {df.height} raw pool records")
            return df

    except Exception as e:
        logger.error(f"❌ Error fetching raw pools data: {e}")
        raise


def fetch_raw_tvl_data(pool_ids: List[str]) -> pl.DataFrame:
    """
    Fetch raw TVL data for given pool IDs

    Args:
        pool_ids: List of pool identifiers

    Returns:
        pl.DataFrame: Raw TVL data with RAW_TVL_SCHEMA
    """
    logger.info(f"Fetching raw TVL data for {len(pool_ids)} pools")

    try:

        # Get raw data from API
        raw_data = get_chart_data_batch(pool_ids)

        # Flatten the data
        all_tvl_records = []
        for pool_id, chart_data in raw_data.items():
            if chart_data.get("status") == "success" and chart_data.get("data"):
                for point in chart_data["data"]:
                    record = {
                        "timestamp": point["timestamp"],
                        "tvl_usd": point["tvlUsd"],
                        "apy": point["apy"],
                        "apy_base": point["apyBase"],
                        "apy_reward": point["apyReward"],
                        "pool_id": pool_id,
                    }
                    all_tvl_records.append(record)

        # Convert to Polars DataFrame with increased schema inference length
        # This prevents data type overflow errors with large datasets
        df = pl.DataFrame(all_tvl_records, strict=False, infer_schema_length=10000)

        # Debug: Log the schema we got
        logger.info(f"Raw TVL data schema: {df.schema}")

        # Fix data types to match RAW_TVL_SCHEMA
        corrected_df = df.with_columns(
            [
                # Convert tvl_usd from Int64 to Float64
                pl.col("tvl_usd").cast(pl.Float64),
                # Ensure other numeric fields are correct types
                pl.col("apy").cast(pl.Float64),
                pl.col("apy_base").cast(pl.Float64),
                pl.col("apy_reward").cast(pl.Float64),
            ]
        )

        # Debug: Log the corrected schema
        logger.info(f"Corrected TVL data schema: {corrected_df.schema}")

        # Validate schema matches RAW_TVL_SCHEMA
        if corrected_df.schema != RAW_TVL_SCHEMA:
            logger.warning(
                f"Schema mismatch: expected {RAW_TVL_SCHEMA}, got {corrected_df.schema}"
            )
            logger.info(
                "This is expected for raw data - will be cleaned in transformation layer"
            )

        logger.info(f"Fetched {corrected_df.height} raw TVL records")

        # Save to output directory
        today = date.today().strftime("%Y-%m-%d")
        output_file = f"output/raw_tvl_{today}.parquet"
        corrected_df.write_parquet(output_file)
        logger.info(f"✅ Saved raw TVL data to {output_file}")

        return corrected_df

    except Exception as e:
        logger.error(f"❌ Error fetching raw TVL data: {e}")
        raise


def filter_pools_by_projects(
    df: pl.DataFrame, target_projects: Set[str] = None
) -> pl.DataFrame:
    """
    Filter pools by target projects

    Args:
        df: Raw pools DataFrame
        target_projects: Set of project slugs to include

    Returns:
        pl.DataFrame: Filtered pools data
    """
    if target_projects is None:
        target_projects = TARGET_PROJECTS

    logger.info(f"Filtering pools by projects: {target_projects}")

    # Check if we have the expected columns
    if "protocol_slug" not in df.columns:
        logger.warning(
            f"Column 'protocol_slug' not found. Available columns: {df.columns}"
        )
        logger.info(
            "Returning unfiltered data - filtering will be done in transformation layer"
        )
        return df

    filtered_df = df.filter(pl.col("protocol_slug").is_in(list(target_projects)))

    logger.info(f"Filtered to {filtered_df.height} pools")
    return filtered_df


def get_pool_ids_from_pools(df: pl.DataFrame) -> List[str]:
    """
    Extract pool IDs from pools data

    Args:
        df: Pools DataFrame

    Returns:
        List[str]: List of pool IDs
    """
    pool_ids = df.select("pool").to_series().to_list()
    logger.info(f"Extracted {len(pool_ids)} pool IDs")
    return pool_ids


def fetch_incremental_tvl_data(pool_ids: List[str], target_date: date) -> pl.DataFrame:
    """
    Fetch TVL data INCREMENTALLY for a specific date only

    Strategy:
    1. Check if we have existing TVL data for this date
    2. If yes, return cached data
    3. If no, fetch only the missing data for this specific date

    Args:
        pool_ids: List of pool identifiers
        target_date: Target date for TVL data

    Returns:
        pl.DataFrame: TVL data for target date only
    """
    logger.info(f"🎯 INCREMENTAL: Fetching TVL data for {target_date} only")

    # Check if we already have data for this date
    cache_file = f"output/raw_tvl_{target_date.strftime('%Y-%m-%d')}.parquet"

    if os.path.exists(cache_file):
        logger.info(f"📦 Found cached TVL data for {target_date}, loading...")
        cached_data = pl.read_parquet(cache_file)

        # Verify the data is for the correct date
        if cached_data.height > 0:
            sample_date = cached_data.select("timestamp").head(1).item()
            if str(sample_date).startswith(target_date.strftime("%Y-%m-%d")):
                logger.info(
                    f"✅ Using cached data: {cached_data.height} records for {target_date}"
                )
                return cached_data
            else:
                logger.warning(
                    f"⚠️  Cached data date mismatch: {sample_date} != {target_date}"
                )

    # No cached data, need to fetch
    logger.info(f"📡 No cached data found, fetching fresh data for {target_date}")
    logger.warning("⚠️  This will still fetch ALL historical data, then filter")
    logger.warning("⚠️  True incremental API calls require DeFiLlama API changes")

    # Fetch all data (this is still the bottleneck)
    all_tvl_data = fetch_raw_tvl_data(pool_ids)

    # Filter to target date only
    target_date_str = target_date.strftime("%Y-%m-%d")
    try:
        incremental_data = all_tvl_data.filter(
            pl.col("timestamp").str.strptime(pl.Date, "%Y-%m-%d") == target_date
        )
    except Exception as e:
        logger.warning(f"Date parsing failed, using string matching: {e}")
        # Fallback to string matching if date parsing fails
        incremental_data = all_tvl_data.filter(
            pl.col("timestamp").str.contains(target_date_str)
        )

    # Cache the filtered data for future use
    if incremental_data.height > 0:
        logger.info(f"💾 Caching {incremental_data.height} records for {target_date}")
        incremental_data.write_parquet(cache_file)

    logger.info(f"✅ INCREMENTAL: {incremental_data.height} records for {target_date}")
    return incremental_data

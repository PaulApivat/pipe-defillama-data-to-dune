"""
Pipeline Orchestration - Orchestration Layer

Pure workflow coordination functions that compose extract, transform, and load operations.
No business logic, just orchestration.
"""

import polars as pl
from datetime import date
from typing import Dict, Any, Tuple, Optional

# Extract layer imports
from ..extract.data_fetcher import (
    fetch_raw_pools_data,
    fetch_raw_tvl_data,
    filter_pools_by_projects,
    get_pool_ids_from_pools,
)

# Transform layer imports
from ..transformation.transformers import (
    transform_raw_pools_to_current_state,
    transform_raw_tvl_to_historical_tvl,
    create_scd2_dimensions,
    create_historical_facts,
    filter_by_projects,
    sort_by_tvl,
    sort_by_timestamp,
    get_summary_stats,
)

# Load layer imports
from ..load.local_storage import (
    save_current_state_data,
    save_tvl_data,
    save_scd2_data,
    save_historical_facts_data,
    file_exists,
    get_latest_file,
)

from ..load.dune_uploader import DuneUploader

import logging

logger = logging.getLogger(__name__)

# Target projects
TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
}


def run_current_state_pipeline() -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Run the current state pipeline: extract -> transform -> load

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: (current_state_df, scd2_df)
    """
    logger.info("🔄 Running current state pipeline...")

    # Extract: Fetch raw pools data
    logger.info("Step 1: Extracting raw pools data...")
    raw_pools = fetch_raw_pools_data()

    # Transform: Filter and transform to current state
    logger.info("Step 2: Transforming to current state...")
    filtered_pools = filter_pools_by_projects(raw_pools, TARGET_PROJECTS)
    current_state = transform_raw_pools_to_current_state(filtered_pools)
    current_state = sort_by_tvl(current_state, descending=True)

    # Transform: Create SCD2 dimensions
    logger.info("Step 3: Creating SCD2 dimensions...")
    snap_date = date.today()
    scd2_df = create_scd2_dimensions(current_state, snap_date)

    # Load: Save data
    logger.info("Step 4: Saving current state data...")
    save_current_state_data(current_state)
    save_scd2_data(scd2_df)

    # Get summary stats
    stats = get_summary_stats(current_state, "current_state")
    logger.info(f"✅ Current state pipeline completed: {stats['total_records']} pools")

    return current_state, scd2_df


def run_tvl_pipeline(existing_tvl_file: Optional[str] = None) -> pl.DataFrame:
    """
    Run the TVL pipeline: extract -> transform -> load

    Args:
        existing_tvl_file: Optional existing TVL file for incremental updates

    Returns:
        pl.DataFrame: TVL data
    """
    logger.info("🔄 Running TVL pipeline...")

    # Extract: Get pool IDs from current state
    logger.info("Step 1: Getting pool IDs from current state...")
    raw_pools = fetch_raw_pools_data()
    filtered_pools = filter_pools_by_projects(raw_pools, TARGET_PROJECTS)
    pool_ids = get_pool_ids_from_pools(filtered_pools)

    # Extract: Fetch TVL data
    logger.info("Step 2: Extracting TVL data...")
    raw_tvl = fetch_raw_tvl_data(pool_ids)

    # Transform: Convert to historical TVL format
    logger.info("Step 3: Transforming to historical TVL...")
    tvl_data = transform_raw_tvl_to_historical_tvl(raw_tvl)
    tvl_data = sort_by_timestamp(tvl_data, descending=False)

    # Load: Save data
    logger.info("Step 4: Saving TVL data...")
    save_tvl_data(tvl_data)

    # Get summary stats
    stats = get_summary_stats(tvl_data, "historical_tvl")
    logger.info(f"✅ TVL pipeline completed: {stats['total_records']} records")

    return tvl_data


def run_historical_facts_pipeline(
    tvl_file: Optional[str] = None, scd2_file: Optional[str] = None
) -> pl.DataFrame:
    """
    Run the historical facts pipeline: load -> transform -> load

    Args:
        tvl_file: Optional TVL file path
        scd2_file: Optional SCD2 file path

    Returns:
        pl.DataFrame: Historical facts data
    """
    logger.info("🔄 Running historical facts pipeline...")

    # Load: Get TVL and SCD2 data
    logger.info("Step 1: Loading TVL and SCD2 data...")

    if tvl_file and file_exists(tvl_file):
        tvl_data = pl.read_parquet(tvl_file)
        logger.info(f"Loaded TVL data from {tvl_file}: {tvl_data.height} records")
    else:
        # Use latest TVL file
        latest_tvl = get_latest_file("tvl_data_*.parquet")
        if not latest_tvl:
            raise FileNotFoundError("No TVL data file found")
        tvl_data = pl.read_parquet(latest_tvl)
        logger.info(f"Loaded TVL data from {latest_tvl}: {tvl_data.height} records")

    if scd2_file and file_exists(scd2_file):
        scd2_data = pl.read_parquet(scd2_file)
        logger.info(f"Loaded SCD2 data from {scd2_file}: {scd2_data.height} records")
    else:
        # Use SCD2 file
        scd2_path = "output/pool_dim_scd2.parquet"
        if not file_exists(scd2_path):
            raise FileNotFoundError("No SCD2 data file found")
        scd2_data = pl.read_parquet(scd2_path)
        logger.info(f"Loaded SCD2 data from {scd2_path}: {scd2_data.height} records")

    # Transform: Create historical facts
    logger.info("Step 2: Creating historical facts...")
    historical_facts = create_historical_facts(tvl_data, scd2_data)

    # Load: Save historical facts
    logger.info("Step 3: Saving historical facts...")
    save_historical_facts_data(historical_facts)

    # Get summary stats
    stats = get_summary_stats(historical_facts, "historical_facts")
    logger.info(
        f"✅ Historical facts pipeline completed: {stats['total_records']} records"
    )

    return historical_facts


def run_dune_upload_pipeline(historical_facts_file: Optional[str] = None) -> bool:
    """
    Run the Dune upload pipeline: load -> upload

    Args:
        historical_facts_file: Optional historical facts file path

    Returns:
        bool: True if successful
    """
    logger.info("🔄 Running Dune upload pipeline...")

    # Load: Get historical facts data
    logger.info("Step 1: Loading historical facts data...")

    if historical_facts_file and file_exists(historical_facts_file):
        historical_facts = pl.read_parquet(historical_facts_file)
        logger.info(
            f"Loaded historical facts from {historical_facts_file}: {historical_facts.height} records"
        )
    else:
        # Use latest historical facts file
        latest_facts = get_latest_file("historical_facts_*.parquet")
        if not latest_facts:
            raise FileNotFoundError("No historical facts file found")
        historical_facts = pl.read_parquet(latest_facts)
        logger.info(
            f"Loaded historical facts from {latest_facts}: {historical_facts.height} records"
        )

    # Load: Upload to Dune
    logger.info("Step 2: Uploading to Dune...")

    # Create Dune uploader
    dune_uploader = DuneUploader()

    # Define table schema
    schema = {
        "timestamp": "date",
        "pool_old_clean": "string",
        "pool_id": "string",
        "protocol_slug": "string",
        "chain": "string",
        "symbol": "string",
        "tvl_usd": "double",
        "apy": "double",
        "apy_base": "double",
        "apy_reward": "double",
        "valid_from": "date",
        "valid_to": "date",
        "is_current": "boolean",
        "attrib_hash": "string",
        "is_active": "boolean",
    }

    # Create table
    dune_uploader.create_table("defillama_historical_facts", schema)

    # Upload data
    data = historical_facts.to_dicts()
    dune_uploader.upload_data("defillama_historical_facts", data)

    logger.info(f"✅ Dune upload pipeline completed: {len(data)} records uploaded")
    return True


def run_full_pipeline() -> Dict[str, Any]:
    """
    Run the complete pipeline: current state -> TVL -> historical facts -> Dune upload

    Returns:
        Dict: Pipeline results and statistics
    """
    logger.info("🚀 Running full pipeline...")

    results = {}

    try:
        # Step 1: Current state pipeline
        logger.info("=" * 50)
        logger.info("STEP 1: CURRENT STATE PIPELINE")
        logger.info("=" * 50)
        current_state, scd2_df = run_current_state_pipeline()
        results["current_state"] = {
            "records": current_state.height,
            "pools": current_state.select(pl.col("pool").n_unique()).item(),
        }

        # Step 2: TVL pipeline
        logger.info("=" * 50)
        logger.info("STEP 2: TVL PIPELINE")
        logger.info("=" * 50)
        tvl_data = run_tvl_pipeline()
        results["tvl"] = {
            "records": tvl_data.height,
            "pools": tvl_data.select(pl.col("pool_id").n_unique()).item(),
        }

        # Step 3: Historical facts pipeline
        logger.info("=" * 50)
        logger.info("STEP 3: HISTORICAL FACTS PIPELINE")
        logger.info("=" * 50)
        historical_facts = run_historical_facts_pipeline()
        results["historical_facts"] = {
            "records": historical_facts.height,
            "pools": historical_facts.select(pl.col("pool_id").n_unique()).item(),
        }

        # Step 4: Dune upload pipeline
        logger.info("=" * 50)
        logger.info("STEP 4: DUNE UPLOAD PIPELINE")
        logger.info("=" * 50)
        dune_success = run_dune_upload_pipeline()
        results["dune_upload"] = {"success": dune_success}

        logger.info("🎉 Full pipeline completed successfully!")
        return results

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        results["error"] = str(e)
        raise

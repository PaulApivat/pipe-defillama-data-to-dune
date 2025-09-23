import polars as pl
import os
from pathlib import Path
from datetime import date
from src.datasources.defillama.yieldpools.historical_tvl import YieldPoolsTVLFact
from src.datasources.defillama.yieldpools.current_state import YieldPoolsCurrentState
from src.coreutils.logging import setup_logging

# setup logging
logger = setup_logging()

TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
}


def fetch_tvl_functional():
    """Fetch historical TVL with functional pipeline"""
    logger.info("Fetching historical TVL with functional pipeline...")

    try:
        # load metadata (current state data)
        logger.info("Loading metadata from current state...")
        metadata = YieldPoolsCurrentState.fetch().filter_by_projects(TARGET_PROJECTS)

        # get today's date
        today = date.today().strftime("%Y-%m-%d")

        # check for existing TVL data for incremental update
        existing_tvl_file = f"output/tvl_data_{today}.parquet"

        # fetch historical TVL data (incremental if existing data found)
        logger.info("Fetching historical TVL data for all pools in metadata...")

        # Check if we should use incremental or full fetch
        if os.path.exists(existing_tvl_file):
            logger.info("Using incremental TVL fetch...")
            tvl_data = (
                YieldPoolsTVLFact.fetch_incremental_tvl(
                    metadata_source=metadata, existing_tvl_file=existing_tvl_file
                )
                .validate_schema()
                .sort_by_timestamp(descending=False)
            )
        else:
            logger.info("Using full TVL fetch for all pools...")
            tvl_data = (
                YieldPoolsTVLFact.fetch_tvl_for_metadata_pools(metadata_source=metadata)
                .validate_schema()
                .sort_by_timestamp(descending=False)
            )

        # get summary stats
        stats = tvl_data.get_summary_stats()
        logger.info(f"DEBUG: Available stats keys: {list(stats.keys())}")
        logger.info(f"✅ Processed {stats['total_records']} TVL records")
        logger.info(f"   Unique Pools: {stats['unique_pools']}")
        logger.info(f"   Date Range: {stats['date_range']}")
        logger.info(f"   TVL Sum: ${stats['tvl_usd_sum']:,.0f}")
        logger.info(f"   Average APY: {stats['apy_mean']:.2f}%")
        logger.info(f"   Average APY Base: {stats['apy_base_mean']:.2f}%")
        logger.info(f"   Average APY Reward: {stats['apy_reward_mean']:.2f}%")

        # save data
        tvl_data.to_parquet(f"output/tvl_data_{today}.parquet")
        tvl_data.to_json(f"output/tvl_data_{today}.json")
        logger.info(f"✅ Data saved to output/tvl_data_{today}.*")

        return tvl_data

    except Exception as e:
        logger.error(f"❌ Error fetching_tvl_functional: {e}")
        raise


def fetch_tvl_with_historical_facts():
    """Fetch TVL data and create historical facts to be joined with SCD2 dimensions"""
    logger.info("🔄 Fetching historical TVL data and historical facts...")

    try:
        # load metadata (current state data)
        logger.info("Loading metadata from current state...")
        metadata = YieldPoolsCurrentState.fetch().filter_by_projects(TARGET_PROJECTS)

        # get today's date
        today = date.today().strftime("%Y-%m-%d")

        # check for existing TVL data for incremental update
        existing_tvl_file = f"output/tvl_data_{today}.parquet"

        # Fetch historical TVL data (incremental if existing data found)
        logger.info("Fetching historical TVL data for all pools in metadata...")

        # Check if we should use incremental or full fetch
        if os.path.exists(existing_tvl_file):
            logger.info("Using incremental TVL fetch...")
            tvl_data = (
                YieldPoolsTVLFact.fetch_incremental_tvl(
                    metadata_source=metadata, existing_tvl_file=existing_tvl_file
                )
                .validate_schema()
                .sort_by_timestamp(descending=False)
            )
        else:
            logger.info("Using full TVL fetch for all pools...")
            tvl_data = (
                YieldPoolsTVLFact.fetch_tvl_for_metadata_pools(metadata_source=metadata)
                .validate_schema()
                .sort_by_timestamp(descending=False)
            )

        # Load SCD2 dimension
        scd2_df = pl.read_parquet("output/pool_dim_scd2.parquet")

        # create historical facts
        historical_facts = tvl_data.create_historical_facts(scd2_df)

        # save historical facts
        today = date.today()
        filename = f"output/historical_facts_{today}.parquet"
        historical_facts.write_parquet(filename)
        logger.info(f"✅ Historical facts saved to {filename}")

        logger.info("✅ Historical facts created successfully")
        return tvl_data, historical_facts

    except Exception as e:
        logger.error(f"❌ Error creating historical facts: {e}")
        raise


def fetch_tvl_with_incremental_historical_facts():
    """Fetch TVL data and create incremental historical facts for today"""
    logger.info("🔄 Fetching TVL data and creating incremental historical facts...")

    try:
        # Load metadata (current state data)
        logger.info("Loading metadata from current state...")
        metadata = YieldPoolsCurrentState.fetch().filter_by_projects(TARGET_PROJECTS)

        # get today's date
        today = date.today().strftime("%Y-%m-%d")

        # check for existing TVL data for incremental update
        existing_tvl_file = f"output/tvl_data_{today}.parquet"

        # Fetch historical TVL data (incremental if existing data found)
        logger.info("Fetching historical TVL data for all pools in metadata...")

        # Check if we should use incremental or full fetch
        if os.path.exists(existing_tvl_file):
            logger.info("Using incremental TVL fetch...")
            tvl_data = (
                YieldPoolsTVLFact.fetch_incremental_tvl(
                    metadata_source=metadata, existing_tvl_file=existing_tvl_file
                )
                .validate_schema()
                .sort_by_timestamp(descending=False)
            )
        else:
            logger.info("Using full TVL fetch for all pools...")
            tvl_data = (
                YieldPoolsTVLFact.fetch_tvl_for_metadata_pools(metadata_source=metadata)
                .validate_schema()
                .sort_by_timestamp(descending=False)
            )

        # Load SCD2 dimensions
        scd2_df = pl.read_parquet("output/pool_dim_scd2.parquet")

        # Create incremental historical facts for today
        today = date.today()
        historical_facts = tvl_data.create_incremental_historical_facts(scd2_df, today)

        # Save incremental historical facts
        filename = f"output/historical_facts_{today}.parquet"
        historical_facts.write_parquet(filename)
        logger.info(f"✅ Incremental historical facts saved to {filename}")

        logger.info("✅ Incremental historical facts created successfully")
        return tvl_data, historical_facts

    except Exception as e:
        logger.error(f"❌ Error creating incremental historical facts: {e}")
        raise


if __name__ == "__main__":
    fetch_tvl_with_historical_facts()
    # -- previous main function: fetch_tvl_functional()

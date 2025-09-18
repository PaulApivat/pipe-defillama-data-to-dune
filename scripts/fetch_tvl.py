import polars as pl
from pathlib import Path
from datetime import date
from src.datasources.defillama.yieldpools.tvl_fact import YieldPoolsTVLFact
from src.datasources.defillama.yieldpools.pools_old import YieldPoolsCurrentState
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
        tvl_data = (
            YieldPoolsTVLFact.fetch_incremental_tvl(
                metadata_source=metadata, existing_tvl_file=existing_tvl_file
            )
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


if __name__ == "__main__":
    fetch_tvl_functional()

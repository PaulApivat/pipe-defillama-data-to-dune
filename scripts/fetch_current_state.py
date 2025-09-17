#!/usr/bin/env python3
"""
Fetch current state data from DeFiLlama PoolsOld API endpoint.
This replaces the old metadata approach with enhanced current state data.
"""

import polars as pl
from datetime import date
from src.datasources.defillama.yieldpools.pools_old import YieldPoolsCurrentState
from src.datasources.defillama.yieldpools.schemas import METADATA_SCHEMA
from src.coreutils.logging import setup_logging


logger = setup_logging()

# Same target projects as fetch_yields.py
TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
}


def fetch_current_state():
    """Fetch current state data for all pools, filtered by target projects."""
    logger.info("🔄 Fetching current state data from DeFiLlama PoolsOld API...")

    try:
        # create instance and apply functional pipeline
        current_state = (
            YieldPoolsCurrentState.fetch()
            .filter_by_projects(TARGET_PROJECTS)
            .transform_pool_old()
            .validate_schema(METADATA_SCHEMA)
            .sort_by_tvl(descending=True)
        )

        # Get summary stats
        stats = current_state.get_summary_stats()
        logger.info(f"✅ Processed {stats['total_pools']} pools")
        logger.info(f"   Protocols: {stats['protocol_slug_unique']}")
        logger.info(f"   Chains: {stats['chain_unique']}")
        logger.info(f"   Symbols: {stats['symbol_unique']}")
        logger.info(
            f"   Underlying Tokens: {stats['underlying_tokens_avg_count']} (avg), {stats['underlying_tokens_max_count']} (max)"
        )
        logger.info(
            f"   Reward Tokens: {stats['reward_tokens_avg_count']} (avg), {stats['reward_tokens_max_count']} (max)"
        )
        logger.info(f"   Timestamps: {stats['timestamp_unique']}")
        logger.info(f"   Total TVL: ${stats['tvl_usd_sum']:,.0f}")
        logger.info(f"   Average APY: {stats['apy_mean']:.2f}%")
        logger.info(f"   Average APY Base: {stats['apy_base_mean']:.2f}%")
        logger.info(f"   Average APY Reward: {stats['apy_reward_mean']:.2f}%")
        logger.info(f"   Pool Old: {stats['pool_old_unique']}")

        # Save data
        today = date.today().strftime("%Y-%m-%d")
        current_state.to_parquet(f"output/current_state_{today}.parquet")
        current_state.to_json(f"output/current_state_{today}.json")

        logger.info(f"✅ Data saved to output/current_state_{today}.*")

        return current_state

    except Exception as e:
        logger.error(f"❌ Error fetching current state data: {e}")
        raise


if __name__ == "__main__":
    fetch_current_state()

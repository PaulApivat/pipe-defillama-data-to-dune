#!/usr/bin/env python3
"""
Fetch current state data from DeFiLlama PoolsOld API endpoint.
This replaces the old metadata approach with enhanced current state data.
"""

import polars as pl
from datetime import date
from src.datasources.defillama.yieldpools.current_state import YieldPoolsCurrentState
from src.datasources.defillama.yieldpools.schemas import CURRENT_STATE_SCHEMA
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
    """Fetch current state data for all pools, filtered by target projects.
    Update SCD2 dimensions"""
    logger.info(
        "🔄 Fetching current state from DeFiLlama PoolsOld API and updating SCD2 dimensions..."
    )

    try:
        # create instance and apply functional pipeline
        current_state = (
            YieldPoolsCurrentState.fetch()
            .filter_by_projects(TARGET_PROJECTS)
            .transform_pool_old()
            .validate_schema(CURRENT_STATE_SCHEMA)
            .sort_by_tvl(descending=True)
        )

        # Update SCD2 dimensions
        snap_date = date.today()
        scd2_df = current_state.update_scd2_dimensions(snap_date)
        current_state.save_scd2_dimensions(scd2_df)

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

        # Return both current_state and scd2_df
        logger.info("✅ SCD2 dimensions updated successfully")
        return current_state, scd2_df

    except Exception as e:
        logger.error(
            f"❌ Error fetching current state and updating SCD2 dimensions: {e}"
        )
        raise


if __name__ == "__main__":
    fetch_current_state()

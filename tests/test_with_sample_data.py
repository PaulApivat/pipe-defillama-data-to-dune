#!/usr/bin/env python3
"""
Test script using sample data to validate SCD2 pipeline without API calls
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import polars as pl
from datetime import date
from src.datasources.defillama.yieldpools.schemas import (
    CURRENT_STATE_SCHEMA,
    HISTORICAL_TVL_SCHEMA,
    POOL_DIM_SCD2_SCHEMA,
    DAILY_METRICS_SCHEMA,
)
from src.datasources.defillama.yieldpools.scd2_manager import SCD2Manager
from src.coreutils.logging import setup_logging

logger = setup_logging()


def create_sample_current_state():
    """Create sample current state data matching CURRENT_STATE_SCHEMA"""
    return pl.DataFrame(
        {
            "pool": ["test-pool-1", "test-pool-2", "test-pool-3"],
            "protocol_slug": [
                "pancakeswap-amm",
                "pancakeswap-amm",
                "pancakeswap-amm-v3",
            ],
            "chain": ["Ethereum", "BSC", "Ethereum"],
            "symbol": ["USDC-ETH", "BNB-BUSD", "USDC-ETH"],
            "underlying_tokens": [["USDC", "ETH"], ["BNB", "BUSD"], ["USDC", "ETH"]],
            "reward_tokens": [["CAKE"], ["CAKE"], ["CAKE"]],
            "timestamp": [
                "2025-01-15T00:00:00Z",
                "2025-01-15T00:00:00Z",
                "2025-01-15T00:00:00Z",
            ],
            "tvl_usd": [1000000.0, 2000000.0, 500000.0],
            "apy": [5.5, 8.2, 12.1],
            "apy_base": [3.0, 5.0, 7.0],
            "apy_reward": [2.5, 3.2, 5.1],
            "pool_old": ["test-pool-1", "test-pool-2", "test-pool-3"],
        }
    )


def create_sample_tvl_data():
    """Create sample TVL data matching HISTORICAL_TVL_SCHEMA"""
    return pl.DataFrame(
        {
            "timestamp": [
                "2025-01-15T00:00:00Z",
                "2025-01-15T00:00:00Z",
                "2025-01-15T00:00:00Z",
                "2025-01-16T00:00:00Z",
                "2025-01-16T00:00:00Z",
                "2025-01-16T00:00:00Z",
            ],
            "pool_id": [
                "test-pool-1",
                "test-pool-2",
                "test-pool-3",
                "test-pool-1",
                "test-pool-2",
                "test-pool-3",
            ],
            "tvl_usd": [1000000.0, 2000000.0, 500000.0, 1050000.0, 2100000.0, 520000.0],
            "apy": [5.5, 8.2, 12.1, 5.7, 8.5, 12.3],
            "apy_base": [3.0, 5.0, 7.0, 3.2, 5.2, 7.1],
            "apy_reward": [2.5, 3.2, 5.1, 2.5, 3.3, 5.2],
        }
    )


def test_scd2_pipeline():
    """Test SCD2 pipeline with sample data"""
    logger.info("🧪 Testing SCD2 pipeline with sample data...")

    # Create sample data
    sample_current_state = create_sample_current_state()
    sample_tvl = create_sample_tvl_data()

    logger.info(f"📊 Sample current state: {sample_current_state.shape[0]} rows")
    logger.info(f"📊 Sample TVL data: {sample_tvl.shape[0]} rows")

    # Test SCD2 dimension update
    logger.info("🔄 Testing SCD2 dimension update...")
    snap_date = date(2025, 1, 15)

    with SCD2Manager() as scd2_manager:
        # Register the current state data
        scd2_manager.register_dataframes(sample_current_state, None)

        # Update SCD2 dimensions
        scd2_df = scd2_manager.update_scd2_dimension_sql(snap_date)

    logger.info(f"✅ SCD2 dimensions created: {scd2_df.shape[0]} rows")
    logger.info(f"   Schema: {scd2_df.schema}")

    # Test daily metrics creation
    logger.info("🔄 Testing daily metrics creation...")

    with SCD2Manager() as scd2_manager:
        # Register both TVL data and SCD2 dimensions
        scd2_manager.register_dataframes(sample_current_state, scd2_df)
        daily_metrics = scd2_manager.create_daily_metrics_view_sql(sample_tvl)

    logger.info(f"✅ Daily metrics created: {daily_metrics.shape[0]} rows")
    logger.info(f"   Schema: {daily_metrics.schema}")

    # Validate schemas
    logger.info("🔍 Validating schemas...")

    # Check SCD2 schema
    expected_scd2_columns = set(POOL_DIM_SCD2_SCHEMA.keys())
    actual_scd2_columns = set(scd2_df.columns)
    if expected_scd2_columns == actual_scd2_columns:
        logger.info("✅ SCD2 schema validation passed")
    else:
        logger.error(
            f"❌ SCD2 schema mismatch: {expected_scd2_columns - actual_scd2_columns}"
        )

    # Check daily metrics schema
    expected_daily_columns = set(DAILY_METRICS_SCHEMA.keys())
    actual_daily_columns = set(daily_metrics.columns)
    if expected_daily_columns == actual_daily_columns:
        logger.info("✅ Daily metrics schema validation passed")
    else:
        logger.error(
            f"❌ Daily metrics schema mismatch: {expected_daily_columns - actual_daily_columns}"
        )

    # Show sample results
    logger.info("📋 Sample SCD2 data:")
    logger.info(scd2_df.head(2).to_pandas().to_string())

    logger.info("📋 Sample daily metrics data:")
    logger.info(daily_metrics.head(2).to_pandas().to_string())

    logger.info("✅ Sample data test completed successfully!")
    return scd2_df, daily_metrics


def test_scheduler_with_sample_data():
    """Test scheduler methods with sample data (dry run)"""
    logger.info("🧪 Testing scheduler with sample data...")

    from src.scheduler.scd2_scheduler import SCD2Scheduler

    # Create scheduler in dry run mode
    scheduler = SCD2Scheduler(dry_run=True)

    # Test weekly update
    logger.info("📅 Testing weekly dimension update...")
    scheduler.run_weekly_dimension_update()

    # Test daily update
    logger.info("📅 Testing daily fact update...")
    scheduler.run_daily_fact_update()

    logger.info("✅ Scheduler test completed!")


if __name__ == "__main__":
    # Test SCD2 pipeline
    scd2_df, daily_metrics = test_scd2_pipeline()

    print("\n" + "=" * 50 + "\n")

    # Test scheduler
    test_scheduler_with_sample_data()

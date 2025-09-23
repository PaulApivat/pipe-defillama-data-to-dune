#!/usr/bin/env python3
"""
SCD2 Fix Test

This test specifically regenerates the SCD2 data with the corrected valid_from date
and verifies that the join now works correctly with all historical TVL data.
"""

import sys
import os
import polars as pl
import duckdb
from datetime import date

# Add project root to path
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.datasources.defillama.yieldpools.current_state import YieldPoolsCurrentState
from src.datasources.defillama.yieldpools.scd2_manager import SCD2Manager
from src.coreutils.logging import setup_logging

logger = setup_logging()

# Test configuration
TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
}


def test_scd2_fix():
    """Test SCD2 fix with corrected valid_from date"""
    logger.info("🧪 Testing SCD2 Fix")

    # Load existing TVL data
    today = date.today()
    tvl_file = f"output/tvl_data_{today}.parquet"

    # If today's file doesn't exist, use the most recent one
    if not os.path.exists(tvl_file):
        import glob

        tvl_files = glob.glob("output/tvl_data_*.parquet")
        if tvl_files:
            tvl_file = max(tvl_files, key=os.path.getctime)
            logger.info(f"Using most recent TVL file: {tvl_file}")
        else:
            logger.error("❌ No TVL files found")
            return False

    if not os.path.exists(tvl_file):
        logger.error(f"❌ TVL file not found: {tvl_file}")
        logger.info("Please run the pipeline first to generate test data")
        return False

    # Load TVL data
    logger.info("Loading existing TVL data...")
    tvl_df = pl.read_parquet(tvl_file)
    logger.info(f"TVL data: {tvl_df.height} records")

    # Get date range from TVL data
    min_date = tvl_df.select(pl.col("timestamp").min()).item()
    max_date = tvl_df.select(pl.col("timestamp").max()).item()
    logger.info(f"TVL date range: {min_date} to {max_date}")

    # Fetch current state and create SCD2 dimensions with corrected logic
    logger.info("Fetching current state and creating SCD2 dimensions...")
    current_state = YieldPoolsCurrentState.fetch().filter_by_projects(TARGET_PROJECTS)

    # Create SCD2 dimensions with corrected valid_from date
    with SCD2Manager() as scd2_manager:
        scd2_manager.register_dataframes(current_state.df, None)
        scd2_df = scd2_manager.update_scd2_dimension_sql(today)

    # Check the valid_from dates
    valid_from_min = scd2_df.select(pl.col("valid_from").min()).item()
    valid_from_max = scd2_df.select(pl.col("valid_from").max()).item()
    logger.info(f"SCD2 valid_from range: {valid_from_min} to {valid_from_max}")

    # Save corrected SCD2 data
    scd2_file = "output/pool_dim_scd2_fixed.parquet"
    scd2_df.write_parquet(scd2_file)
    logger.info(f"✅ Corrected SCD2 data saved to {scd2_file}")

    # Test join with corrected SCD2 data
    logger.info("Testing join with corrected SCD2 data...")
    conn = duckdb.connect(":memory:")
    conn.register("tvl_data", tvl_df)
    conn.register("scd2_data", scd2_df)

    # Test the join query
    join_query = """
    WITH tvl_with_date AS (
        SELECT
            pool_id,
            tvl_usd,
            apy,
            apy_base,
            apy_reward,
            timestamp::DATE as date
        FROM tvl_data
    )
    SELECT
        t.date as timestamp,
        SPLIT_PART(d.pool_old, '-', 1) as pool_old_clean,
        t.pool_id,
        d.protocol_slug,
        d.chain,
        d.symbol,
        t.tvl_usd,
        t.apy,
        t.apy_base,
        t.apy_reward,
        d.valid_from,
        d.valid_to,
        d.is_current,
        d.attrib_hash,
        d.is_active
    FROM tvl_with_date t
    JOIN scd2_data d ON (
        t.pool_id = d.pool_id AND 
        t.date >= d.valid_from AND 
        t.date < d.valid_to
    )
    ORDER BY t.date, t.pool_id
    """

    result_df = conn.execute(join_query).pl()

    logger.info(f"✅ Join completed: {result_df.height} records")

    # Verify join results
    logger.info("\n🔍 Join Verification:")
    logger.info(f"   TVL records: {tvl_df.height}")
    logger.info(f"   SCD2 records: {scd2_df.height}")
    logger.info(f"   Join result: {result_df.height}")

    # Check if all TVL records have matching dimensions
    tvl_pool_ids = set(tvl_df.select("pool_id").to_series().to_list())
    result_pool_ids = set(result_df.select("pool_id").to_series().to_list())

    logger.info(f"   TVL unique pools: {len(tvl_pool_ids)}")
    logger.info(f"   Result unique pools: {len(result_pool_ids)}")

    if tvl_pool_ids == result_pool_ids:
        logger.info("   ✅ All TVL pools have matching dimension records")
        success = True
    else:
        missing_pools = tvl_pool_ids - result_pool_ids
        logger.warning(
            f"   ⚠️  Missing dimension records for {len(missing_pools)} pools"
        )
        logger.warning(f"   Missing pools: {list(missing_pools)[:5]}...")
        success = False

    # Check date range
    if result_df.height > 0:
        result_min_date = result_df.select(pl.col("timestamp").min()).item()
        result_max_date = result_df.select(pl.col("timestamp").max()).item()
        logger.info(f"\n📅 Result Date Range: {result_min_date} to {result_max_date}")

        # Check if we now have historical data
        if result_min_date < date(2025, 9, 22):
            logger.info("   ✅ Historical data is now included in the join!")
        else:
            logger.warning("   ⚠️  Still only getting recent data")

    # Save result for inspection
    output_file = f"output/scd2_fix_test_{today}.parquet"
    result_df.write_parquet(output_file)
    logger.info(f"💾 Join result saved to: {output_file}")

    conn.close()

    if success:
        logger.info(
            "🎉 SCD2 fix test passed! All TVL records now have matching dimension records."
        )
    else:
        logger.error(
            "❌ SCD2 fix test failed. Some TVL records still don't have matching dimension records."
        )

    return success


def main():
    """Run the SCD2 fix test"""
    logger.info("🧪 Starting SCD2 Fix Test")

    try:
        success = test_scd2_fix()
        if success:
            logger.info("✅ SCD2 fix test completed successfully!")
        else:
            logger.error("❌ SCD2 fix test failed!")
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        raise


if __name__ == "__main__":
    main()

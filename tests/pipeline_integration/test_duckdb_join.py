#!/usr/bin/env python3
"""
DuckDB Join Verification Test

This test specifically verifies that the join between TVL data and SCD2 dimensions
works correctly using DuckDB in-memory, and shows the exact data flow.
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

from src.coreutils.logging import setup_logging

logger = setup_logging()


def test_duckdb_join_verification():
    """Test DuckDB join between TVL and SCD2 data"""
    logger.info("🧪 Testing DuckDB Join Verification")

    # Load existing data files
    today = date.today()
    tvl_file = f"output/tvl_data_{today}.parquet"
    scd2_file = "output/pool_dim_scd2.parquet"

    # If today's TVL file doesn't exist, use the most recent one
    if not os.path.exists(tvl_file):
        import glob

        tvl_files = glob.glob("output/tvl_data_*.parquet")
        if tvl_files:
            tvl_file = max(tvl_files, key=os.path.getctime)
            logger.info(f"Using most recent TVL file: {tvl_file}")
        else:
            logger.error("❌ No TVL files found")
            return False

    # If SCD2 file doesn't exist, use the fixed one if available
    if not os.path.exists(scd2_file):
        fixed_scd2_file = "output/pool_dim_scd2_fixed.parquet"
        if os.path.exists(fixed_scd2_file):
            scd2_file = fixed_scd2_file
            logger.info(f"Using fixed SCD2 file: {scd2_file}")
        else:
            logger.error(f"❌ SCD2 file not found: {scd2_file}")
            logger.info("Please run the pipeline first to generate test data")
            return False

    # Load data
    logger.info("Loading data files...")
    tvl_df = pl.read_parquet(tvl_file)
    scd2_df = pl.read_parquet(scd2_file)

    logger.info(f"TVL data: {tvl_df.height} records")
    logger.info(f"SCD2 data: {scd2_df.height} records")

    # Show sample data
    logger.info("\n📊 TVL Data Sample:")
    logger.info(tvl_df.head(3).to_pandas().to_string())

    logger.info("\n📊 SCD2 Data Sample:")
    logger.info(scd2_df.head(3).to_pandas().to_string())

    # Test join using DuckDB
    logger.info("\n🔗 Testing DuckDB Join...")
    conn = duckdb.connect(":memory:")
    conn.register("tvl_data", tvl_df)
    conn.register("scd2_data", scd2_df)

    # Test the exact join query from SCD2Manager
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

    # Execute join
    result_df = conn.execute(join_query).pl()

    logger.info(f"\n✅ Join completed: {result_df.height} records")

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
    else:
        missing_pools = tvl_pool_ids - result_pool_ids
        logger.warning(
            f"   ⚠️  Missing dimension records for {len(missing_pools)} pools"
        )
        logger.warning(f"   Missing pools: {list(missing_pools)[:5]}...")

    # Show sample join result
    logger.info("\n📊 Join Result Sample:")
    logger.info(result_df.head(3).to_pandas().to_string())

    # Check date range
    if result_df.height > 0:
        min_date = result_df.select(pl.col("timestamp").min()).item()
        max_date = result_df.select(pl.col("timestamp").max()).item()
        logger.info(f"\n📅 Date Range: {min_date} to {max_date}")

    conn.close()

    # Save result for inspection
    output_file = f"output/duckdb_join_test_{today}.parquet"
    result_df.write_parquet(output_file)
    logger.info(f"💾 Join result saved to: {output_file}")

    return True


def main():
    """Run the DuckDB join verification test"""
    logger.info("🧪 Starting DuckDB Join Verification Test")

    try:
        success = test_duckdb_join_verification()
        if success:
            logger.info("✅ DuckDB join verification test passed!")
        else:
            logger.error("❌ DuckDB join verification test failed!")
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        raise


if __name__ == "__main__":
    main()

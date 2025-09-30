#!/usr/bin/env python3
"""
Debug the join between TVL data and dimensions
"""

import os
import sys
import polars as pl
import duckdb
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transformation.transformers import (
    create_pool_dimensions,
    filter_pools_by_projects,
)

# Load environment variables
load_dotenv()


def test_join_debug():
    """Debug the join between TVL data and dimensions"""

    print("🔍 Debugging Join Between TVL Data and Dimensions")
    print("=" * 60)

    try:
        # Load the data from GitHub workflow
        print("1️⃣ Loading data from GitHub workflow...")
        raw_pools_df = pl.read_parquet("output/raw_pools_2025-09-30.parquet")
        raw_tvl_df = pl.read_parquet("output/raw_tvl_2025-09-30.parquet")

        print(f"✅ Loaded {raw_pools_df.height} pools, {raw_tvl_df.height} TVL records")

        # Create dimensions
        print("\n2️⃣ Creating dimensions...")
        dimensions_df = create_pool_dimensions(raw_pools_df)
        print(f"✅ Created {dimensions_df.height} dimension records")

        # Filter dimensions
        print("\n3️⃣ Filtering dimensions...")
        target_projects = [
            "uniswap-v3",
            "uniswap-v2",
            "uniswap-v1",
            "pancakeswap-amm",
            "aerodrome-v1",
            "aerodrome-slipstream",
            "fluid-dex",
            "curve-dex",
            "pancakeswap-amm-v3",
        ]
        filtered_dimensions_df = filter_pools_by_projects(
            dimensions_df, target_projects
        )
        print(f"✅ Filtered to {filtered_dimensions_df.height} dimension records")

        # Check pool_id overlap
        print("\n4️⃣ Checking pool_id overlap...")
        tvl_pool_ids = set(raw_tvl_df["pool_id"].to_list())
        dim_pool_ids = set(filtered_dimensions_df["pool_id"].to_list())

        overlap = tvl_pool_ids.intersection(dim_pool_ids)
        tvl_only = tvl_pool_ids - dim_pool_ids
        dim_only = dim_pool_ids - tvl_pool_ids

        print(f"📊 TVL pool_ids: {len(tvl_pool_ids):,}")
        print(f"📊 Dimension pool_ids: {len(dim_pool_ids):,}")
        print(f"📊 Overlap: {len(overlap):,}")
        print(f"📊 TVL only: {len(tvl_only):,}")
        print(f"📊 Dimensions only: {len(dim_only):,}")

        if len(overlap) == 0:
            print("❌ NO OVERLAP! This is the problem!")
            print("🔍 Sample TVL pool_ids:", list(tvl_pool_ids)[:5])
            print("🔍 Sample Dimension pool_ids:", list(dim_pool_ids)[:5])
        else:
            print(f"✅ {len(overlap):,} pool_ids match - join should work")

        # Test the actual join
        print("\n5️⃣ Testing actual join...")
        conn = duckdb.connect()
        conn.register("tvl_data", raw_tvl_df)
        conn.register("pool_dimensions", filtered_dimensions_df)

        # Simple join test
        join_test_sql = """
            SELECT COUNT(*) as join_count
            FROM tvl_data t
            JOIN pool_dimensions d ON t.pool_id = d.pool_id
        """

        join_result = conn.execute(join_test_sql).fetchone()
        join_count = join_result[0] if join_result else 0

        print(f"📊 Join result: {join_count:,} records")

        if join_count == 0:
            print("❌ JOIN FAILED - No matching records!")
        elif join_count < 1000:
            print(f"⚠️  JOIN PARTIAL - Only {join_count:,} records joined")
        else:
            print(f"✅ JOIN SUCCESS - {join_count:,} records joined")

        conn.close()

        return join_count > 0

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_join_debug()
    print(f"\n{'✅ Join working' if success else '❌ Join failing'}")

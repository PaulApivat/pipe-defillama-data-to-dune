#!/usr/bin/env python3
"""
Debug script to test historical facts creation
"""

import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract.data_fetcher import fetch_raw_pools_data, fetch_raw_tvl_data
from src.transformation.transformers import (
    create_pool_dimensions,
    create_historical_facts,
    filter_pools_by_projects,
)

# Load environment variables
load_dotenv()


def test_historical_facts_creation():
    """Test the historical facts creation process"""

    print("🔍 Testing Historical Facts Creation")
    print("=" * 50)

    try:
        # Step 1: Load raw data (use existing files if available)
        print("1️⃣ Loading raw data...")

        # Check if we have recent raw data files
        import glob

        raw_pools_files = glob.glob("output/raw_pools_*.parquet")
        raw_tvl_files = glob.glob("output/raw_tvl_*.parquet")

        if raw_pools_files and raw_tvl_files:
            # Use most recent files
            latest_pools = max(raw_pools_files, key=os.path.getctime)
            latest_tvl = max(raw_tvl_files, key=os.path.getctime)

            print(f"📁 Using existing files:")
            print(f"   Pools: {latest_pools}")
            print(f"   TVL: {latest_tvl}")

            import polars as pl

            raw_pools_df = pl.read_parquet(latest_pools)
            raw_tvl_df = pl.read_parquet(latest_tvl)

        else:
            print("📥 Fetching fresh data...")
            raw_pools_df = fetch_raw_pools_data()
            raw_tvl_df = fetch_raw_tvl_data(raw_pools_df["pool_id"].to_list())

        print(f"✅ Loaded {raw_pools_df.height} pools, {raw_tvl_df.height} TVL records")

        # Step 2: Create dimensions
        print("\n2️⃣ Creating pool dimensions...")
        dimensions_df = create_pool_dimensions(raw_pools_df)
        print(f"✅ Created {dimensions_df.height} dimension records")

        # Step 3: Filter dimensions
        print("\n3️⃣ Filtering dimensions by target projects...")
        target_projects = ["uniswap-v3", "uniswap-v2", "uniswap-v1"]
        filtered_dimensions_df = filter_pools_by_projects(
            dimensions_df, target_projects
        )
        print(f"✅ Filtered to {filtered_dimensions_df.height} dimension records")

        # Step 4: Create historical facts
        print("\n4️⃣ Creating historical facts...")
        historical_facts_df = create_historical_facts(
            raw_tvl_df, filtered_dimensions_df, None  # No date filter
        )
        print(f"✅ Created {historical_facts_df.height} historical fact records")

        # Step 5: Show sample data
        print("\n5️⃣ Sample historical facts:")
        print(historical_facts_df.head(3))

        # Step 6: Check schema
        print(f"\n6️⃣ Schema: {historical_facts_df.schema}")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_historical_facts_creation()
    print(f"\n{'✅ Test passed' if success else '❌ Test failed'}")

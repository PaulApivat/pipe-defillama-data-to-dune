"""
Test Extract Layer - Verify pure I/O functions work correctly and save data
"""

import sys
import os
from datetime import date
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from src.extract.data_fetcher import (
    fetch_raw_pools_data,
    fetch_raw_tvl_data,
    filter_pools_by_projects,
    get_pool_ids_from_pools,
    TARGET_PROJECTS,
)
from src.extract.schemas import RAW_POOLS_SCHEMA, RAW_TVL_SCHEMA
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_extract_layer():
    """Test that Extract layer functions work correctly"""

    print("🧪 Testing Extract Layer...")

    try:
        # Test 1: Fetch raw pools data
        print("\n1️⃣ Testing fetch_raw_pools_data()...")
        pools_df = fetch_raw_pools_data()
        print(f"✅ Fetched {pools_df.height} raw pool records")
        print(f"   Columns: {pools_df.columns}")
        print(f"   Sample data: {pools_df.head(2)}")

        # Verify schema compliance
        print("\n🔍 Verifying schema compliance...")
        if pools_df.schema == RAW_POOLS_SCHEMA:
            print("✅ Schema matches RAW_POOLS_SCHEMA exactly")
        else:
            print("❌ Schema mismatch detected:")
            print(f"   Expected: {RAW_POOLS_SCHEMA}")
            print(f"   Actual:   {pools_df.schema}")
            return False

        # Verify only target projects are included
        print("\n🔍 Verifying target project filtering...")
        unique_protocols = (
            pools_df.select("protocol_slug").unique().to_series().to_list()
        )
        unexpected_protocols = [p for p in unique_protocols if p not in TARGET_PROJECTS]

        if unexpected_protocols:
            print(f"❌ ERROR: Found unexpected protocols: {unexpected_protocols}")
            print(f"   Expected only: {TARGET_PROJECTS}")
            return False
        else:
            print(f"✅ All protocols are in target projects: {unique_protocols}")

        # Verify data types
        print("\n🔍 Verifying data types...")
        tvl_usd_type = pools_df.select("tvl_usd").dtypes[0]
        pool_old_type = pools_df.select("pool_old").dtypes[0]

        if tvl_usd_type != pl.Float64:
            print(f"❌ ERROR: tvl_usd type is {tvl_usd_type}, expected Float64")
            return False
        else:
            print("✅ tvl_usd type is correct (Float64)")

        if pool_old_type != pl.String:
            print(f"❌ ERROR: pool_old type is {pool_old_type}, expected String")
            return False
        else:
            print("✅ pool_old type is correct (String)")

        # Verify pools data was saved
        today = date.today().strftime("%Y-%m-%d")
        expected_pools_file = f"output/raw_pools_{today}.parquet"
        if os.path.exists(expected_pools_file):
            print(f"✅ Verified: Raw pools data saved to {expected_pools_file}")
            # Verify the saved file can be loaded
            saved_pools = pl.read_parquet(expected_pools_file)
            print(f"   Saved file contains {saved_pools.height} records")
        else:
            print(f"❌ ERROR: Raw pools data not saved to {expected_pools_file}")
            return False

        # Test 2: Filter pools by projects
        print("\n2️⃣ Testing filter_pools_by_projects()...")
        filtered_df = filter_pools_by_projects(pools_df)
        print(f"✅ Filtered to {filtered_df.height} pools")
        print(f"   Target projects: {TARGET_PROJECTS}")

        # Test 3: Extract pool IDs
        print("\n3️⃣ Testing get_pool_ids_from_pools()...")
        pool_ids = get_pool_ids_from_pools(filtered_df)
        print(f"✅ Extracted {len(pool_ids)} pool IDs")
        print(f"   Sample IDs: {pool_ids[:3]}")

        # Test 4: Fetch raw TVL data (FULL HISTORICAL DATASET)
        print("\n4️⃣ Testing fetch_raw_tvl_data() with FULL dataset...")
        print("⚠️  This will fetch historical TVL data for ALL pools (1+ hours)...")
        print("⚠️  Press Ctrl+C to cancel if needed...")

        tvl_df = fetch_raw_tvl_data(pool_ids)  # Use ALL pool IDs, not just 3

        print(f"✅ Fetched {tvl_df.height} raw TVL records")
        print(f"   Columns: {tvl_df.columns}")
        print(f"   Sample data: {tvl_df.head(2)}")

        # Verify TVL schema compliance
        print("\n🔍 Verifying TVL schema compliance...")
        if tvl_df.schema == RAW_TVL_SCHEMA:
            print("✅ TVL schema matches RAW_TVL_SCHEMA exactly")
        else:
            print("❌ TVL schema mismatch detected:")
            print(f"   Expected: {RAW_TVL_SCHEMA}")
            print(f"   Actual:   {tvl_df.schema}")
            return False

        # Verify TVL data was saved
        expected_tvl_file = f"output/raw_tvl_{today}.parquet"
        if os.path.exists(expected_tvl_file):
            print(f"✅ Verified: Raw TVL data saved to {expected_tvl_file}")
            # Verify the saved file can be loaded
            saved_tvl = pl.read_parquet(expected_tvl_file)
            print(f"   Saved file contains {saved_tvl.height} records")
        else:
            print(f"❌ ERROR: Raw TVL data not saved to {expected_tvl_file}")
            return False

        # Test 5: Verify data integrity
        print("\n5️⃣ Testing data integrity...")

        # Check that saved pools data matches returned data
        if pools_df.equals(saved_pools):
            print("✅ Pools data integrity verified - saved data matches returned data")
        else:
            print(
                "❌ ERROR: Pools data integrity failed - saved data doesn't match returned data"
            )
            return False

        # Check that saved TVL data matches returned data
        if tvl_df.equals(saved_tvl):
            print("✅ TVL data integrity verified - saved data matches returned data")
        else:
            print(
                "❌ ERROR: TVL data integrity failed - saved data doesn't match returned data"
            )
            return False

        # Test 6: Verify file timestamps
        print("\n6️⃣ Testing file timestamps...")
        pools_mtime = os.path.getmtime(expected_pools_file)
        tvl_mtime = os.path.getmtime(expected_tvl_file)
        current_time = os.path.getmtime(".")

        if pools_mtime > current_time - 60:  # Within last minute
            print("✅ Pools file timestamp is recent")
        else:
            print("❌ ERROR: Pools file timestamp is too old")
            return False

        if tvl_mtime > current_time - 60:  # Within last minute
            print("✅ TVL file timestamp is recent")
        else:
            print("❌ ERROR: TVL file timestamp is too old")
            return False

        print("\n🎉 All Extract layer tests passed!")
        print(f"   ✅ Fetched {pools_df.height} pools and {tvl_df.height} TVL records")
        print(f"   ✅ Schema compliance verified")
        print(f"   ✅ Target project filtering verified")
        print(f"   ✅ Data types verified")
        print(f"   ✅ Saved data to {expected_pools_file} and {expected_tvl_file}")
        print(f"   ✅ Data integrity verified")
        print(f"   ✅ File timestamps are recent")

        print("\n🎉 All Extract layer tests passed!")
        return True

    except Exception as e:
        print(f"❌ Extract layer test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_extract_layer()

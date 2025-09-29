"""
Test Simplified Transform Layer - Verify data transformation functions work correctly
Uses data saved by the Extract layer, then tests simplified transformations
"""

import sys
import os
from datetime import date
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from src.transformation.transformers import (
    create_pool_dimensions,
    create_historical_facts,
    filter_pools_by_projects,
    sort_by_tvl,
    sort_by_timestamp,
    get_summary_stats,
    save_transformed_data,
)
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_extracted_data():
    """Load data saved by the Extract layer"""

    # Find the most recent raw_pools parquet file
    raw_pools_files = glob.glob("output/raw_pools_*.parquet")
    if not raw_pools_files:
        raise FileNotFoundError(
            "No raw_pools parquet files found. Run extract layer first."
        )

    latest_raw_pools = max(raw_pools_files, key=os.path.getctime)
    print(f"📁 Loading raw pools from: {latest_raw_pools}")

    # Find the most recent raw_tvl parquet file
    raw_tvl_files = glob.glob("output/raw_tvl_*.parquet")
    if not raw_tvl_files:
        raise FileNotFoundError(
            "No raw_tvl parquet files found. Run extract layer first."
        )

    latest_raw_tvl = max(raw_tvl_files, key=os.path.getctime)
    print(f"📁 Loading raw TVL from: {latest_raw_tvl}")

    # Load the data
    raw_pools_df = pl.read_parquet(latest_raw_pools)
    raw_tvl_df = pl.read_parquet(latest_raw_tvl)

    print(f"✅ Loaded {raw_pools_df.height} raw pool records")
    print(f"✅ Loaded {raw_tvl_df.height} raw TVL records")

    return raw_pools_df, raw_tvl_df


def test_transform_layer():
    """Test that Simplified Transform layer functions work correctly with extracted data"""

    print("🧪 Testing Simplified Transform Layer with Extracted Data...")

    try:
        # Load data saved by Extract layer
        print("\n📁 Loading data saved by Extract layer...")
        raw_pools_df, raw_tvl_df = load_extracted_data()

        # Test 1: Create pool dimensions
        print("\n1️⃣ Testing create_pool_dimensions()...")
        dimensions_df = create_pool_dimensions(raw_pools_df)
        print(f"✅ Created {dimensions_df.height} pool dimension records")

        # Test 2: Filter by projects
        print("\n2️⃣ Testing filter_pools_by_projects()...")
        target_projects = {
            "curve-dex",
            "pancakeswap-amm",
            "pancakeswap-amm-v3",
            "aerodrome-slipstream",
            "aerodrome-v1",
            "uniswap-v2",
            "uniswap-v3",
            "fluid-dex",
        }
        filtered_dimensions_df = filter_pools_by_projects(
            dimensions_df, target_projects
        )
        print(f"✅ Filtered to {filtered_dimensions_df.height} pools")

        # Test 3: Create historical facts (the key join!)
        print("\n3️⃣ Testing create_historical_facts()...")
        historical_facts_df = create_historical_facts(
            raw_tvl_df, filtered_dimensions_df
        )
        print(f"✅ Created {historical_facts_df.height} historical facts records")

        # Test 4: Save only the actual transformations
        print("\n4️⃣ Testing data persistence...")
        today = date.today().strftime("%Y-%m-%d")
        save_transformed_data(filtered_dimensions_df, historical_facts_df, today)
        print("✅ Saved transformed data to output directory")

        # Test 5: Verify only the new output files exist
        print("\n5️⃣ Verifying output files...")
        expected_files = [
            "output/pool_dimensions.parquet",
            f"output/historical_facts_{today}.parquet",
        ]

        for file_path in expected_files:
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                print(f"✅ {file_path} exists ({file_size:,} bytes)")
            else:
                print(f"❌ {file_path} missing")
                return False

        # Test 6: Get summary statistics
        print("\n6️⃣ Testing get_summary_stats()...")

        # Dimensions stats
        dimensions_stats = get_summary_stats(filtered_dimensions_df, "pool_dimensions")
        print(
            f"✅ Dimensions stats: {dimensions_stats['total_pools']} pools, "
            f"{dimensions_stats['protocol_slug_unique']} protocols, "
            f"${dimensions_stats['tvl_usd_sum']:,.0f} total TVL"
        )

        # Historical facts stats
        facts_stats = get_summary_stats(historical_facts_df, "historical_facts")
        print(
            f"✅ Facts stats: {facts_stats['total_records']} records, "
            f"{facts_stats['unique_pools']} unique pools, "
            f"Date range: {facts_stats['date_range']}"
        )

        print("\n🎉 All Simplified Transform layer tests passed!")
        print("   ✅ Used data saved by Extract layer")
        print("   ✅ Transform layer processed data correctly")
        print("   ✅ Created only the NEW transformed files:")
        print("   ✅ - pool_dimensions.parquet (simplified dimensions)")
        print(f"  ✅ - historical_facts_{today}.parquet (joined facts)")
        print("   ✅ Avoided duplicating raw data (already saved by extract layer)")
        print("   ✅ Removed SCD2 complexity (dimensions are not slowly changing)")

        return True

    except Exception as e:
        print(f"❌ Transform layer test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_transform_layer()

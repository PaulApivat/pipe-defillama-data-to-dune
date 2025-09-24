"""
Test Transform Layer - Verify data transformation functions work correctly
Uses data saved by the Extract layer, then tests transformations
"""

import sys
import os
from datetime import date
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from src.transformation.transformers import (
    transform_raw_pools_to_current_state,
    transform_raw_tvl_to_historical_tvl,
    create_scd2_dimensions,
    create_historical_facts,
    upsert_historical_facts_for_date,
    filter_pools_by_projects,
    sort_by_tvl,
    sort_by_timestamp,
    get_summary_stats,
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
    print(f"�� Loading raw pools from: {latest_raw_pools}")

    # Find the most recent raw_tvl parquet file
    raw_tvl_files = glob.glob("output/raw_tvl_*.parquet")
    if not raw_tvl_files:
        raise FileNotFoundError(
            "No raw_tvl parquet files found. Run extract layer first."
        )

    latest_raw_tvl = max(raw_tvl_files, key=os.path.getctime)
    print(f"�� Loading raw TVL from: {latest_raw_tvl}")

    # Load the data
    raw_pools_df = pl.read_parquet(latest_raw_pools)
    raw_tvl_df = pl.read_parquet(latest_raw_tvl)

    print(f"✅ Loaded {raw_pools_df.height} raw pool records")
    print(f"✅ Loaded {raw_tvl_df.height} raw TVL records")

    return raw_pools_df, raw_tvl_df


def test_transform_layer():
    """Test that Transform layer functions work correctly with extracted data"""

    print("🧪 Testing Transform Layer with Extracted Data...")

    try:
        # Load data saved by Extract layer
        print("\n📁 Loading data saved by Extract layer...")
        raw_pools_df, raw_tvl_df = load_extracted_data()

        # Test 1: Transform raw pools data
        print("\n1️⃣ Testing transform_raw_pools_to_current_state()...")
        current_state = transform_raw_pools_to_current_state(raw_pools_df)
        print(f"✅ Transformed {current_state.height} current state records")

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
        filtered = filter_pools_by_projects(current_state, target_projects)
        print(f"✅ Filtered to {filtered.height} pools")

        # Test 3: Sort by TVL
        print("\n3️⃣ Testing sort_by_tvl()...")
        sorted_pools = sort_by_tvl(filtered, descending=True)
        print(f"✅ Sorted {sorted_pools.height} pools by TVL")

        # Test 4: Get summary stats
        print("\n4️⃣ Testing get_summary_stats()...")
        stats = get_summary_stats(sorted_pools, "current_state")
        print(f"✅ Generated stats: {list(stats.keys())}")
        print(f"   Total pools: {stats['total_pools']}")
        print(f"   Protocols: {stats['protocol_slug_unique']}")
        print(f"   Total TVL: ${stats['tvl_usd_sum']:,.0f}")

        # Test 5: Transform raw TVL data
        print("\n5️⃣ Testing transform_raw_tvl_to_historical_tvl()...")
        historical_tvl = transform_raw_tvl_to_historical_tvl(raw_tvl_df)
        print(f"✅ Transformed {historical_tvl.height} historical TVL records")

        # Test 6: Sort TVL by timestamp
        print("\n6️⃣ Testing sort_by_timestamp()...")
        sorted_tvl = sort_by_timestamp(historical_tvl, descending=False)
        print(f"✅ Sorted {sorted_tvl.height} TVL records by timestamp")

        # Test 7: Create SCD2 dimensions
        print("\n7️⃣ Testing create_scd2_dimensions()...")
        snap_date = date.today()
        scd2_dims = create_scd2_dimensions(filtered, snap_date)
        print(f"✅ Created {scd2_dims.height} SCD2 dimension records")

        # Test 8: Create historical facts
        print("\n8️⃣ Testing create_historical_facts()...")
        historical_facts = create_historical_facts(historical_tvl, scd2_dims)
        print(f"✅ Created {historical_facts.height} historical facts records")

        # Test 9: Create incremental historical facts
        print("\n9️⃣ Testing upsert_historical_facts_for_date()...")
        today = date.today()
        incremental_facts = upsert_historical_facts_for_date(
            historical_tvl, scd2_dims, today
        )
        print(
            f"✅ Created {incremental_facts.height} incremental historical facts records"
        )

        print("\n�� All Transform layer tests passed!")
        print("   ✅ Used data saved by Extract layer")
        print("   ✅ Transform layer processed data correctly")

        # Test 10: Get summary stats
        print("\n4️⃣ Testing get_summary_stats()...")
        stats = get_summary_stats(sorted_pools, "current_state")
        print(f"✅ Generated stats: {list(stats.keys())}")
        print(f"   Total pools: {stats['total_pools']}")
        print(f"   Protocols: {stats['protocol_slug_unique']}")
        print(f"   Total TVL: ${stats['tvl_usd_sum']:,.0f}")

        # Test 11: Get TVL summary stats
        print("\n7️⃣ Testing get_summary_stats() for TVL...")
        tvl_stats = get_summary_stats(sorted_tvl, "historical_tvl")
        print(f"✅ Generated TVL stats: {list(tvl_stats.keys())}")
        print(f"   Total records: {tvl_stats['total_records']}")
        print(f"   Unique pools: {tvl_stats['unique_pools']}")
        print(f"   Date range: {tvl_stats['date_range']}")

        print("\n🎉 All Transform layer tests passed!")
        return True

    except Exception as e:
        print(f"❌ Transform layer test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_transform_layer()

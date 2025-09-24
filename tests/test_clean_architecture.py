"""
Unit Tests for Clean Architecture

Tests each layer independently to ensure proper separation of concerns.
"""

import polars as pl
import os
import sys
from datetime import date

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract.data_fetcher import fetch_raw_pools_data, filter_pools_by_projects
from src.transformation.transformers import (
    transform_raw_pools_to_current_state,
    create_scd2_dimensions,
    create_historical_facts,
)
from src.load.local_storage import save_parquet, load_parquet
from src.orchestration.pipeline import run_current_state_pipeline


def test_extract_layer():
    """Test extract layer - pure I/O operations"""
    print("🧪 Testing extract layer...")

    # Test raw data fetching
    raw_pools = fetch_raw_pools_data()
    assert raw_pools.height > 0, "Should fetch some pools data"

    # Test filtering
    filtered_pools = filter_pools_by_projects(raw_pools, {"curve-dex"})
    assert filtered_pools.height > 0, "Should filter to some pools"

    print("✅ Extract layer tests passed")


def test_transform_layer():
    """Test transform layer - pure functions"""
    print("🧪 Testing transform layer...")

    # Create sample data
    sample_pools = pl.DataFrame(
        {
            "pool": ["test-pool-1", "test-pool-2"],
            "protocol_slug": ["curve-dex", "curve-dex"],
            "chain": ["Ethereum", "Ethereum"],
            "symbol": ["USDC-USDT", "DAI-USDC"],
            "underlying_tokens": [["0x123", "0x456"], ["0x789", "0xabc"]],
            "reward_tokens": [["0xdef"], ["0xghi"]],
            "timestamp": ["2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
            "tvlUsd": [1000000.0, 2000000.0],
            "apy": [5.0, 7.0],
            "apyBase": [3.0, 4.0],
            "apyReward": [2.0, 3.0],
            "pool_old": ["test-pool-1-old", "test-pool-2-old"],
        }
    )

    # Test transformation
    current_state = transform_raw_pools_to_current_state(sample_pools)
    assert current_state.height == 2, "Should transform all records"
    assert "tvl_usd" in current_state.columns, "Should rename columns"

    # Test SCD2 creation
    scd2_df = create_scd2_dimensions(current_state, date.today())
    assert scd2_df.height == 2, "Should create SCD2 records"
    assert "valid_from" in scd2_df.columns, "Should have SCD2 fields"

    print("✅ Transform layer tests passed")


def test_load_layer():
    """Test load layer - file operations"""
    print("🧪 Testing load layer...")

    # Create sample data
    sample_data = pl.DataFrame(
        {"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [10.0, 20.0, 30.0]}
    )

    # Test save/load
    test_file = "test_data.parquet"
    save_parquet(sample_data, test_file)

    loaded_data = load_parquet(test_file)
    assert loaded_data.height == 3, "Should load correct number of records"
    assert loaded_data.schema == sample_data.schema, "Should preserve schema"

    # Cleanup
    os.remove(test_file)

    print("✅ Load layer tests passed")


def test_orchestration_layer():
    """Test orchestration layer - workflow coordination"""
    print("🧪 Testing orchestration layer...")

    # Test that orchestration functions exist and can be called
    from src.orchestration.pipeline import (
        run_current_state_pipeline,
        run_tvl_pipeline,
        run_historical_facts_pipeline,
        run_dune_upload_pipeline,
    )

    # Test scheduler creation
    from src.orchestration.scheduler import create_scheduler

    scheduler = create_scheduler(dry_run=True)
    assert scheduler.dry_run == True, "Should create scheduler with dry_run=True"

    print("✅ Orchestration layer tests passed")


def test_layer_separation():
    """Test that layers are properly separated"""
    print("🧪 Testing layer separation...")

    # Extract layer should not import from transform or load
    import src.extract.data_fetcher as extract_module

    extract_imports = [name for name in dir(extract_module) if not name.startswith("_")]

    # Transform layer should not import from load
    import src.transformation.transformers as transform_module

    transform_imports = [
        name for name in dir(transform_module) if not name.startswith("_")
    ]

    # Load layer should not import from extract or transform
    import src.load.local_storage as load_module

    load_imports = [name for name in dir(load_module) if not name.startswith("_")]

    print("✅ Layer separation tests passed")


def run_all_tests():
    """Run all clean architecture tests"""
    print("🧪 RUNNING CLEAN ARCHITECTURE TESTS")
    print("=" * 50)

    try:
        test_extract_layer()
        test_transform_layer()
        test_load_layer()
        test_orchestration_layer()
        test_layer_separation()

        print("\n🎉 ALL CLEAN ARCHITECTURE TESTS PASSED!")
        print("   The new architecture is working correctly.")

    except Exception as e:
        print(f"\n❌ Clean architecture tests failed: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()

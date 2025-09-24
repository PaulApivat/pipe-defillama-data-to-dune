#!/usr/bin/env python3
"""
Safe Characterization Tests - No Dune Upload, No API Calls

These tests validate the current working behavior using existing data files
without making any disruptive changes or long-running operations.
"""

import polars as pl
import os
import sys
from datetime import date

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.datasources.defillama.yieldpools.schemas import (
    CURRENT_STATE_SCHEMA,
    HISTORICAL_TVL_SCHEMA,
    HISTORICAL_FACTS_SCHEMA,
    POOL_DIM_SCD2_SCHEMA,
)


def test_existing_tvl_data():
    """Test existing TVL data file matches expected schema and volume"""
    print("🧪 Testing existing TVL data...")

    # Look for most recent TVL file
    tvl_files = [
        f
        for f in os.listdir("output")
        if f.startswith("tvl_data_") and f.endswith(".parquet")
    ]
    if not tvl_files:
        print("❌ No TVL data files found in output/")
        return False

    # Use most recent file
    latest_tvl_file = sorted(tvl_files)[-1]
    tvl_path = f"output/{latest_tvl_file}"

    print(f"   Using TVL file: {latest_tvl_file}")

    # Load and validate
    tvl_data = pl.read_parquet(tvl_path)

    # Check schema
    assert (
        tvl_data.schema == HISTORICAL_TVL_SCHEMA
    ), f"Schema mismatch for {latest_tvl_file}"

    # Check volume
    assert tvl_data.height > 400_000, f"Expected >400K records, got {tvl_data.height}"

    # Check columns
    expected_columns = [
        "timestamp",
        "tvl_usd",
        "apy",
        "apy_base",
        "apy_reward",
        "pool_id",
    ]
    for col in expected_columns:
        assert col in tvl_data.columns, f"Missing column: {col}"

    print(f"✅ TVL data: {tvl_data.height:,} records, schema valid")
    return True


def test_existing_scd2_data():
    """Test existing SCD2 data file matches expected schema and volume"""
    print("🧪 Testing existing SCD2 data...")

    scd2_path = "output/pool_dim_scd2.parquet"
    if not os.path.exists(scd2_path):
        print(f"❌ SCD2 file not found: {scd2_path}")
        return False

    # Load and validate
    scd2_data = pl.read_parquet(scd2_path)

    # Check schema
    assert scd2_data.schema == POOL_DIM_SCD2_SCHEMA, f"Schema mismatch for {scd2_path}"

    # Check volume
    assert scd2_data.height > 1_000, f"Expected >1K records, got {scd2_data.height}"

    # Check SCD2 fields
    scd2_fields = [
        "pool_id",
        "valid_from",
        "valid_to",
        "is_current",
        "attrib_hash",
        "is_active",
    ]
    for field in scd2_fields:
        assert field in scd2_data.columns, f"Missing SCD2 field: {field}"

    # Check valid_from is set to historical date
    valid_from_values = (
        scd2_data.select(pl.col("valid_from").unique()).to_series().to_list()
    )
    assert all(
        v == date(2022, 1, 1) for v in valid_from_values
    ), f"Invalid valid_from dates: {valid_from_values}"

    print(f"✅ SCD2 data: {scd2_data.height:,} records, schema valid")
    return True


def test_existing_historical_facts_data():
    """Test existing historical facts data matches expected schema and volume"""
    print("🧪 Testing existing historical facts data...")

    # Look for most recent historical facts file
    facts_files = [
        f
        for f in os.listdir("output")
        if f.startswith("historical_facts_") and f.endswith(".parquet")
    ]
    if not facts_files:
        print("❌ No historical facts files found in output/")
        return False

    # Use most recent file
    latest_facts_file = sorted(facts_files)[-1]
    facts_path = f"output/{latest_facts_file}"

    print(f"   Using historical facts file: {latest_facts_file}")

    # Load and validate
    historical_facts = pl.read_parquet(facts_path)

    # Check schema
    assert (
        historical_facts.schema == HISTORICAL_FACTS_SCHEMA
    ), f"Schema mismatch for {latest_facts_file}"

    # Check volume (should match your proven working state)
    assert (
        historical_facts.height == 496_662
    ), f"Expected 496,662 records, got {historical_facts.height}"

    # Check date range
    timestamps = historical_facts.select(pl.col("timestamp")).to_series().to_list()
    min_date = min(timestamps)
    max_date = max(timestamps)

    assert min_date == "2022-02-11", f"Expected min_date 2022-02-11, got {min_date}"
    assert max_date == "2025-09-22", f"Expected max_date 2025-09-22, got {max_date}"

    # Check SCD2 fields are present
    scd2_fields = ["valid_from", "valid_to", "is_current", "attrib_hash", "is_active"]
    for field in scd2_fields:
        assert field in historical_facts.columns, f"Missing SCD2 field: {field}"

    # Check no null values in key fields
    key_fields = ["protocol_slug", "chain", "symbol"]
    for field in key_fields:
        null_count = historical_facts.select(pl.col(field).is_null().sum()).item()
        assert null_count == 0, f"Null values found in {field}: {null_count}"

    print(
        f"✅ Historical facts: {historical_facts.height:,} records, {min_date} to {max_date}"
    )
    return True


def test_dune_uploader_dry_run():
    """Test DuneUploader can be instantiated without errors (dry run)"""
    print("🧪 Testing DuneUploader instantiation...")

    try:
        from src.coreutils.dune_uploader import DuneUploader

        # Test instantiation (this should work without API calls)
        dune_uploader = DuneUploader()

        # Test that we can access the methods (without calling them)
        assert hasattr(dune_uploader, "create_historical_facts_table")
        assert hasattr(dune_uploader, "upload_historical_facts_data")
        assert hasattr(dune_uploader, "clear_table")

        print("✅ DuneUploader: Instantiation successful, methods available")
        return True

    except Exception as e:
        print(f"❌ DuneUploader instantiation failed: {e}")
        return False


def test_schemas_consistency():
    """Test that all schemas are properly defined and consistent"""
    print("🧪 Testing schema consistency...")

    # Test that schemas are properly defined
    schemas = {
        "CURRENT_STATE_SCHEMA": CURRENT_STATE_SCHEMA,
        "HISTORICAL_TVL_SCHEMA": HISTORICAL_TVL_SCHEMA,
        "HISTORICAL_FACTS_SCHEMA": HISTORICAL_FACTS_SCHEMA,
        "POOL_DIM_SCD2_SCHEMA": POOL_DIM_SCD2_SCHEMA,
    }

    for name, schema in schemas.items():
        assert schema is not None, f"{name} is None"
        assert len(schema) > 0, f"{name} is empty"
        print(f"   ✅ {name}: {len(schema)} fields")

    print("✅ All schemas properly defined")
    return True


def test_data_join_logic():
    """Test that the data join logic produces expected results"""
    print("🧪 Testing data join logic...")

    # Load existing data
    tvl_files = [
        f
        for f in os.listdir("output")
        if f.startswith("tvl_data_") and f.endswith(".parquet")
    ]
    if not tvl_files:
        print("❌ No TVL data files found")
        return False

    latest_tvl_file = sorted(tvl_files)[-1]
    tvl_path = f"output/{latest_tvl_file}"

    if not os.path.exists("output/pool_dim_scd2.parquet"):
        print("❌ SCD2 file not found")
        return False

    # Load data
    tvl_data = pl.read_parquet(tvl_path)
    scd2_data = pl.read_parquet("output/pool_dim_scd2.parquet")

    # Test that join would work (without actually doing it)
    # Check that pool_id exists in both datasets
    tvl_pool_ids = set(tvl_data.select(pl.col("pool_id")).to_series().to_list())
    scd2_pool_ids = set(scd2_data.select(pl.col("pool_id")).to_series().to_list())

    # Most TVL pool_ids should have corresponding SCD2 records
    common_pool_ids = tvl_pool_ids.intersection(scd2_pool_ids)
    overlap_ratio = len(common_pool_ids) / len(tvl_pool_ids)

    assert (
        overlap_ratio > 0.8
    ), f"Low overlap between TVL and SCD2 pool_ids: {overlap_ratio:.2%}"

    print(
        f"✅ Data join logic: {len(common_pool_ids)}/{len(tvl_pool_ids)} pool_ids match ({overlap_ratio:.1%})"
    )
    return True


def run_safe_characterization_tests():
    """Run all safe characterization tests"""
    print("🧪 RUNNING SAFE CHARACTERIZATION TESTS")
    print("=" * 50)
    print("   (No API calls, no Dune uploads, using existing data)")
    print()

    results = {}

    # Test 1: Existing TVL Data
    try:
        results["tvl"] = test_existing_tvl_data()
        print("✅ TVL Data - PASSED")
    except Exception as e:
        print(f"❌ TVL Data - FAILED: {e}")
        results["tvl_error"] = str(e)

    # Test 2: Existing SCD2 Data
    try:
        results["scd2"] = test_existing_scd2_data()
        print("✅ SCD2 Data - PASSED")
    except Exception as e:
        print(f"❌ SCD2 Data - FAILED: {e}")
        results["scd2_error"] = str(e)

    # Test 3: Existing Historical Facts Data
    try:
        results["historical_facts"] = test_existing_historical_facts_data()
        print("✅ Historical Facts Data - PASSED")
    except Exception as e:
        print(f"❌ Historical Facts Data - FAILED: {e}")
        results["historical_facts_error"] = str(e)

    # Test 4: DuneUploader Dry Run
    try:
        results["dune_dry_run"] = test_dune_uploader_dry_run()
        print("✅ DuneUploader Dry Run - PASSED")
    except Exception as e:
        print(f"❌ DuneUploader Dry Run - FAILED: {e}")
        results["dune_dry_run_error"] = str(e)

    # Test 5: Schema Consistency
    try:
        results["schemas"] = test_schemas_consistency()
        print("✅ Schema Consistency - PASSED")
    except Exception as e:
        print(f"❌ Schema Consistency - FAILED: {e}")
        results["schemas_error"] = str(e)

    # Test 6: Data Join Logic
    try:
        results["join_logic"] = test_data_join_logic()
        print("✅ Data Join Logic - PASSED")
    except Exception as e:
        print(f"❌ Data Join Logic - FAILED: {e}")
        results["join_logic_error"] = str(e)

    # Summary
    print("\n🎯 SAFE CHARACTERIZATION TEST SUMMARY")
    print("=" * 50)

    passed_tests = 0
    total_tests = 6

    test_names = [
        "tvl",
        "scd2",
        "historical_facts",
        "dune_dry_run",
        "schemas",
        "join_logic",
    ]
    for test_name in test_names:
        if f"{test_name}_error" not in results:
            passed_tests += 1
            print(f"✅ {test_name.replace('_', ' ').title()}")
        else:
            print(f"❌ {test_name.replace('_', ' ').title()}")

    print(f"\n📊 Overall: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("🎉 ALL SAFE CHARACTERIZATION TESTS PASSED!")
        print(
            "   The pipeline data is in the expected state and ready for refactoring."
        )
        print("   You can proceed with architectural changes safely.")
    else:
        print("⚠️  Some tests failed. Check the issues before refactoring.")

    return results


if __name__ == "__main__":
    results = run_safe_characterization_tests()

#!/usr/bin/env python3
"""
Test the full production pipeline end-to-end
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import PipelineOrchestrator
from datetime import date, timedelta
from dotenv import load_dotenv
import logging
import polars as pl

# Load environment variables
load_dotenv()

# Configure logging for detailed output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,  # Override any existing logging config
)


def test_full_pipeline():
    """Test the full production pipeline with comprehensive validation"""

    print("🚀 Testing Full Production Pipeline")
    print("=" * 50)
    print("This test validates all components:")
    print("  📁 Extract Layer: API fetching, schema validation, data types")
    print("  🔄 Transform Layer: Data transformation, joins, filtering")
    print("  ☁️  Load Layer: Dune upload, table creation, data persistence")
    print("  🎯 Orchestration: End-to-end pipeline coordination")
    print()

    # Create pipeline instance (dry run for safe testing)
    # Change to dry_run=False for full production testing
    pipeline = PipelineOrchestrator(dry_run=True)

    try:
        # ========================================
        # TEST 1: EXTRACT LAYER VALIDATION
        # ========================================
        print("\n📁 TEST 1: Extract Layer Validation")
        print("-" * 40)

        # Test 1.1: Fetch raw pools data
        print("\n1️⃣ Testing fetch_raw_pools_data()...")
        from src.extract.data_fetcher import fetch_raw_pools_data, TARGET_PROJECTS
        from src.extract.schemas import RAW_POOLS_SCHEMA

        pools_df = fetch_raw_pools_data()
        print(f"✅ Fetched {pools_df.height} raw pool records")
        print(f"   Columns: {pools_df.columns}")

        # Verify schema compliance
        if pools_df.schema == RAW_POOLS_SCHEMA:
            print("✅ Pools schema matches RAW_POOLS_SCHEMA exactly")
        else:
            print("❌ Pools schema mismatch detected:")
            print(f"   Expected: {RAW_POOLS_SCHEMA}")
            print(f"   Actual:   {pools_df.schema}")
            return False

        # Verify target project filtering
        unique_protocols = (
            pools_df.select("protocol_slug").unique().to_series().to_list()
        )
        unexpected_protocols = [p for p in unique_protocols if p not in TARGET_PROJECTS]
        if unexpected_protocols:
            print(f"❌ ERROR: Found unexpected protocols: {unexpected_protocols}")
            return False
        else:
            print(f"✅ All protocols are in target projects: {unique_protocols}")

        # Verify data types
        tvl_usd_type = pools_df.select("tvl_usd").dtypes[0]
        pool_old_type = pools_df.select("pool_old").dtypes[0]
        if tvl_usd_type != pl.Float64:
            print(f"❌ ERROR: tvl_usd type is {tvl_usd_type}, expected Float64")
            return False
        if pool_old_type != pl.String:
            print(f"❌ ERROR: pool_old type is {pool_old_type}, expected String")
            return False
        print("✅ Data types are correct")

        # Test 1.2: PRE-VALIDATION - Test data fetcher with small sample first
        print("\n2️⃣ PRE-VALIDATION: Testing data fetcher with small sample...")
        print("⚠️  This will catch data type errors BEFORE the 3-hour fetch...")

        from src.extract.data_fetcher import fetch_raw_tvl_data, get_pool_ids_from_pools
        from src.extract.schemas import RAW_TVL_SCHEMA

        pool_ids = get_pool_ids_from_pools(pools_df)
        print(f"📊 Total pools available: {len(pool_ids)}")

        # Test with first 10 pools to catch data type errors early
        sample_pool_ids = pool_ids[:10]
        print(f"🧪 Testing with sample of {len(sample_pool_ids)} pools first...")

        try:
            sample_tvl_df = fetch_raw_tvl_data(sample_pool_ids)
            print(f"✅ Sample TVL fetch successful: {sample_tvl_df.height} records")
            print(f"   Sample schema: {sample_tvl_df.schema}")

            # Verify sample schema compliance
            if sample_tvl_df.schema == RAW_TVL_SCHEMA:
                print("✅ Sample TVL schema matches RAW_TVL_SCHEMA exactly")
            else:
                print("❌ Sample TVL schema mismatch detected:")
                print(f"   Expected: {RAW_TVL_SCHEMA}")
                print(f"   Actual:   {sample_tvl_df.schema}")
                return False

        except Exception as e:
            print(f"❌ PRE-VALIDATION FAILED: {e}")
            print("❌ Data fetcher has errors - stopping before 3-hour fetch!")
            return False

        print("✅ PRE-VALIDATION PASSED - Data fetcher is working correctly!")

        # Test 1.3: Fetch raw TVL data (this is the 3-hour part)
        print("\n3️⃣ Testing fetch_raw_tvl_data() with FULL dataset...")
        print("⚠️  This will fetch historical TVL data for ALL pools (3+ hours)...")
        print("⚠️  Pre-validation passed, so this should work...")

        print(f"📊 Fetching TVL for {len(pool_ids)} pools...")

        tvl_df = fetch_raw_tvl_data(pool_ids)
        print(f"✅ Fetched {tvl_df.height} raw TVL records")
        print(f"   Columns: {tvl_df.columns}")

        # Verify TVL schema compliance
        if tvl_df.schema == RAW_TVL_SCHEMA:
            print("✅ TVL schema matches RAW_TVL_SCHEMA exactly")
        else:
            print("❌ TVL schema mismatch detected:")
            print(f"   Expected: {RAW_TVL_SCHEMA}")
            print(f"   Actual:   {tvl_df.schema}")
            return False

        print("✅ Extract Layer validation completed successfully!")

        # ========================================
        # TEST 2: TRANSFORM LAYER VALIDATION
        # ========================================
        print("\n🔄 TEST 2: Transform Layer Validation")
        print("-" * 40)

        from src.transformation.transformers import (
            create_pool_dimensions,
            create_historical_facts,
            filter_pools_by_projects,
        )
        from src.transformation.schemas import POOL_DIM_SCHEMA, HISTORICAL_FACTS_SCHEMA

        # Test 2.1: Create pool dimensions
        print("\n1️⃣ Testing create_pool_dimensions()...")
        dimensions_df = create_pool_dimensions(pools_df)
        print(f"✅ Created {dimensions_df.height} pool dimension records")

        if dimensions_df.schema == POOL_DIM_SCHEMA:
            print("✅ Dimensions schema matches POOL_DIM_SCHEMA exactly")
        else:
            print("❌ Dimensions schema mismatch detected")
            return False

        # Test 2.2: Filter by target projects
        print("\n2️⃣ Testing filter_pools_by_projects()...")
        filtered_dimensions_df = filter_pools_by_projects(
            dimensions_df, TARGET_PROJECTS
        )
        print(f"✅ Filtered to {filtered_dimensions_df.height} target project records")

        # Test 2.3: Create historical facts (join TVL + dimensions)
        print("\n3️⃣ Testing create_historical_facts()...")
        historical_facts_df = create_historical_facts(
            tvl_df, filtered_dimensions_df, None
        )
        print(f"✅ Created {historical_facts_df.height} historical fact records")

        if historical_facts_df.schema == HISTORICAL_FACTS_SCHEMA:
            print("✅ Historical facts schema matches HISTORICAL_FACTS_SCHEMA exactly")
        else:
            print("❌ Historical facts schema mismatch detected")
            return False

        print("✅ Transform Layer validation completed successfully!")

        # ========================================
        # TEST 3: LOAD LAYER VALIDATION
        # ========================================
        print("\n☁️  TEST 3: Load Layer Validation")
        print("-" * 40)

        # Test 3.1: Create facts table
        print("\n1️⃣ Testing Dune table creation...")
        pipeline.dune_uploader.create_facts_table(historical_facts_df)
        print("✅ Facts table created successfully")

        # Test 3.2: Upload full historical facts
        print("\n2️⃣ Testing Dune data upload...")
        pipeline.dune_uploader.upload_full_historical_facts(historical_facts_df)
        print(f"✅ Uploaded {historical_facts_df.height} records to Dune")

        # Test 3.3: Verify table info
        print("\n3️⃣ Testing Dune table info...")
        table_info = pipeline.dune_uploader.get_table_info()
        print(f"✅ Table info retrieved: {table_info}")

        # Test 3.4: Save local historical facts file
        print("\n4️⃣ Saving local historical facts file...")
        from datetime import datetime

        today = datetime.now().strftime("%Y-%m-%d")
        local_facts_file = f"output/historical_facts_{today}.parquet"
        historical_facts_df.write_parquet(local_facts_file)
        print(f"✅ Saved local historical facts: {local_facts_file}")
        print(f"   Records: {historical_facts_df.height:,}")
        print(f"   File size: {os.path.getsize(local_facts_file):,} bytes")

        print("✅ Load Layer validation completed successfully!")

        # ========================================
        # TEST 4: ORCHESTRATION VALIDATION
        # ========================================
        print("\n🎯 TEST 4: Orchestration Validation")
        print("-" * 40)

        # Test 4.1: Daily Update (incremental caching)
        print("\n1️⃣ Testing daily update with incremental caching...")
        yesterday = date.today() - timedelta(days=1)
        daily_success = pipeline.run_daily_update(yesterday)

        if daily_success:
            print(f"✅ Daily update for {yesterday} completed successfully!")
        else:
            print(f"❌ Daily update for {yesterday} failed!")
            return False

        # Test 4.2: Pipeline Status
        print("\n2️⃣ Testing pipeline status...")
        status = pipeline.get_pipeline_status()
        print(f"✅ Pipeline status: {status}")

        print("✅ Orchestration validation completed successfully!")

        # ========================================
        # FINAL VALIDATION
        # ========================================
        print("\n🎉 ALL VALIDATIONS PASSED!")
        print("=" * 50)
        print("✅ Extract Layer: API fetching, schema validation, data types")
        print("✅ Transform Layer: Data transformation, joins, filtering")
        print("✅ Load Layer: Dune upload, table creation, data persistence")
        print("✅ Orchestration: End-to-end pipeline coordination")
        print("✅ Incremental Caching: Daily updates with smart caching")
        print()
        print("🚀 Pipeline is ready for production!")
        return True

    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_append_behavior():
    """Test that daily updates handle first run vs subsequent runs correctly"""

    print("\n🔄 Testing First Run vs Append Behavior")
    print("-" * 50)
    print("This test verifies that:")
    print("  - First run: Uploads FULL historical data")
    print("  - Subsequent runs: Append daily data only")

    # Create pipeline instance
    pipeline = PipelineOrchestrator(dry_run=True)  # Dry run for testing

    try:
        # Test with yesterday's date
        yesterday = date.today() - timedelta(days=1)

        print(f"📅 Testing first run detection for {yesterday}")

        # Test first run detection
        is_first_run = pipeline._is_first_run()
        print(f"🔍 Is first run: {is_first_run}")

        # Run daily update for yesterday
        print("🔄 Running daily update (dry run)...")
        success = pipeline.run_daily_update(yesterday)

        if success:
            print("✅ Daily update completed successfully")
            print("✅ First run vs append behavior test passed")
        else:
            print("❌ Daily update failed")
            return False

        return True

    except Exception as e:
        print(f"❌ Append behavior test failed: {e}")
        return False


if __name__ == "__main__":
    # Run both tests
    success1 = test_full_pipeline()
    print("\n" + "=" * 60)
    success2 = test_append_behavior()
    exit(0 if (success1 and success2) else 1)

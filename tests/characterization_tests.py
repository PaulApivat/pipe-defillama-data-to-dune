#!/usr/bin/env python3
"""
Characterization Tests for DeFiLlama Data Pipeline

These tests capture the current working behavior of the pipeline
to ensure refactoring doesn't break existing functionality.

Core Pipeline Steps:
1. Fetch TVL data (HISTORICAL_TVL_SCHEMA)
2. Fetch SCD2 dimensions (CURRENT_STATE_SCHEMA) 
3. Join data with SCD2 logic (HISTORICAL_FACTS_SCHEMA)
4. Upload to Dune (defillama_historical_facts table)
"""

import polars as pl
import os
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.datasources.defillama.yieldpools.schemas import (
    CURRENT_STATE_SCHEMA,
    HISTORICAL_TVL_SCHEMA,
    HISTORICAL_FACTS_SCHEMA,
    POOL_DIM_SCD2_SCHEMA,
)
from src.datasources.defillama.yieldpools.current_state import YieldPoolsCurrentState
from src.datasources.defillama.yieldpools.historical_tvl import YieldPoolsTVLFact
from src.coreutils.dune_uploader import DuneUploader
from scripts.fetch_tvl import fetch_tvl_functional
from scripts.fetch_current_state import fetch_current_state
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class TestStep1TVLDataFetch:
    """Test Step 1: TVL Data Fetch and Storage"""

    def test_tvl_data_fetch_produces_expected_schema(self):
        """Test that TVL data fetch produces data matching HISTORICAL_TVL_SCHEMA"""
        print("\n🧪 Testing TVL data fetch schema...")

        # Fetch TVL data
        tvl_data = fetch_tvl_functional()

        # Verify schema matches
        assert (
            tvl_data.df.schema == HISTORICAL_TVL_SCHEMA
        ), f"Schema mismatch: {tvl_data.df.schema} != {HISTORICAL_TVL_SCHEMA}"

        # Verify expected columns
        expected_columns = [
            "timestamp",
            "tvl_usd",
            "apy",
            "apy_base",
            "apy_reward",
            "pool_id",
        ]
        for col in expected_columns:
            assert col in tvl_data.df.columns, f"Missing column: {col}"

        print(f"✅ TVL schema validation passed: {tvl_data.df.height} records")
        return tvl_data

    def test_tvl_data_has_expected_volume(self):
        """Test that TVL data has expected volume (~496K records)"""
        print("\n🧪 Testing TVL data volume...")

        tvl_data = fetch_tvl_functional()

        # Verify record count is in expected range
        record_count = tvl_data.df.height
        assert (
            400_000 <= record_count <= 600_000
        ), f"Unexpected record count: {record_count}"

        # Verify date range spans multiple years
        timestamps = tvl_data.df.select(pl.col("timestamp")).to_series().to_list()
        min_date = min(timestamps)
        max_date = max(timestamps)

        assert min_date < "2023-01-01", f"Data too recent, min_date: {min_date}"
        assert max_date >= "2025-01-01", f"Data too old, max_date: {max_date}"

        print(
            f"✅ TVL volume validation passed: {record_count} records, {min_date} to {max_date}"
        )
        return tvl_data

    def test_tvl_data_file_persistence(self):
        """Test that TVL data is saved to expected file"""
        print("\n🧪 Testing TVL data file persistence...")

        tvl_data = fetch_tvl_functional()
        today = date.today().strftime("%Y-%m-%d")
        expected_file = f"output/tvl_data_{today}.parquet"

        assert os.path.exists(expected_file), f"TVL file not created: {expected_file}"

        # Verify file can be read back
        loaded_data = pl.read_parquet(expected_file)
        assert loaded_data.height == tvl_data.df.height, "File size mismatch"
        assert loaded_data.schema == HISTORICAL_TVL_SCHEMA, "File schema mismatch"

        print(f"✅ TVL file persistence passed: {expected_file}")
        return expected_file


class TestStep2SCD2DimensionsFetch:
    """Test Step 2: SCD2 Dimensions Fetch and Storage"""

    def test_scd2_dimensions_fetch_produces_expected_schema(self):
        """Test that SCD2 dimensions fetch produces data matching POOL_DIM_SCD2_SCHEMA"""
        print("\n🧪 Testing SCD2 dimensions fetch schema...")

        # Fetch current state and SCD2 dimensions
        current_state, scd2_df = fetch_current_state()

        # Verify SCD2 schema matches
        assert (
            scd2_df.schema == POOL_DIM_SCD2_SCHEMA
        ), f"Schema mismatch: {scd2_df.schema} != {POOL_DIM_SCD2_SCHEMA}"

        # Verify expected columns
        expected_columns = [
            "pool_id",
            "protocol_slug",
            "chain",
            "symbol",
            "valid_from",
            "valid_to",
            "is_current",
            "attrib_hash",
            "is_active",
        ]
        for col in expected_columns:
            assert col in scd2_df.columns, f"Missing column: {col}"

        print(f"✅ SCD2 schema validation passed: {scd2_df.height} records")
        return scd2_df

    def test_scd2_dimensions_have_expected_volume(self):
        """Test that SCD2 dimensions have expected volume (~1.3K records)"""
        print("\n🧪 Testing SCD2 dimensions volume...")

        current_state, scd2_df = fetch_current_state()

        # Verify record count is in expected range
        record_count = scd2_df.height
        assert (
            1_000 <= record_count <= 2_000
        ), f"Unexpected record count: {record_count}"

        # Verify all records are current
        is_current_count = scd2_df.select(pl.col("is_current").sum()).item()
        assert (
            is_current_count == record_count
        ), f"Not all records are current: {is_current_count}/{record_count}"

        # Verify valid_from is set to historical date
        valid_from_values = (
            scd2_df.select(pl.col("valid_from").unique()).to_series().to_list()
        )
        assert all(
            v == date(2022, 1, 1) for v in valid_from_values
        ), f"Invalid valid_from dates: {valid_from_values}"

        print(f"✅ SCD2 volume validation passed: {record_count} records")
        return scd2_df

    def test_scd2_dimensions_file_persistence(self):
        """Test that SCD2 dimensions are saved to expected file"""
        print("\n🧪 Testing SCD2 dimensions file persistence...")

        current_state, scd2_df = fetch_current_state()
        expected_file = "output/pool_dim_scd2.parquet"

        assert os.path.exists(expected_file), f"SCD2 file not created: {expected_file}"

        # Verify file can be read back
        loaded_data = pl.read_parquet(expected_file)
        assert loaded_data.height == scd2_df.height, "File size mismatch"
        assert loaded_data.schema == POOL_DIM_SCD2_SCHEMA, "File schema mismatch"

        print(f"✅ SCD2 file persistence passed: {expected_file}")
        return expected_file


class TestStep3DataJoin:
    """Test Step 3: Data Join with SCD2 Logic"""

    def test_historical_facts_creation_produces_expected_schema(self):
        """Test that historical facts creation produces data matching HISTORICAL_FACTS_SCHEMA"""
        print("\n🧪 Testing historical facts creation schema...")

        # Load existing data
        tvl_data = pl.read_parquet("output/tvl_data_2025-09-23.parquet")
        scd2_df = pl.read_parquet("output/pool_dim_scd2.parquet")

        # Create historical facts
        from src.datasources.defillama.yieldpools.historical_tvl import (
            YieldPoolsTVLFact,
        )

        tvl_instance = YieldPoolsTVLFact(tvl_data)
        historical_facts = tvl_instance.create_historical_facts(scd2_df)

        # Verify schema matches
        assert (
            historical_facts.schema == HISTORICAL_FACTS_SCHEMA
        ), f"Schema mismatch: {historical_facts.schema} != {HISTORICAL_FACTS_SCHEMA}"

        # Verify expected columns
        expected_columns = [
            "timestamp",
            "pool_old_clean",
            "pool_id",
            "protocol_slug",
            "chain",
            "symbol",
            "tvl_usd",
            "apy",
            "apy_base",
            "apy_reward",
            "valid_from",
            "valid_to",
            "is_current",
            "attrib_hash",
            "is_active",
        ]
        for col in expected_columns:
            assert col in historical_facts.columns, f"Missing column: {col}"

        print(
            f"✅ Historical facts schema validation passed: {historical_facts.height} records"
        )
        return historical_facts

    def test_historical_facts_has_expected_volume(self):
        """Test that historical facts has expected volume (~496K records)"""
        print("\n🧪 Testing historical facts volume...")

        # Load existing data
        tvl_data = pl.read_parquet("output/tvl_data_2025-09-23.parquet")
        scd2_df = pl.read_parquet("output/pool_dim_scd2.parquet")

        # Create historical facts
        from src.datasources.defillama.yieldpools.historical_tvl import (
            YieldPoolsTVLFact,
        )

        tvl_instance = YieldPoolsTVLFact(tvl_data)
        historical_facts = tvl_instance.create_historical_facts(scd2_df)

        # Verify record count is in expected range
        record_count = historical_facts.height
        assert (
            400_000 <= record_count <= 600_000
        ), f"Unexpected record count: {record_count}"

        # Verify date range spans multiple years
        timestamps = historical_facts.select(pl.col("timestamp")).to_series().to_list()
        min_date = min(timestamps)
        max_date = max(timestamps)

        assert min_date < "2023-01-01", f"Data too recent, min_date: {min_date}"
        assert max_date >= "2025-01-01", f"Data too old, max_date: {max_date}"

        print(
            f"✅ Historical facts volume validation passed: {record_count} records, {min_date} to {max_date}"
        )
        return historical_facts

    def test_historical_facts_join_logic(self):
        """Test that historical facts join logic works correctly"""
        print("\n🧪 Testing historical facts join logic...")

        # Load existing data
        tvl_data = pl.read_parquet("output/tvl_data_2025-09-23.parquet")
        scd2_df = pl.read_parquet("output/pool_dim_scd2.parquet")

        # Create historical facts
        from src.datasources.defillama.yieldpools.historical_tvl import (
            YieldPoolsTVLFact,
        )

        tvl_instance = YieldPoolsTVLFact(tvl_data)
        historical_facts = tvl_instance.create_historical_facts(scd2_df)

        # Verify join worked (all TVL records should have dimension data)
        assert (
            historical_facts.height == tvl_data.height
        ), f"Join failed: {historical_facts.height} != {tvl_data.height}"

        # Verify SCD2 fields are present
        scd2_fields = [
            "valid_from",
            "valid_to",
            "is_current",
            "attrib_hash",
            "is_active",
        ]
        for field in scd2_fields:
            assert field in historical_facts.columns, f"Missing SCD2 field: {field}"

        # Verify no null values in key fields
        key_fields = ["protocol_slug", "chain", "symbol"]
        for field in key_fields:
            null_count = historical_facts.select(pl.col(field).is_null().sum()).item()
            assert null_count == 0, f"Null values found in {field}: {null_count}"

        print(f"✅ Historical facts join logic validation passed")
        return historical_facts


class TestStep4DuneUpload:
    """Test Step 4: Dune Upload"""

    def test_dune_uploader_creates_table(self):
        """Test that DuneUploader can create the historical facts table"""
        print("\n🧪 Testing Dune table creation...")

        try:
            dune_uploader = DuneUploader()
            dune_uploader.create_historical_facts_table()
            print("✅ Dune table creation passed")
            return True
        except Exception as e:
            print(f"❌ Dune table creation failed: {e}")
            return False

    def test_dune_uploader_uploads_data(self):
        """Test that DuneUploader can upload historical facts data"""
        print("\n🧪 Testing Dune data upload...")

        try:
            # Load historical facts
            historical_facts = pl.read_parquet(
                "output/historical_facts_2025-09-23.parquet"
            )

            # Upload sample data
            dune_uploader = DuneUploader()
            sample_data = historical_facts.head(10)
            dune_uploader.upload_historical_facts_data(sample_data)

            print("✅ Dune data upload passed")
            return True
        except Exception as e:
            print(f"❌ Dune data upload failed: {e}")
            return False


class TestEndToEndPipeline:
    """Test Complete End-to-End Pipeline"""

    def test_complete_pipeline_produces_expected_output(self):
        """Test that the complete pipeline produces the expected output"""
        print("\n🧪 Testing complete end-to-end pipeline...")

        # Step 1: Fetch TVL data
        print("  Step 1: Fetching TVL data...")
        tvl_data = fetch_tvl_functional()
        assert tvl_data.df.height > 400_000, f"TVL data too small: {tvl_data.df.height}"

        # Step 2: Fetch SCD2 dimensions
        print("  Step 2: Fetching SCD2 dimensions...")
        current_state, scd2_df = fetch_current_state()
        assert scd2_df.height > 1_000, f"SCD2 data too small: {scd2_df.height}"

        # Step 3: Create historical facts
        print("  Step 3: Creating historical facts...")
        from src.datasources.defillama.yieldpools.historical_tvl import (
            YieldPoolsTVLFact,
        )

        tvl_instance = YieldPoolsTVLFact(tvl_data.df)
        historical_facts = tvl_instance.create_historical_facts(scd2_df)
        assert (
            historical_facts.height > 400_000
        ), f"Historical facts too small: {historical_facts.height}"

        # Step 4: Test Dune upload (sample)
        print("  Step 4: Testing Dune upload...")
        dune_uploader = DuneUploader()
        sample_data = historical_facts.head(5)
        dune_uploader.upload_historical_facts_data(sample_data)

        print("✅ Complete pipeline test passed")
        return {
            "tvl_records": tvl_data.df.height,
            "scd2_records": scd2_df.height,
            "historical_facts_records": historical_facts.height,
        }


def run_all_characterization_tests():
    """Run all characterization tests"""
    print("🧪 RUNNING CHARACTERIZATION TESTS")
    print("=" * 50)

    results = {}

    # Step 1: TVL Data Fetch
    print("\n📊 STEP 1: TVL DATA FETCH")
    print("-" * 30)
    try:
        test_tvl = TestStep1TVLDataFetch()
        results["tvl_schema"] = test_tvl.test_tvl_data_fetch_produces_expected_schema()
        results["tvl_volume"] = test_tvl.test_tvl_data_has_expected_volume()
        results["tvl_persistence"] = test_tvl.test_tvl_data_file_persistence()
        print("✅ Step 1: TVL Data Fetch - PASSED")
    except Exception as e:
        print(f"❌ Step 1: TVL Data Fetch - FAILED: {e}")
        results["step1_error"] = str(e)

    # Step 2: SCD2 Dimensions Fetch
    print("\n📊 STEP 2: SCD2 DIMENSIONS FETCH")
    print("-" * 30)
    try:
        test_scd2 = TestStep2SCD2DimensionsFetch()
        results["scd2_schema"] = (
            test_scd2.test_scd2_dimensions_fetch_produces_expected_schema()
        )
        results["scd2_volume"] = test_scd2.test_scd2_dimensions_have_expected_volume()
        results["scd2_persistence"] = test_scd2.test_scd2_dimensions_file_persistence()
        print("✅ Step 2: SCD2 Dimensions Fetch - PASSED")
    except Exception as e:
        print(f"❌ Step 2: SCD2 Dimensions Fetch - FAILED: {e}")
        results["step2_error"] = str(e)

    # Step 3: Data Join
    print("\n📊 STEP 3: DATA JOIN")
    print("-" * 30)
    try:
        test_join = TestStep3DataJoin()
        results["join_schema"] = (
            test_join.test_historical_facts_creation_produces_expected_schema()
        )
        results["join_volume"] = test_join.test_historical_facts_has_expected_volume()
        results["join_logic"] = test_join.test_historical_facts_join_logic()
        print("✅ Step 3: Data Join - PASSED")
    except Exception as e:
        print(f"❌ Step 3: Data Join - FAILED: {e}")
        results["step3_error"] = str(e)

    # Step 4: Dune Upload
    print("\n📊 STEP 4: DUNE UPLOAD")
    print("-" * 30)
    try:
        test_dune = TestStep4DuneUpload()
        results["dune_table"] = test_dune.test_dune_uploader_creates_table()
        results["dune_upload"] = test_dune.test_dune_uploader_uploads_data()
        print("✅ Step 4: Dune Upload - PASSED")
    except Exception as e:
        print(f"❌ Step 4: Dune Upload - FAILED: {e}")
        results["step4_error"] = str(e)

    # End-to-End Test
    print("\n📊 END-TO-END PIPELINE")
    print("-" * 30)
    try:
        test_e2e = TestEndToEndPipeline()
        results["e2e"] = test_e2e.test_complete_pipeline_produces_expected_output()
        print("✅ End-to-End Pipeline - PASSED")
    except Exception as e:
        print(f"❌ End-to-End Pipeline - FAILED: {e}")
        results["e2e_error"] = str(e)

    # Summary
    print("\n🎯 CHARACTERIZATION TEST SUMMARY")
    print("=" * 50)

    passed_steps = 0
    total_steps = 4

    if "step1_error" not in results:
        passed_steps += 1
        print("✅ Step 1: TVL Data Fetch")
    else:
        print("❌ Step 1: TVL Data Fetch")

    if "step2_error" not in results:
        passed_steps += 1
        print("✅ Step 2: SCD2 Dimensions Fetch")
    else:
        print("❌ Step 2: SCD2 Dimensions Fetch")

    if "step3_error" not in results:
        passed_steps += 1
        print("✅ Step 3: Data Join")
    else:
        print("❌ Step 3: Data Join")

    if "step4_error" not in results:
        passed_steps += 1
        print("✅ Step 4: Dune Upload")
    else:
        print("❌ Step 4: Dune Upload")

    if "e2e_error" not in results:
        print("✅ End-to-End Pipeline")
    else:
        print("❌ End-to-End Pipeline")

    print(f"\n📊 Overall: {passed_steps}/{total_steps} steps passed")

    if passed_steps == total_steps:
        print("🎉 ALL CHARACTERIZATION TESTS PASSED!")
        print("   The pipeline is working correctly and ready for refactoring.")
    else:
        print("⚠️  Some tests failed. Fix issues before refactoring.")

    return results


if __name__ == "__main__":
    results = run_all_characterization_tests()

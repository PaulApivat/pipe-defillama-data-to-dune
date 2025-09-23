#!/usr/bin/env python3
"""
Comprehensive Pipeline Integration Tests

This test suite verifies each step of the data pipeline:
1. TVL data is fetched and stored in 'output/tvl_data_{today}.parquet'
2. SCD2 dimensions are fetched and stored in 'output/pool_dim_scd2.parquet'  
3. Data is joined between steps 1 and 2 to create 'output/historical_facts_{today}.parquet'
4. Data from step 3 is uploaded to Dune

Each test uses DuckDB in-memory to verify data integrity and joins.
"""

import sys
import os
import polars as pl
import duckdb
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.datasources.defillama.yieldpools.current_state import YieldPoolsCurrentState
from src.datasources.defillama.yieldpools.historical_tvl import YieldPoolsTVLFact
from src.datasources.defillama.yieldpools.scd2_manager import SCD2Manager
from src.coreutils.dune_uploader import DuneUploader
from src.coreutils.logging import setup_logging

logger = setup_logging()

# Test configuration
TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
}


class PipelineIntegrationTests:
    """Test suite for pipeline integration"""

    def __init__(self):
        self.today = date.today()
        self.tvl_file = f"output/tvl_data_{self.today}.parquet"
        self.scd2_file = "output/pool_dim_scd2.parquet"
        self.historical_facts_file = f"output/historical_facts_{self.today}.parquet"

    def test_step_1_tvl_data_fetch(self):
        """Test Step 1: TVL data is fetched and stored"""
        logger.info("🧪 Testing Step 1: TVL Data Fetch")

        # Fetch TVL data
        logger.info("Fetching TVL data...")
        metadata = YieldPoolsCurrentState.fetch().filter_by_projects(TARGET_PROJECTS)
        tvl_data = YieldPoolsTVLFact.fetch_tvl_for_metadata_pools(
            metadata_source=metadata
        )

        # Validate data
        assert tvl_data.df.height > 0, "TVL data should not be empty"
        assert "pool_id" in tvl_data.df.columns, "TVL data should have pool_id column"
        assert (
            "timestamp" in tvl_data.df.columns
        ), "TVL data should have timestamp column"
        assert "tvl_usd" in tvl_data.df.columns, "TVL data should have tvl_usd column"

        # Save to file
        tvl_data.to_parquet(self.tvl_file)
        logger.info(f"✅ Step 1 passed: TVL data saved to {self.tvl_file}")
        logger.info(f"   Records: {tvl_data.df.height}")
        logger.info(
            f"   Unique pools: {tvl_data.df.select(pl.col('pool_id').n_unique()).item()}"
        )

        return tvl_data

    def test_step_2_scd2_dimensions_fetch(self):
        """Test Step 2: SCD2 dimensions are fetched and stored"""
        logger.info("🧪 Testing Step 2: SCD2 Dimensions Fetch")

        # Fetch current state and create SCD2 dimensions
        logger.info("Fetching current state and creating SCD2 dimensions...")
        current_state = YieldPoolsCurrentState.fetch().filter_by_projects(
            TARGET_PROJECTS
        )

        # Create SCD2 dimensions
        with SCD2Manager() as scd2_manager:
            scd2_manager.register_dataframes(current_state.df, None)
            scd2_df = scd2_manager.update_scd2_dimension_sql(self.today)

        # Validate data
        assert scd2_df.height > 0, "SCD2 data should not be empty"
        assert "pool_id" in scd2_df.columns, "SCD2 data should have pool_id column"
        assert (
            "valid_from" in scd2_df.columns
        ), "SCD2 data should have valid_from column"
        assert "valid_to" in scd2_df.columns, "SCD2 data should have valid_to column"
        assert (
            "is_current" in scd2_df.columns
        ), "SCD2 data should have is_current column"

        # Save to file
        scd2_df.write_parquet(self.scd2_file)
        logger.info(f"✅ Step 2 passed: SCD2 dimensions saved to {self.scd2_file}")
        logger.info(f"   Records: {scd2_df.height}")
        logger.info(
            f"   Unique pools: {scd2_df.select(pl.col('pool_id').n_unique()).item()}"
        )

        return scd2_df

    def test_step_3_data_join(self):
        """Test Step 3: Data is joined between TVL and SCD2 dimensions"""
        logger.info("🧪 Testing Step 3: Data Join")

        # Load data from previous steps
        logger.info("Loading TVL and SCD2 data...")
        tvl_df = pl.read_parquet(self.tvl_file)
        scd2_df = pl.read_parquet(self.scd2_file)

        logger.info(f"TVL data: {tvl_df.height} records")
        logger.info(f"SCD2 data: {scd2_df.height} records")

        # Test join using DuckDB in-memory
        logger.info("Testing join using DuckDB...")
        conn = duckdb.connect(":memory:")
        conn.register("tvl_data", tvl_df)
        conn.register("scd2_data", scd2_df)

        # Test the join query
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

        result_df = conn.execute(join_query).pl()

        # Validate join results
        assert result_df.height > 0, "Join result should not be empty"
        assert (
            result_df.height == tvl_df.height
        ), f"Join should preserve all TVL records: {result_df.height} vs {tvl_df.height}"

        # Check that all TVL records have matching dimension records
        tvl_pool_ids = set(tvl_df.select("pool_id").to_series().to_list())
        result_pool_ids = set(result_df.select("pool_id").to_series().to_list())
        assert (
            tvl_pool_ids == result_pool_ids
        ), "All TVL pool_ids should have matching dimension records"

        # Save joined data
        result_df.write_parquet(self.historical_facts_file)
        logger.info(
            f"✅ Step 3 passed: Historical facts saved to {self.historical_facts_file}"
        )
        logger.info(f"   Records: {result_df.height}")
        logger.info(
            f"   Unique pools: {result_df.select(pl.col('pool_id').n_unique()).item()}"
        )

        conn.close()
        return result_df

    def test_step_4_dune_upload(self):
        """Test Step 4: Data is uploaded to Dune"""
        logger.info("🧪 Testing Step 4: Dune Upload")

        # Load historical facts data
        historical_facts_df = pl.read_parquet(self.historical_facts_file)
        logger.info(f"Loading historical facts: {historical_facts_df.height} records")

        # Test Dune uploader (dry run)
        logger.info("Testing Dune uploader...")
        dune_uploader = DuneUploader()

        # Test table creation
        logger.info("Testing table creation...")
        try:
            dune_uploader.create_historical_facts_table()
            logger.info("✅ Table creation successful")
        except Exception as e:
            logger.error(f"❌ Table creation failed: {e}")
            raise

        # Test data upload (this will actually upload to Dune)
        logger.info("Testing data upload...")
        try:
            dune_uploader.upload_historical_facts_data(historical_facts_df)
            logger.info("✅ Data upload successful")
        except Exception as e:
            logger.error(f"❌ Data upload failed: {e}")
            raise

        logger.info(
            f"✅ Step 4 passed: {historical_facts_df.height} records uploaded to Dune"
        )

    def test_full_pipeline(self):
        """Test the complete pipeline end-to-end"""
        logger.info("🚀 Testing Full Pipeline End-to-End")

        try:
            # Step 1: TVL data
            tvl_data = self.test_step_1_tvl_data_fetch()

            # Step 2: SCD2 dimensions
            scd2_data = self.test_step_2_scd2_dimensions_fetch()

            # Step 3: Data join
            historical_facts = self.test_step_3_data_join()

            # Step 4: Dune upload
            self.test_step_4_dune_upload()

            logger.info("🎉 Full pipeline test completed successfully!")
            logger.info(
                f"   Final result: {historical_facts.height} historical facts records"
            )

        except Exception as e:
            logger.error(f"❌ Pipeline test failed: {e}")
            raise

    def cleanup_test_files(self):
        """Clean up test files"""
        logger.info("🧹 Cleaning up test files...")

        files_to_clean = [self.tvl_file, self.scd2_file, self.historical_facts_file]

        for file_path in files_to_clean:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"   Removed: {file_path}")

        logger.info("✅ Cleanup completed")


def main():
    """Run the pipeline integration tests"""
    logger.info("🧪 Starting Pipeline Integration Tests")

    # Create test instance
    tests = PipelineIntegrationTests()

    try:
        # Run individual step tests
        logger.info("\n" + "=" * 60)
        logger.info("RUNNING INDIVIDUAL STEP TESTS")
        logger.info("=" * 60)

        tests.test_step_1_tvl_data_fetch()
        tests.test_step_2_scd2_dimensions_fetch()
        tests.test_step_3_data_join()
        tests.test_step_4_dune_upload()

        # Run full pipeline test
        logger.info("\n" + "=" * 60)
        logger.info("RUNNING FULL PIPELINE TEST")
        logger.info("=" * 60)

        tests.test_full_pipeline()

        logger.info("\n🎉 All tests passed successfully!")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
    finally:
        # Cleanup
        tests.cleanup_test_files()


if __name__ == "__main__":
    main()

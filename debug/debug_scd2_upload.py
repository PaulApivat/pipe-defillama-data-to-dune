#!/usr/bin/env python3
"""
Debug script to test SCD2 data upload specifically
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import os
from dotenv import load_dotenv
from src.coreutils.dune_uploader import DuneUploader
from src.coreutils.logging import setup_logging
from scripts.fetch_current_state import fetch_current_state

# Load environment variables
load_dotenv()
logger = setup_logging()


def debug_scd2_upload():
    """Debug SCD2 data upload step by step"""

    logger.info("🧪 Testing SCD2 data upload step by step...")

    try:
        # Fetch current state and create SCD2 data
        logger.info("🔄 Fetching current state data...")
        current_state, scd2_df = fetch_current_state()

        logger.info(f"📊 SCD2 data shape: {scd2_df.shape}")

        # Test with different batch sizes
        uploader = DuneUploader()

        # Create table
        logger.info("Creating SCD2 table...")
        uploader.create_scd2_dimension_table()

        # Test with 1 row
        logger.info("🔄 Testing upload with 1 row...")
        test_data_1 = scd2_df.head(1).to_dicts()
        result_1 = uploader.upload_data("pool_dim_scd2", test_data_1)
        logger.info("✅ 1-row upload test passed")

        # Test with 5 rows
        logger.info("🔄 Testing upload with 5 rows...")
        test_data_5 = scd2_df.head(5).to_dicts()
        result_5 = uploader.upload_data("pool_dim_scd2", test_data_5)
        logger.info("✅ 5-row upload test passed")

        # Test with 10 rows
        logger.info("🔄 Testing upload with 10 rows...")
        test_data_10 = scd2_df.head(10).to_dicts()
        result_10 = uploader.upload_data("pool_dim_scd2", test_data_10)
        logger.info("✅ 10-row upload test passed")

        # Test with 50 rows
        logger.info("🔄 Testing upload with 50 rows...")
        test_data_50 = scd2_df.head(50).to_dicts()
        result_50 = uploader.upload_data("pool_dim_scd2", test_data_50)
        logger.info("✅ 50-row upload test passed")

        # Test with 100 rows
        logger.info("🔄 Testing upload with 100 rows...")
        test_data_100 = scd2_df.head(100).to_dicts()
        result_100 = uploader.upload_data("pool_dim_scd2", test_data_100)
        logger.info("✅ 100-row upload test passed")

        # If all small batches work, try the full dataset
        logger.info("🔄 Testing upload with full dataset (1350 rows)...")
        full_data = scd2_df.to_dicts()
        result_full = uploader.upload_data("pool_dim_scd2", full_data)
        logger.info("✅ Full dataset upload test passed")
        logger.info(f"Upload result: {result_full}")

    except Exception as e:
        logger.error(f"❌ SCD2 upload test failed: {e}")

        # Try to get more details about the error
        if hasattr(e, "response"):
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response text: {e.response.text}")


if __name__ == "__main__":
    debug_scd2_upload()

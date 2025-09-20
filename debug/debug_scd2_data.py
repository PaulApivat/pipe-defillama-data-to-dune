#!/usr/bin/env python3
"""
Debug script to test SCD2 data format and upload
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


def debug_scd2_data():
    """Debug SCD2 data format and upload"""

    logger.info("🧪 Testing SCD2 data format and upload...")

    try:
        # Fetch current state and create SCD2 data
        logger.info("🔄 Fetching current state data...")
        current_state, scd2_df = fetch_current_state()

        logger.info(f"📊 SCD2 data shape: {scd2_df.shape}")
        logger.info(f"📊 SCD2 columns: {scd2_df.columns}")
        logger.info(f"📊 SCD2 schema: {scd2_df.schema}")

        # Show sample data
        logger.info("📋 Sample SCD2 data (first 2 rows):")
        sample_data = scd2_df.head(2)
        logger.info(sample_data.to_pandas().to_string())

        # Test data conversion
        logger.info("🔄 Testing data conversion...")
        data_dicts = scd2_df.to_dicts()
        logger.info(f"📊 Converted to {len(data_dicts)} dictionaries")

        # Show sample converted data
        if data_dicts:
            sample_dict = data_dicts[0]
            logger.info("📋 Sample converted data (first row):")
            for key, value in sample_dict.items():
                logger.info(f"  {key}: {type(value).__name__} = {value}")

        # Test with a small subset first
        logger.info("🔄 Testing upload with small subset...")
        uploader = DuneUploader()

        # Create table
        logger.info("Creating SCD2 test table...")
        uploader.create_table(
            table_name="test_scd2_debug",
            schema=uploader._polars_to_dune_schema(scd2_df.schema),
            description="Test SCD2 data upload",
        )

        # Test upload with just 1 row
        test_data = data_dicts[:1]  # Just first row
        logger.info(f"Testing upload with 1 row: {test_data[0]}")

        result = uploader.upload_data("test_scd2_debug", test_data)
        logger.info("✅ Small subset upload test passed")
        logger.info(f"Upload result: {result}")

        # If that works, try with 5 rows
        logger.info("🔄 Testing upload with 5 rows...")
        test_data_5 = data_dicts[:5]
        result_5 = uploader.upload_data("test_scd2_debug", test_data_5)
        logger.info("✅ 5-row upload test passed")
        logger.info(f"Upload result: {result_5}")

    except Exception as e:
        logger.error(f"❌ SCD2 data test failed: {e}")

        # Try to get more details about the error
        if hasattr(e, "response"):
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response text: {e.response.text}")


if __name__ == "__main__":
    debug_scd2_data()

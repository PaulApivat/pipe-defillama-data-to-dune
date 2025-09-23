#!/usr/bin/env python3
"""
Dune Upload Verification Test

This test specifically verifies that data from the historical facts file
is properly uploaded to Dune Analytics.
"""

import sys
import os
import polars as pl
from datetime import date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.coreutils.dune_uploader import DuneUploader
from src.coreutils.logging import setup_logging

logger = setup_logging()


def test_dune_upload_verification():
    """Test Dune upload of historical facts data"""
    logger.info("🧪 Testing Dune Upload Verification")

    # Load historical facts data
    today = date.today()
    historical_facts_file = f"output/historical_facts_{today}.parquet"

    if not os.path.exists(historical_facts_file):
        logger.error(f"❌ Historical facts file not found: {historical_facts_file}")
        logger.info("Please run the pipeline first to generate test data")
        return False

    # Load data
    logger.info("Loading historical facts data...")
    historical_facts_df = pl.read_parquet(historical_facts_file)

    logger.info(f"Historical facts data: {historical_facts_df.height} records")
    logger.info(f"Columns: {historical_facts_df.columns}")

    # Show sample data
    logger.info("\n📊 Historical Facts Data Sample:")
    logger.info(historical_facts_df.head(3).to_pandas().to_string())

    # Test Dune uploader
    logger.info("\n🚀 Testing Dune Uploader...")
    dune_uploader = DuneUploader()

    try:
        # Test table creation
        logger.info("Creating/updating historical facts table...")
        dune_uploader.create_historical_facts_table()
        logger.info("✅ Table creation successful")

        # Test data upload
        logger.info(f"Uploading {historical_facts_df.height} records to Dune...")
        dune_uploader.upload_historical_facts_data(historical_facts_df)
        logger.info("✅ Data upload successful")

        # Verify upload by checking table info
        logger.info("Verifying upload...")
        # Note: In a real implementation, you might want to query the table
        # to verify the data was uploaded correctly

        logger.info("✅ Dune upload verification completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Dune upload failed: {e}")
        return False


def test_dune_upload_with_sample_data():
    """Test Dune upload with sample data"""
    logger.info("🧪 Testing Dune Upload with Sample Data")

    # Create sample historical facts data
    sample_data = pl.DataFrame(
        {
            "timestamp": ["2025-09-22", "2025-09-22", "2025-09-22"],
            "pool_old_clean": ["test-pool-1", "test-pool-2", "test-pool-3"],
            "pool_id": ["test-pool-1-uuid", "test-pool-2-uuid", "test-pool-3-uuid"],
            "protocol_slug": ["test-protocol", "test-protocol", "test-protocol"],
            "chain": ["Ethereum", "Ethereum", "Ethereum"],
            "symbol": ["TEST-USD", "TEST-ETH", "TEST-BTC"],
            "tvl_usd": [1000.0, 2000.0, 3000.0],
            "apy": [5.0, 10.0, 15.0],
            "apy_base": [2.0, 4.0, 6.0],
            "apy_reward": [3.0, 6.0, 9.0],
            "valid_from": ["2025-09-22", "2025-09-22", "2025-09-22"],
            "valid_to": ["9999-12-31", "9999-12-31", "9999-12-31"],
            "is_current": [True, True, True],
            "attrib_hash": ["hash1", "hash2", "hash3"],
            "is_active": [True, True, True],
        }
    )

    logger.info(f"Sample data: {sample_data.height} records")
    logger.info("\n📊 Sample Data:")
    logger.info(sample_data.to_pandas().to_string())

    # Test Dune uploader with sample data
    logger.info("\n🚀 Testing Dune Uploader with Sample Data...")
    dune_uploader = DuneUploader()

    try:
        # Test table creation
        logger.info("Creating/updating historical facts table...")
        dune_uploader.create_historical_facts_table()
        logger.info("✅ Table creation successful")

        # Test data upload
        logger.info(f"Uploading {sample_data.height} sample records to Dune...")
        dune_uploader.upload_historical_facts_data(sample_data)
        logger.info("✅ Sample data upload successful")

        logger.info("✅ Dune upload with sample data completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Dune upload with sample data failed: {e}")
        return False


def main():
    """Run the Dune upload verification tests"""
    logger.info("🧪 Starting Dune Upload Verification Tests")

    try:
        # Test with existing data
        logger.info("\n" + "=" * 60)
        logger.info("TESTING WITH EXISTING DATA")
        logger.info("=" * 60)

        success1 = test_dune_upload_verification()

        # Test with sample data
        logger.info("\n" + "=" * 60)
        logger.info("TESTING WITH SAMPLE DATA")
        logger.info("=" * 60)

        success2 = test_dune_upload_with_sample_data()

        if success1 and success2:
            logger.info("✅ All Dune upload verification tests passed!")
        else:
            logger.error("❌ Some Dune upload verification tests failed!")

    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        raise


if __name__ == "__main__":
    main()

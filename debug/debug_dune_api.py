#!/usr/bin/env python3
"""
Debug script to test Dune API connectivity and authentication
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import os
from dotenv import load_dotenv
from src.coreutils.dune_uploader import DuneUploader
from src.coreutils.logging import setup_logging

# Load environment variables
load_dotenv()
logger = setup_logging()


def debug_dune_api():
    """Debug Dune API connection and authentication"""

    # Check if API key is loaded
    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        logger.error("❌ DUNE_API_KEY not found in environment variables")
        return False

    logger.info(f"✅ DUNE_API_KEY found: {api_key[:10]}...{api_key[-4:]}")

    # Test basic API connectivity
    try:
        uploader = DuneUploader()
        logger.info("✅ DuneUploader initialized successfully")

        # Test a simple API call (list tables)
        logger.info("🔄 Testing API connectivity...")

        # Try to get table info first (this might be a read-only operation)
        import requests

        headers = {"X-DUNE-API-KEY": api_key}

        # Test with a simple endpoint
        test_url = "https://api.dune.com/api/v1/query/1"  # This is a public query
        response = requests.get(test_url, headers=headers)

        if response.status_code == 200:
            logger.info("✅ API connectivity test passed")
        else:
            logger.warning(
                f"⚠️  API test returned status {response.status_code}: {response.text}"
            )

        # Test table creation with minimal data
        logger.info("🔄 Testing table creation...")
        try:
            result = uploader.create_table(
                table_name="test_table_debug",
                schema=[{"name": "test_col", "type": "varchar", "nullable": True}],
                description="Test table for debugging",
            )
            logger.info("✅ Table creation test passed")
            logger.info(f"Result: {result}")

            # Test data upload
            logger.info("🔄 Testing data upload...")
            test_data = [{"test_col": "hello world"}]
            upload_result = uploader.upload_data("test_table_debug", test_data)
            logger.info("✅ Data upload test passed")
            logger.info(f"Upload result: {upload_result}")

        except Exception as e:
            logger.error(f"❌ Test failed: {e}")

            # Try to get more details about the error
            if hasattr(e, "response"):
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text}")

    except Exception as e:
        logger.error(f"❌ DuneUploader initialization failed: {e}")
        return False

    return True


if __name__ == "__main__":
    debug_dune_api()

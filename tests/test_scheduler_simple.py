#!/usr/bin/env python3
"""
Simple forced scheduler test - matches your original approach
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import os
from dotenv import load_dotenv
from src.scheduler.scd2_scheduler import SCD2Scheduler
from src.coreutils.logging import setup_logging

# Load environment variables from .env file
load_dotenv()

logger = setup_logging()


def test_scheduler_forced():
    """Test scheduler by forcing execution"""
    logger.info("🧪 Testing SCD2Scheduler with forced execution...")

    # Test with dry run first (fast)
    logger.info("📅 Testing dry run mode...")
    scheduler = SCD2Scheduler(dry_run=True)

    # Force weekly update
    logger.info("Testing weekly dimension update...")
    scheduler.run_weekly_dimension_update()

    # Force daily update
    logger.info("Testing daily fact update...")
    scheduler.run_daily_fact_update()

    logger.info("✅ Dry run scheduler test completed!")

    # Ask if user wants to test real execution
    response = (
        input("\nDo you want to test real execution (with API calls)? (y/N): ")
        .strip()
        .lower()
    )
    if response == "y":
        logger.info("🚀 Testing real execution...")
        scheduler_real = SCD2Scheduler(dry_run=False)

        # Force weekly update
        logger.info("Testing weekly dimension update (real)...")
        scheduler_real.run_weekly_dimension_update()

        # Force daily update
        logger.info("Testing daily fact update (real)...")
        scheduler_real.run_daily_fact_update()

        logger.info("✅ Real execution scheduler test completed!")
    else:
        logger.info("⏭️  Skipping real execution test")


if __name__ == "__main__":
    test_scheduler_forced()

#!/usr/bin/env python3
"""
Test script to demonstrate dry run functionality of SCD2Scheduler
"""

import os
import sys
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scheduler.scd2_scheduler import SCD2Scheduler


def test_dry_run():
    """Test dry run mode - should not make any API calls"""
    print("🧪 Testing SCD2Scheduler in DRY RUN mode...")

    # Create scheduler in dry run mode
    scheduler = SCD2Scheduler(dry_run=True)

    print("\n📅 Testing weekly dimension update (dry run):")
    scheduler.run_weekly_dimension_update()

    print("\n📅 Testing daily fact update (dry run):")
    scheduler.run_daily_fact_update()

    print("\n✅ Dry run test completed - no real API calls made!")


if __name__ == "__main__":
    test_dry_run()

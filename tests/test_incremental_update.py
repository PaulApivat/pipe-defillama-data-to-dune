#!/usr/bin/env python3
"""
Test incremental daily update functionality
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline_test import PipelineOrchestrator
from datetime import date, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def test_incremental_update():
    """Test incremental daily update with tomorrow's date"""

    print("🧪 Testing Incremental Daily Update")
    print("=" * 50)

    # Create pipeline instance
    pipeline = PipelineOrchestrator(dry_run=False)

    # Use tomorrow's date to simulate a daily update
    tomorrow = date.today() + timedelta(days=1)
    print(f"📅 Simulating daily update for: {tomorrow}")

    try:
        # Run daily update (this should be incremental)
        print("\n🔄 Running daily update...")
        success = pipeline.run_daily_update(tomorrow)

        if success:
            print("✅ Daily update completed successfully!")

            # Check the results
            print(f"\n📊 Check your Dune table for data with date: {tomorrow}")
            print(
                "Query: SELECT pool_old_clean, COUNT(*) FROM dune.uniswap_fnd.test_run_defillama_historical_facts"
            )
            print(
                "       WHERE timestamp::DATE = '2025-09-27' GROUP BY 1 ORDER BY 2 DESC"
            )

        else:
            print("❌ Daily update failed!")

    except Exception as e:
        print(f"❌ Error during daily update: {e}")


if __name__ == "__main__":
    test_incremental_update()

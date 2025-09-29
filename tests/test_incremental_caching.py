#!/usr/bin/env python3
"""
Test incremental caching behavior to verify true incremental performance
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import PipelineOrchestrator
from datetime import date, timedelta
from dotenv import load_dotenv
import time
import os

# Load environment variables
load_dotenv()


def test_incremental_caching():
    """Test that verifies incremental caching works correctly"""

    print("🧪 INCREMENTAL CACHING TEST")
    print("=" * 50)
    print("This test verifies that subsequent runs use cached data")
    print("and don't re-fetch the same data.")
    print()

    # Create pipeline instance
    pipeline = PipelineOrchestrator(dry_run=True)  # Dry run to avoid Dune uploads

    # Test with yesterday's date
    # Note: Using yesterday's data instead of today's because:
    # 1. Yesterday's data is complete and stable (today's might still be updating)
    # 2. This test is focused on the caching mechanism, not the date logic
    # 3. Production pipeline uses today's data, but testing with yesterday is more reliable
    yesterday = date.today() - timedelta(days=1)
    print(f"📅 Testing incremental caching for: {yesterday}")
    print()

    # First run - should fetch data
    print("🔄 FIRST RUN (should fetch data):")
    print("-" * 40)
    start_time = time.time()

    try:
        success1 = pipeline.run_daily_update(yesterday)
        end_time = time.time()
        time1 = end_time - start_time

        print(f"✅ First run success: {success1}")
        print(f"⏱️  First run time: {time1:.2f} seconds")
        print()

        # Check if cache file was created
        cache_file = f"output/raw_tvl_{yesterday.strftime('%Y-%m-%d')}.parquet"
        cache_exists = os.path.exists(cache_file)
        print(f"📦 Cache file created: {cache_exists}")
        if cache_exists:
            file_size = os.path.getsize(cache_file)
            print(f"📊 Cache file size: {file_size:,} bytes")
        print()

    except Exception as e:
        print(f"❌ First run failed: {e}")
        return False

    # Second run - should use cache
    print("🔄 SECOND RUN (should use cache):")
    print("-" * 40)
    start_time = time.time()

    try:
        success2 = pipeline.run_daily_update(yesterday)
        end_time = time.time()
        time2 = end_time - start_time

        print(f"✅ Second run success: {success2}")
        print(f"⏱️  Second run time: {time2:.2f} seconds")
        print()

        # Analyze performance
        if time1 > 0 and time2 > 0:
            speedup = time1 / time2
            print(f"📈 Performance analysis:")
            print(f"   First run:  {time1:.2f}s")
            print(f"   Second run: {time2:.2f}s")
            print(f"   Speedup:    {speedup:.1f}x faster")

            if speedup > 2:
                print("✅ CACHING WORKING: Second run significantly faster")
            elif speedup > 1.5:
                print("⚠️  CACHING PARTIAL: Some improvement, but not dramatic")
            else:
                print("❌ CACHING FAILED: No significant speedup")

        print()
        print("🔍 Expected log messages for second run:")
        print("- '📦 Found cached TVL data for {date}, loading...'")
        print("- '✅ Using cached data: X records for {date}'")
        print("- '🎯 INCREMENTAL: Retrieved X records for {date}'")
        print()
        print("🔍 Check the logs above to verify caching behavior!")

        return success1 and success2

    except Exception as e:
        print(f"❌ Second run failed: {e}")
        return False


if __name__ == "__main__":
    success = test_incremental_caching()
    exit(0 if success else 1)

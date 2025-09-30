#!/usr/bin/env python3
"""
Test the first run detection logic with detailed logging
"""

import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import PipelineOrchestrator

# Load environment variables
load_dotenv()


def test_first_run_detection_with_logs():
    """Test the first run detection logic with detailed logging"""

    print("🔍 Testing First Run Detection Logic with Detailed Logs")
    print("=" * 60)

    # Create pipeline instance
    pipeline = PipelineOrchestrator(dry_run=True)

    try:
        # Test the first run detection
        print("🔍 Checking if this is first run...")
        is_first_run = pipeline._is_first_run()

        print(f"🔍 Is first run: {is_first_run}")

        if is_first_run:
            print("✅ Will upload FULL historical data")
            print("   - This should create 2.6M+ rows in Dune")
        else:
            print("❌ Will append daily data only")
            print("   - This will only add ~5,590 rows to Dune")
            print("   - This is WRONG for the first run!")

        return is_first_run

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    is_first_run = test_first_run_detection_with_logs()
    print(
        f"\n{'✅ Correctly detected as first run' if is_first_run else '❌ INCORRECTLY detected as subsequent run'}"
    )

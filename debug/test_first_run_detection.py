#!/usr/bin/env python3
"""
Test the first run detection logic
"""

import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import PipelineOrchestrator

# Load environment variables
load_dotenv()


def test_first_run_detection():
    """Test the first run detection logic"""

    print("🔍 Testing First Run Detection Logic")
    print("=" * 50)

    # Create pipeline instance
    pipeline = PipelineOrchestrator(dry_run=True)

    try:
        # Test the first run detection
        is_first_run = pipeline._is_first_run()

        print(f"🔍 Is first run: {is_first_run}")

        if is_first_run:
            print("✅ Will upload FULL historical data")
        else:
            print("✅ Will append daily data only")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


if __name__ == "__main__":
    success = test_first_run_detection()
    print(f"\n{'✅ Test passed' if success else '❌ Test failed'}")

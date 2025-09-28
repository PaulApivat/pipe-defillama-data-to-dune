#!/usr/bin/env python3
"""
Test Pipeline Script

Quick script to test the pipeline with 10 pools and test table.
"""

import sys
import os
from datetime import date

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from src.orchestration.pipeline_test import PipelineOrchestrator
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

from dotenv import load_dotenv

load_dotenv()


def test_initial_load():
    """Test initial load with 10 pools"""
    print("🧪 Testing Initial Load with 10 pools...")

    orchestrator = PipelineOrchestrator(dry_run=False)  # Set to True for dry run

    try:
        success = orchestrator.run_initial_load()
        if success:
            print("✅ Initial load test completed successfully!")
        else:
            print("❌ Initial load test failed!")
        return success
    except Exception as e:
        print(f"❌ Initial load test error: {e}")
        return False


def test_daily_update():
    """Test daily update with 10 pools"""
    print("🧪 Testing Daily Update with 10 pools...")

    orchestrator = PipelineOrchestrator(dry_run=False)  # Set to True for dry run

    try:
        success = orchestrator.run_daily_update()
        if success:
            print("✅ Daily update test completed successfully!")
        else:
            print("❌ Daily update test failed!")
        return success
    except Exception as e:
        print(f"❌ Daily update test error: {e}")
        return False


def main():
    """Run both tests"""
    print("🚀 Starting Pipeline Tests...")
    print("📊 Test Table: dune.uniswap_fnd.test_run_defillama_historical_facts")
    print("🔢 Test Data: 10 pools only")
    print()

    # Test 1: Initial Load
    print("=" * 50)
    print("TEST 1: INITIAL LOAD")
    print("=" * 50)
    initial_success = test_initial_load()

    print()

    # Test 2: Daily Update
    print("=" * 50)
    print("TEST 2: DAILY UPDATE")
    print("=" * 50)
    daily_success = test_daily_update()

    print()
    print("=" * 50)
    print("TEST RESULTS")
    print("=" * 50)
    print(f"Initial Load: {'✅ PASSED' if initial_success else '❌ FAILED'}")
    print(f"Daily Update: {'✅ PASSED' if daily_success else '❌ FAILED'}")

    if initial_success and daily_success:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    exit(main())

#!/usr/bin/env python3
"""
Pipeline Integration Test Runner

This script runs all the pipeline integration tests in sequence.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.coreutils.logging import setup_logging

logger = setup_logging()


def run_all_tests():
    """Run all pipeline integration tests"""
    logger.info("🚀 Running All Pipeline Integration Tests")

    try:
        # Test 1: DuckDB Join Verification
        logger.info("\n" + "=" * 80)
        logger.info("TEST 1: DUCKDB JOIN VERIFICATION")
        logger.info("=" * 80)

        from test_duckdb_join import main as test_duckdb_join

        test_duckdb_join()

        # Test 2: Dune Upload Verification
        logger.info("\n" + "=" * 80)
        logger.info("TEST 2: DUNE UPLOAD VERIFICATION")
        logger.info("=" * 80)

        from test_dune_upload import main as test_dune_upload

        test_dune_upload()

        # Test 3: Full Pipeline Steps
        logger.info("\n" + "=" * 80)
        logger.info("TEST 3: FULL PIPELINE STEPS")
        logger.info("=" * 80)

        from test_pipeline_steps import main as test_pipeline_steps

        test_pipeline_steps()

        logger.info("\n🎉 All pipeline integration tests completed successfully!")

    except Exception as e:
        logger.error(f"❌ Test suite failed: {e}")
        raise


def run_individual_test(test_name):
    """Run an individual test"""
    logger.info(f"🧪 Running individual test: {test_name}")

    try:
        if test_name == "duckdb_join":
            from test_duckdb_join import main as test_duckdb_join

            test_duckdb_join()
        elif test_name == "dune_upload":
            from test_dune_upload import main as test_dune_upload

            test_dune_upload()
        elif test_name == "pipeline_steps":
            from test_pipeline_steps import main as test_pipeline_steps

            test_pipeline_steps()
        else:
            logger.error(f"❌ Unknown test: {test_name}")
            logger.info("Available tests: duckdb_join, dune_upload, pipeline_steps")
            return False

        logger.info(f"✅ Test {test_name} completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Test {test_name} failed: {e}")
        return False


def main():
    """Main test runner"""
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Integration Test Runner")
    parser.add_argument(
        "--test", help="Run specific test (duckdb_join, dune_upload, pipeline_steps)"
    )
    parser.add_argument("--all", action="store_true", help="Run all tests")

    args = parser.parse_args()

    if args.test:
        run_individual_test(args.test)
    elif args.all:
        run_all_tests()
    else:
        logger.info("Please specify --test <test_name> or --all")
        logger.info("Available tests: duckdb_join, dune_upload, pipeline_steps")


if __name__ == "__main__":
    main()

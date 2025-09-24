"""
Main Entry Point - Clean Architecture Pipeline

This is the main entry point for the refactored pipeline.
Provides simple interfaces to run the complete pipeline or individual components.
"""

import sys
import os
from datetime import date
from typing import Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import run_full_pipeline
from src.orchestration.scheduler import create_scheduler
from src.coreutils.logging import setup_logging

logger = setup_logging()


def run_pipeline(component: str = "full", dry_run: bool = False) -> dict:
    """
    Run the pipeline or specific components

    Args:
        component: "full", "current_state", "tvl", "historical_facts", "dune_upload"
        dry_run: If True, don't actually run pipelines (for testing)

    Returns:
        dict: Results and statistics
    """
    logger.info(f"🚀 Running {component} pipeline (dry_run={dry_run})")

    if dry_run:
        logger.info("🔍 DRY RUN MODE - No actual operations will be performed")

    try:
        if component == "full":
            return run_full_pipeline()

        elif component == "current_state":
            from src.orchestration.pipeline import run_current_state_pipeline

            current_state, scd2_df = run_current_state_pipeline()
            return {
                "current_state": {"records": current_state.height},
                "scd2": {"records": scd2_df.height},
            }

        elif component == "tvl":
            from src.orchestration.pipeline import run_tvl_pipeline

            tvl_data = run_tvl_pipeline()
            return {"tvl": {"records": tvl_data.height}}

        elif component == "historical_facts":
            from src.orchestration.pipeline import run_historical_facts_pipeline

            historical_facts = run_historical_facts_pipeline()
            return {"historical_facts": {"records": historical_facts.height}}

        elif component == "dune_upload":
            from src.orchestration.pipeline import run_dune_upload_pipeline

            success = run_dune_upload_pipeline()
            return {"dune_upload": {"success": success}}

        else:
            raise ValueError(f"Unknown component: {component}")

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise


def run_scheduler(dry_run: bool = False):
    """
    Run the pipeline scheduler

    Args:
        dry_run: If True, don't actually run pipelines
    """
    logger.info(f"📅 Starting scheduler (dry_run={dry_run})")

    scheduler = create_scheduler(dry_run=dry_run)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped by user")
        scheduler.stop()


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="DeFiLlama Data Pipeline")
    parser.add_argument("command", choices=["run", "schedule"], help="Command to run")
    parser.add_argument(
        "--component",
        choices=["full", "current_state", "tvl", "historical_facts", "dune_upload"],
        default="full",
        help="Pipeline component to run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no actual operations)",
    )

    args = parser.parse_args()

    if args.command == "run":
        results = run_pipeline(args.component, args.dry_run)
        print(f"✅ Pipeline completed: {results}")

    elif args.command == "schedule":
        run_scheduler(args.dry_run)


if __name__ == "__main__":
    main()

"""
Scheduler - Orchestration Layer

Pure workflow coordination for scheduled pipeline execution.
Handles daily and weekly cadences.
"""

import schedule
import time
from datetime import date, datetime
from typing import Optional
from .pipeline import (
    run_current_state_pipeline,
    run_tvl_pipeline,
    run_historical_facts_pipeline,
    run_dune_upload_pipeline,
)
from ..load.local_storage import file_exists, get_latest_file
import logging

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Pure workflow coordination for scheduled pipeline execution"""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.running = False

    def run_weekly_dimension_update(self):
        """Weekly: Update SCD2 dimensions"""
        logger.info("🔄 Running weekly dimension update...")

        if self.dry_run:
            logger.info("🔍 DRY RUN: Would run current state pipeline")
            return

        try:
            # Run current state pipeline (includes SCD2 dimensions)
            current_state, scd2_df = run_current_state_pipeline()

            logger.info("✅ Weekly dimension update completed successfully")

        except Exception as e:
            logger.error(f"❌ Weekly dimension update failed: {e}")
            raise

    def run_daily_fact_update(self):
        """Daily: Update historical facts and upload to Dune"""
        logger.info("🔄 Running daily fact update...")

        if self.dry_run:
            logger.info("🔍 DRY RUN: Would run TVL and historical facts pipelines")
            return

        try:
            # Check if we need to run TVL pipeline
            today = date.today().strftime("%Y-%m-%d")
            tvl_file = f"output/tvl_data_{today}.parquet"

            if not file_exists(tvl_file):
                logger.info("🔄 TVL data file not found, running TVL pipeline...")
                run_tvl_pipeline()
            else:
                logger.info(f"📊 Using existing TVL data file: {tvl_file}")

            # Run historical facts pipeline
            logger.info("🔄 Running historical facts pipeline...")
            historical_facts = run_historical_facts_pipeline()

            # Upload to Dune
            logger.info("🔄 Uploading to Dune...")
            run_dune_upload_pipeline()

            logger.info("✅ Daily fact update completed successfully")

        except Exception as e:
            logger.error(f"❌ Daily fact update failed: {e}")
            raise

    def start(self):
        """Start the scheduler"""
        logger.info("🚀 Starting pipeline scheduler...")

        # Schedule weekly dimension update (Mondays at 2 AM)
        schedule.every().monday.at("02:00").do(self.run_weekly_dimension_update)

        # Schedule daily fact update (Daily at 6 AM)
        schedule.every().day.at("06:00").do(self.run_daily_fact_update)

        self.running = True
        logger.info("📅 Scheduler started - Weekly: Mon 2AM, Daily: 6AM")

        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            logger.info("🛑 Scheduler stopped by user")
            self.running = False

    def stop(self):
        """Stop the scheduler"""
        logger.info("🛑 Stopping scheduler...")
        self.running = False

    def run_now(self, pipeline_type: str = "daily"):
        """
        Run pipeline immediately (for testing)

        Args:
            pipeline_type: "daily" or "weekly"
        """
        logger.info(f"🔄 Running {pipeline_type} pipeline now...")

        if pipeline_type == "weekly":
            self.run_weekly_dimension_update()
        elif pipeline_type == "daily":
            self.run_daily_fact_update()
        else:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")

        logger.info(f"✅ {pipeline_type.title()} pipeline completed")


def create_scheduler(dry_run: bool = False) -> PipelineScheduler:
    """
    Create a new pipeline scheduler

    Args:
        dry_run: If True, don't actually run pipelines

    Returns:
        PipelineScheduler: New scheduler instance
    """
    return PipelineScheduler(dry_run=dry_run)

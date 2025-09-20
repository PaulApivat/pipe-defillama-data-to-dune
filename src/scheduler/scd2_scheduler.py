import schedule
import time
from datetime import date, timedelta
from src.coreutils.dune_uploader import DuneUploader
from scripts.fetch_current_state import fetch_current_state
from scripts.fetch_tvl import fetch_tvl_with_daily_metrics
from src.coreutils.logging import setup_logging

logger = setup_logging()


class SCD2Scheduler:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        if not self.dry_run:
            self.dune_uploader = DuneUploader()
        else:
            self.dune_uploader = None

    def run_weekly_dimension_update(self):
        """Weekly: Update SCD2 dimensions"""
        logger.info("🔄 Running weekly dimension update...")

        if self.dry_run:
            logger.info(
                "🔍 DRY RUN: Would fetch current state and update SCD2 dimensions"
            )
            return

        # update SCD2 dimensions
        current_state, scd2_df = fetch_current_state()

        # Upload to Dune
        if self.dune_uploader:
            # Create SCD2 table first (idempotent operation)
            logger.info("Creating/updating SCD2 dimension table...")
            self.dune_uploader.create_scd2_dimension_table()

            # Upload SCD2 dimension data
            logger.info("Uploading SCD2 dimension data...")
            self.dune_uploader.upload_scd2_dimension_data(scd2_df)

        logger.info("✅ Weekly dimension update completed successfully")

    def run_daily_fact_update(self):
        """Daily: Update facts and daily metrics"""
        logger.info("🔄 Running daily fact and metrics update...")

        if self.dry_run:
            logger.info("🔍 DRY RUN: Would fetch TVL data and create daily metrics")
            return

        # update facts and daily metrics
        tvl_data, daily_metrics = fetch_tvl_with_daily_metrics()

        # Upload daily metrics partition
        if self.dune_uploader:
            # Create table first (idempotent operation)
            logger.info("Creating/updating daily metrics table...")
            self.dune_uploader.create_daily_metrics_table()

            today = date.today()
            date_range = (today, today)

            # Delete old partition and Upload new one
            self.dune_uploader.delete_daily_metrics_partition(date_range)
            self.dune_uploader.upload_daily_metrics_partition(daily_metrics, date_range)

        logger.info("✅ Daily fact and metrics update completed successfully")

    def start(self):
        # Schedule weekly dimension update
        schedule.every().monday.at("02:00").do(self.run_weekly_dimension_update)

        # Schedule dialy fact update
        schedule.every().day.at("06:00").do(self.run_daily_fact_update)

        while True:
            schedule.run_pending()
            time.sleep(60)

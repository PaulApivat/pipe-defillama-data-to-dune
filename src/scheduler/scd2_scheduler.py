import schedule
import time
from datetime import date, timedelta
from src.coreutils.dune_uploader import DuneUploader
from scripts.fetch_current_state import fetch_current_state
from scripts.fetch_tvl import (
    fetch_tvl_with_historical_facts,
    fetch_tvl_with_incremental_historical_facts,
)
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
        """Daily: Update historical facts table"""
        logger.info("🔄 Running daily historical facts update...")

        if self.dry_run:
            logger.info("🔍 DRY RUN: Would fetch TVL data and create historical facts")
            return

        # Check if this is initial run or incremental
        today = date.today()

        # Simple logic: if historical facts file exists, use incremental; otherwise use full
        import os

        historical_facts_file = f"output/historical_facts_{today}.parquet"
        tvl_data_file = f"output/tvl_data_{today}.parquet"

        # Ensure TVL data exists first
        if not os.path.exists(tvl_data_file):
            logger.info("🔄 TVL data file not found, creating it first...")
            from scripts.fetch_tvl import fetch_tvl_functional

            tvl_data = fetch_tvl_functional()
            logger.info(f"📊 Created TVL data with {tvl_data.df.height} rows")
        else:
            logger.info(f"📊 Using existing TVL data file: {tvl_data_file}")
            import polars as pl

            existing_tvl = pl.read_parquet(tvl_data_file)
            logger.info(f"📊 Existing TVL data has {existing_tvl.height} rows")

        if os.path.exists(historical_facts_file):
            logger.info("📈 Using incremental historical facts update...")
            tvl_data, historical_facts = fetch_tvl_with_incremental_historical_facts()
        else:
            logger.info("🆕 Using full historical facts update (initial run)...")
            tvl_data, historical_facts = fetch_tvl_with_historical_facts()

        # Upload historical facts
        if self.dune_uploader:
            # Create table first (idempotent operation)
            logger.info("Creating/updating historical facts table...")
            self.dune_uploader.create_historical_facts_table()

            # Upload historical facts data
            self.dune_uploader.upload_historical_facts_data(historical_facts)

        logger.info("✅ Daily historical facts update completed successfully")

    def start(self):
        # Schedule weekly dimension update
        schedule.every().monday.at("02:00").do(self.run_weekly_dimension_update)

        # Schedule dialy fact update
        schedule.every().day.at("06:00").do(self.run_daily_fact_update)

        while True:
            schedule.run_pending()
            time.sleep(60)

"""
Pipeline Orchestrator - Two-Workflow Design

Two distinct workflows with separate GitHub Actions:
1. Initial Load Workflow (defillama_initial_load.yml):
   - Manual trigger only
   - Full historical data upload (2.6M+ rows)
   - Creates Dune table and uploads complete dataset

2. Daily Update Workflow (defillama_daily_pipeline.yml):
   - Scheduled daily at 6 AM UTC + manual trigger
   - Appends daily data only (~5K rows)
   - Enhanced error handling and duplicate detection

No runtime detection needed - mode determines behavior.
"""

import os
import sys
import json
from datetime import date, datetime, timedelta
from typing import Optional, List
import logging
import polars as pl

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Extract layer imports
from src.extract.data_fetcher import (
    fetch_raw_pools_data,
    fetch_raw_tvl_data,
)

# Transform layer imports
from src.transformation.transformers import (
    create_pool_dimensions,
    create_historical_facts,
    filter_pools_by_projects,
    save_transformed_data,
)

# Load layer imports
from src.load.dune_uploader import DuneUploader

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the complete data pipeline with simplified design"""

    def __init__(
        self,
        dune_api_key: Optional[str] = None,
        dry_run: bool = False,
        test_mode: bool = False,
    ):
        """
        Initialize the Pipeline orchestrator

        Args:
            dune_api_key: Dune API key (uses env var if not provided)
            dry_run: If true, skip actual Dune uploads
            test_mode: If true, use test table instead of production table
        """
        self.dry_run = dry_run
        self.test_mode = test_mode

        # Initialize Dune uploader only if not in dry run mode
        if not self.dry_run:
            self.dune_uploader = DuneUploader(api_key=dune_api_key, test_mode=test_mode)
            if test_mode:
                logger.info(
                    "🧪 TEST MODE: Using test table test_run_defillama_historical_facts"
                )
        else:
            self.dune_uploader = None
            logger.info("🔍 DRY RUN MODE: Dune uploader not initialized")

        # Target projects for filtering
        from src.extract.data_fetcher import TARGET_PROJECTS

        self.target_projects = TARGET_PROJECTS

    def run_initial_load(self) -> bool:
        """
        Run initial load - upload full historical dataset

        This is called by the initial load workflow (defillama_initial_load.yml).
        Always does a full historical upload (2.6M+ rows).
        Creates the Dune table and uploads the complete dataset.

        Returns:
            bool: True if successful
        """
        logger.info("🚀 Starting Initial Load Pipeline")
        logger.info("=" * 50)
        logger.info("This will upload the complete historical dataset")
        logger.info("Expected: 2.6M+ historical fact records")
        logger.info("")

        try:
            # Step 1: Extract raw data
            logger.info("🔄 Step 1: Extracting raw data...")
            raw_pools_df = fetch_raw_pools_data()
            raw_tvl_df = fetch_raw_tvl_data(raw_pools_df["pool"].to_list())

            logger.info(
                f"✅ Extracted {raw_pools_df.height} pools, {raw_tvl_df.height} TVL records"
            )

            # Step 2: Transform data
            logger.info("🔄 Step 2: Transforming data...")

            # Create pool dimensions
            dimensions_df = create_pool_dimensions(raw_pools_df)

            # Filter by target projects
            filtered_dimensions_df = filter_pools_by_projects(
                dimensions_df, self.target_projects
            )

            # Create historical facts (ALL historical data, no date filter)
            historical_facts_df = create_historical_facts(
                raw_tvl_df, filtered_dimensions_df, None
            )

            logger.info(
                f"✅ Transformed to {filtered_dimensions_df.height} dimensions, {historical_facts_df.height} facts"
            )

            # Step 3: Save transformed data locally
            logger.info("🔄 Step 3: Saving transformed data locally...")
            today = date.today().strftime("%Y-%m-%d")
            save_transformed_data(filtered_dimensions_df, historical_facts_df, today)

            # Step 4: Load to Dune (if not dry run)
            if not self.dry_run:
                logger.info("🔄 Step 4: Loading data to Dune...")
                self.dune_uploader.upload_full_historical_facts(historical_facts_df)
                logger.info("✅ Initial load to Dune completed successfully")
            else:
                logger.info("🔍 DRY RUN: Skipping Dune upload")

            return True

        except Exception as e:
            logger.error(f"❌ Initial load failed: {e}")
            raise

    def run_daily_update(self, target_date: date) -> bool:
        """
        Run daily update - append daily data only

        This is called by the daily pipeline workflow (defillama_daily_pipeline.yml).
        Always appends data (~5K rows), never does full refresh.
        Includes enhanced error handling and duplicate detection.

        Args:
            target_date: Date to process (required for clarity)

        Returns:
            bool: True if successful
        """

        logger.info(f"🔄 Starting Daily Update Pipeline for {target_date}")
        logger.info("=" * 50)
        logger.info("This will append daily data to existing historical dataset")
        logger.info("Expected: ~5K daily fact records")
        logger.info("")

        try:
            # Step 1: Extract raw data
            logger.info("🔄 Step 1: Extracting raw data...")
            raw_pools_df = fetch_raw_pools_data()
            raw_tvl_df = fetch_raw_tvl_data(raw_pools_df["pool"].to_list())

            logger.info(
                f"✅ Extracted {raw_pools_df.height} pools, {raw_tvl_df.height} TVL records"
            )

            # Step 2: Transform data
            logger.info("🔄 Step 2: Transforming data...")

            # Create pool dimensions
            dimensions_df = create_pool_dimensions(raw_pools_df)

            # Filter by target projects
            filtered_dimensions_df = filter_pools_by_projects(
                dimensions_df, self.target_projects
            )

            # Create historical facts (filtered for target date)
            historical_facts_df = create_historical_facts(
                raw_tvl_df, filtered_dimensions_df, target_date
            )

            logger.info(
                f"✅ Transformed to {filtered_dimensions_df.height} dimensions, {historical_facts_df.height} facts"
            )

            # Step 3: Save temporary daily data
            logger.info("💾 Step 3: Saving temporary daily data...")
            temp_file = f"output/cache/temp_daily_facts_{target_date.strftime('%Y-%m-%d')}.parquet"
            historical_facts_df.write_parquet(temp_file)
            logger.info(f"✅ Saved temporary data to {temp_file}")

            # Step 4: Upload facts to Dune (if not dry run)
            if not self.dry_run:
                logger.info("🔄 Step 4: Appending daily facts to Dune...")
                self.dune_uploader.append_daily_facts(historical_facts_df, target_date)
                logger.info("✅ Daily facts appended to Dune successfully")
            else:
                logger.info("🔍 DRY RUN: Skipping Dune upload")

            # Step 5: Cleanup temporary file after successful upload
            logger.info("🧹 Step 5: Cleaning up temporary files...")
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logger.info(f"✅ Cleaned up {temp_file}")

            logger.info(f"✅ Daily update completed successfully for {target_date}!")
            return True

        except Exception as e:
            logger.error(f"❌ Daily update failed: {e}")
            raise

    def get_pipeline_status(self) -> dict:
        """
        Get current pipeline status

        Returns:
            dict: Pipeline status information
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "dry_run": self.dry_run,
            "target_projects": list(self.target_projects),
            "dune_connected": self.dune_uploader is not None,
            "data_freshness": {
                "pools": True,  # Always fetch fresh pools data
                "tvl": False,  # TVL data is always fresh from API
            },
        }


def main():
    """Main entry point for command line usage"""
    import argparse

    parser = argparse.ArgumentParser(description="DeFiLlama Data Pipeline")
    parser.add_argument(
        "--mode",
        choices=["initial", "daily"],
        required=True,
        help="Pipeline mode: 'initial' for full load, 'daily' for daily update",
    )
    parser.add_argument("--date", help="Target date for daily mode (YYYY-MM-DD format)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no Dune uploads)",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in test mode (use test table instead of production)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Initialize orchestrator
    orchestrator = PipelineOrchestrator(dry_run=args.dry_run, test_mode=args.test_mode)

    try:
        if args.mode == "initial":
            logger.info("🚀 Running initial load...")
            success = orchestrator.run_initial_load()
        elif args.mode == "daily":
            if args.date:
                target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            else:
                target_date = date.today()
            logger.info(f"🔄 Running daily update for {target_date}...")
            success = orchestrator.run_daily_update(target_date)

        if success:
            logger.info("✅ Pipeline completed successfully")
            return 0
        else:
            logger.error("❌ Pipeline failed")
            return 1

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())

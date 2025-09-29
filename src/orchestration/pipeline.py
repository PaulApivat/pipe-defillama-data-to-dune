"""
Pipeline Orchestrator - Orchestration Layer

Coordinates the Extract → Transform → Load pipeline.
Handles both initial load and daily updates (check freshness first)
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
    """Orchestrates the complete data pipeline"""

    def __init__(self, dune_api_key: Optional[str] = None, dry_run: bool = False):
        """
        Initialize the Pipeline orchestrator

        Args:
            dune_api_key: use env var
            dry_run: If true, skip actual Dune uploads
        """
        self.dry_run = dry_run
        self.dune_uploader = None if dry_run else DuneUploader(api_key=dune_api_key)

        # Target projects for filtering
        self.target_projects = {
            "curve-dex",
            "pancakeswap-amm",
            "pancakeswap-amm-v3",
            "aerodrome-slipstream",
            "aerodrome-v1",
            "uniswap-v2",
            "uniswap-v3",
            "fluid-dex",
        }

        # Cache directory for metadata
        self.cache_dir = "output/cache"
        os.makedirs(self.cache_dir, exist_ok=True)

    def _is_data_fresh(self, data_type: str, max_age_hours: int = 24) -> bool:
        """
        Check if data is fresh enough to skip re-fetching

        Args:
            data_type: Type of data to check (pools or tvl)
            max_age_hours: Maximum age in hours before considering stale

        Returns:
            bool: True if data is fresh
        """
        cache_file = os.path.join(self.cache_dir, f"last_{data_type}_fetch.json")

        if not os.path.exists(cache_file):
            return False

        try:
            with open(cache_file, "r") as f:
                metadata = json.load(f)

            last_fetch = datetime.fromisoformat(metadata["timestamp"])
            age_hours = (datetime.now() - last_fetch).total_seconds() / 3600

            return age_hours < max_age_hours

        except (json.JSONDecodeError, KeyError, ValueError):
            return False

    def _save_fetch_metadata(self, data_type: str) -> None:
        """Save metadata about last fetch"""
        cache_file = os.path.join(self.cache_dir, f"last_{data_type}_fetch.json")

        metadata = {"timestamp": datetime.now().isoformat(), "data_type": data_type}

        with open(cache_file, "w") as f:
            json.dump(metadata, f, indent=2)

    def _load_cached_data(
        self, data_type: str, target_date: date
    ) -> Optional[pl.DataFrame]:
        """
        Load cache data if available

        Args:
            data_type: Type of data to load ('pools' or 'tvl')
            target_date: Target date for data

        Returns:
            pl.DataFrame or None if not available
        """
        if data_type == "pools":
            # Pools (dimension data) is current state, use most recent
            pattern = "output/raw_pools_*.parquet"
        else:  # tvl
            # TVL data is date-specific
            pattern = f"output/raw_tvl_{target_date.strftime('%Y-%m-%d')}.parquet"

        import glob

        files = glob.glob(pattern)

        if not files:
            return None

        # Get most recent file
        latest_file = max(files, key=os.path.getctime)

        try:
            return pl.read_parquet(latest_file)
        except Exception as e:
            logger.warning(f"Failed to load cached {data_type} data: {e}")
            return None

    def run_initial_load(self) -> bool:
        """
        Run initial load of full historical dataset

        Returns:
            bool: True if successful
        """
        logger.info("🚀 Starting initial load pipeline...")

        try:
            # step 1: Extract raw data
            logger.info("🔄 Step 1: Extracting raw data...")
            raw_pools_df = fetch_raw_pools_data()
            pool_ids = raw_pools_df.select("pool").to_series().to_list()
            raw_tvl_df = fetch_raw_tvl_data(pool_ids)

            # Save fetch metadata
            self._save_fetch_metadata("pools")
            self._save_fetch_metadata("tvl")

            logger.info(
                f"✅ Extracted {raw_pools_df.height} pools, {raw_tvl_df.height} TVL records"
            )

            # step 2: transform data
            logger.info("🔄 Step 2: Transforming data...")
            # create pool dimensions
            dimensions_df = create_pool_dimensions(raw_pools_df)
            # filter by target projects
            filtered_dimensions_df = filter_pools_by_projects(
                dimensions_df, self.target_projects
            )
            # create historical facts (Join TVL + dimensions )
            historical_facts_df = create_historical_facts(
                raw_tvl_df,
                filtered_dimensions_df,
                None,  # No date filter for initial load
            )

            logger.info(
                f"✅ Transformed {filtered_dimensions_df.height} dimensions, {historical_facts_df.height} facts"
            )

            # step 3: save transformed data locally
            logger.info("🔄 Step 3: Saving transformed data locally...")
            today = date.today().strftime("%Y-%m-%d")
            save_transformed_data(filtered_dimensions_df, historical_facts_df, today)

            # step 4: load to Dune (if not dry run)
            if not self.dry_run:
                logger.info("🔄 Step 4: Loading data to Dune...")
                self.dune_uploader.upload_full_historical_facts(historical_facts_df)
                logger.info("✅ Initial loed to Dune completed successfully")
            else:
                logger.info("🔍 DRY RUN: Skipping Dune upload")

            return True

        except Exception as e:
            logger.error(f"❌ Initial load failed: {e}")
            raise

    def run_daily_update(self, target_date: Optional[date] = None) -> bool:
        """
        Run daily update pipeline

        Args:
            target_date: Date to update (defaults to today)
        Returns:
            bool: True if successful
        """
        if target_date is None:
            target_date = date.today()

        logger.info(f"🔄 Starting daily update pipeline for {target_date}")

        try:
            # Step 1: Extract raw data
            logger.info("📁 Step 1: Extracting raw data...")

            # Check if pools data is fresh
            if self._is_data_fresh("pools"):
                logger.info("�� Pools data is fresh, loading from cache...")
                raw_pools_df = self._load_cached_data("pools", target_date)
                if raw_pools_df is None:
                    logger.info("📦 Cache miss, fetching fresh pools data...")
                    raw_pools_df = fetch_raw_pools_data()
                    self._save_fetch_metadata("pools")
            else:
                logger.info("📦 Pools data is stale, fetching fresh data...")
                raw_pools_df = fetch_raw_pools_data()
                self._save_fetch_metadata("pools")

            # For TVL, we always fetch ALL historical data (DeFiLlama API limitation)
            # Then we filter for today's data in the append logic
            logger.info(
                "📦 Fetching ALL historical TVL data (DeFiLlama API limitation)..."
            )
            pool_ids = raw_pools_df.select("pool").to_series().to_list()

            # Fetch ALL historical data (not incremental)
            logger.info(
                f"🎯 Fetching ALL historical TVL data for {len(pool_ids)} pools..."
            )
            raw_tvl_df = fetch_raw_tvl_data(pool_ids)
            self._save_fetch_metadata("tvl")

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

            # Create historical facts (join TVL + dimensions)
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
                # Check if this is the first run (table doesn't exist or is empty)
                if self._is_first_run():
                    logger.info(
                        f"☁️ Step 4: First run - uploading FULL historical facts..."
                    )
                    self.dune_uploader.upload_full_historical_facts(historical_facts_df)
                else:
                    logger.info(
                        f"☁️ Step 4: Subsequent run - appending daily facts for {target_date}..."
                    )
                    self.dune_uploader.append_daily_facts(
                        historical_facts_df, target_date
                    )

                # Step 5: Cleanup temporary file after successful upload
                logger.info("🧹 Step 5: Cleaning up temporary files...")
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    logger.info(f"✅ Cleaned up {temp_file}")

                logger.info(
                    f"✅ Daily update completed successfully for {target_date}!"
                )
            else:
                logger.info("🔍 Dry run: Skipping Dune upload")

            return True

        except Exception as e:
            logger.error(f"❌ Daily update failed: {e}")
            raise

    def _is_first_run(self) -> bool:
        """
        Check if this is the first run (table doesn't exist or is empty)

        Returns:
            bool: True if this is the first run
        """
        try:
            # Try to get table info
            url = f"{self.dune_uploader.base_url}/table/{self.dune_uploader.namespace}/{self.dune_uploader.facts_table}"
            response = self.dune_uploader.session.get(url)

            if response.status_code == 404:
                logger.info("Table doesn't exist yet - this is the first run")
                return True

            # If table exists, check if it's empty by trying to get row count
            # For now, we'll be conservative and assume it's not the first run
            # In production, you'd want to query the actual row count
            logger.info("Table exists - assuming this is not the first run")
            return False

        except Exception as e:
            logger.warning(f"Could not check if this is first run: {e}")
            # If we can't check, assume it's the first run to be safe
            return True

    def get_pipeline_status(self) -> dict:
        """
        Get current pipeline status

        Returns:
            dict: Pipeline status information
        """
        status = {
            "timestamp": datetime.now().isoformat(),
            "dry_run": self.dry_run,
            "target_projects": list(self.target_projects),
            "dune_connected": False,
            "data_freshness": {
                "pools": self._is_data_fresh("pools"),
                "tvl": self._is_data_fresh("tvl"),
            },
        }

        if not self.dry_run and self.dune_uploader:
            try:
                table_info = self.dune_uploader.get_table_info()
                status["dune_connected"] = True
                status["dune_table_info"] = table_info
            except Exception as e:
                status["dune_error"] = str(e)

        return status

    def _fetch_incremental_tvl_data(
        self, pool_ids: List[str], target_date: date
    ) -> pl.DataFrame:
        """
        Fetch TVL data INCREMENTALLY for target date (with smart caching)

        Args:
            pool_ids: List of pool IDs to fetch
            target_date: Target date for TVL data

        Returns:
            DataFrame with TVL data for target date only
        """
        from src.extract.data_fetcher import fetch_incremental_tvl_data

        logger.info(f"🎯 INCREMENTAL: Fetching TVL data for {target_date}")
        logger.info(f"📊 Pool IDs to fetch: {len(pool_ids)}")

        # Use the new incremental fetcher with caching
        incremental_data = fetch_incremental_tvl_data(pool_ids, target_date)

        logger.info(
            f"✅ INCREMENTAL: Retrieved {incremental_data.height} records for {target_date}"
        )

        return incremental_data


def main():
    """Main function for running the pipeline"""
    import argparse

    parser = argparse.ArgumentParser(description="DeFillama Data Pipeline")
    parser.add_argument(
        "--mode",
        choices=["initial", "daily"],
        required=True,
        help="Pipeline model: intial load or daily update",
    )
    parser.add_argument(
        "--date", type=str, help="Target date for daily update (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no Dune uploads)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # initialize orchestrator
    orchestrator = PipelineOrchestrator(dry_run=args.dry_run)

    try:
        if args.mode == "initial":
            logger.info("🚀 Running initial load...")
            success = orchestrator.run_initial_load()
        elif args.mode == "daily":
            target_date = None
            if args.date:
                target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            logger.info(f"🔄 Running daily update for {target_date} or 'today'...")
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

#!/usr/bin/env python3
"""
Truly incremental pipeline for DeFiLlama data
"""

import os
import logging
from datetime import date, datetime
from typing import Optional

from src.extract.data_fetcher import fetch_raw_pools_data, filter_pools_by_projects
from src.extract.incremental_fetcher import IncrementalFetcher
from src.transformation.transformers import create_pool_dimensions, create_historical_facts
from src.load.dune_uploader import DuneUploader

logger = logging.getLogger(__name__)

class IncrementalPipeline:
    """Truly incremental pipeline that only processes new data"""
    
    def __init__(
        self,
        target_projects: Optional[list] = None,
        dry_run: bool = False,
        namespace: str = "uniswap_fnd"
    ):
        self.target_projects = target_projects or ["pancakeswap-amm", "pancakeswap-amm-v3"]
        self.dry_run = dry_run
        self.incremental_fetcher = IncrementalFetcher()
        self.dune_uploader = DuneUploader(namespace=namespace) if not dry_run else None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def run_initial_load(self) -> bool:
        """
        Run initial full load (one-time setup)
        This fetches ALL historical data
        """
        logger.info("🚀 Starting INITIAL LOAD (full historical data)")
        logger.info("=" * 60)
        
        try:
            # Step 1: Fetch all pools data
            logger.info("📊 Step 1: Fetching all pools data...")
            raw_pools_df = fetch_raw_pools_data()
            logger.info(f"✅ Fetched {raw_pools_df.height} pools")
            
            # Step 2: Fetch all TVL data
            logger.info("📊 Step 2: Fetching all historical TVL data...")
            pool_ids = raw_pools_df.select("pool").to_series().to_list()
            raw_tvl_df = fetch_raw_tvl_data(pool_ids)
            logger.info(f"✅ Fetched {raw_tvl_df.height} TVL records")
            
            # Step 3: Transform data
            logger.info("🔄 Step 3: Transforming data...")
            dimensions_df = create_pool_dimensions(raw_pools_df)
            filtered_dimensions_df = filter_pools_by_projects(dimensions_df, self.target_projects)
            historical_facts_df = create_historical_facts(raw_tvl_df, filtered_dimensions_df, None)
            
            logger.info(f"✅ Created {historical_facts_df.height} historical facts")
            
            # Step 4: Upload to Dune
            if not self.dry_run:
                logger.info("☁️ Step 4: Uploading to Dune...")
                self.dune_uploader.create_historical_facts_table()
                self.dune_uploader.upload_historical_facts_data(historical_facts_df)
                logger.info("✅ Initial load completed!")
            
            # Step 5: Save state
            self.incremental_fetcher.save_run_state(
                datetime.now(), 
                raw_pools_df.height, 
                raw_tvl_df.height
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Initial load failed: {e}")
            raise
    
    def run_incremental_update(self, target_date: Optional[date] = None) -> bool:
        """
        Run incremental update for a specific date
        This fetches ONLY data for the target date
        """
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"🔄 Starting INCREMENTAL UPDATE for {target_date}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Check if we need to fetch pools (weekly refresh)
            if self.incremental_fetcher.should_fetch_pools():
                logger.info("📊 Step 1: Fetching fresh pools data (weekly refresh)...")
                raw_pools_df = fetch_raw_pools_data()
                self.incremental_fetcher.save_run_state(
                    datetime.now(), 
                    raw_pools_df.height, 
                    0  # TVL count will be updated later
                )
            else:
                logger.info("📊 Step 1: Using cached pools data...")
                # Load from cache or fetch minimal data
                raw_pools_df = fetch_raw_pools_data()
            
            # Step 2: Check if we need to fetch TVL for target date
            if self.incremental_fetcher.should_fetch_tvl(target_date):
                logger.info(f"📊 Step 2: Fetching incremental TVL data for {target_date}...")
                pool_ids = raw_pools_df.select("pool").to_series().to_list()
                raw_tvl_df = self.incremental_fetcher.get_incremental_tvl_data(pool_ids, target_date)
                logger.info(f"✅ Fetched {raw_tvl_df.height} TVL records for {target_date}")
            else:
                logger.info(f"📊 Step 2: TVL data for {target_date} is fresh, skipping fetch")
                return True  # No new data to process
            
            # Step 3: Transform data
            logger.info("🔄 Step 3: Transforming incremental data...")
            dimensions_df = create_pool_dimensions(raw_pools_df)
            filtered_dimensions_df = filter_pools_by_projects(dimensions_df, self.target_projects)
            historical_facts_df = create_historical_facts(raw_tvl_df, filtered_dimensions_df, target_date)
            
            logger.info(f"✅ Created {historical_facts_df.height} facts for {target_date}")
            
            # Step 4: Upload to Dune (incremental)
            if not self.dry_run and historical_facts_df.height > 0:
                logger.info(f"☁️ Step 4: Upserting incremental data for {target_date}...")
                self.dune_uploader.upsert_daily_facts(historical_facts_df, target_date)
                logger.info(f"✅ Incremental update completed for {target_date}!")
            elif historical_facts_df.height == 0:
                logger.warning(f"⚠️ No data found for {target_date}, skipping upload")
            else:
                logger.info("🔍 Dry run: Skipping Dune upload")
            
            # Step 5: Update state
            self.incremental_fetcher.save_run_state(
                datetime.now(), 
                raw_pools_df.height, 
                raw_tvl_df.height
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Incremental update failed: {e}")
            raise
    
    def get_pipeline_status(self) -> dict:
        """Get current pipeline status"""
        return {
            "timestamp": datetime.now().isoformat(),
            "incremental_state": self.incremental_fetcher.get_run_summary(),
            "target_projects": self.target_projects,
            "dry_run": self.dry_run
        }

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Incremental DeFiLlama Pipeline")
    parser.add_argument("--mode", choices=["initial", "incremental"], required=True,
                       help="Pipeline mode: initial (full load) or incremental (daily update)")
    parser.add_argument("--date", type=str, help="Target date for incremental mode (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode")
    
    args = parser.parse_args()
    
    pipeline = IncrementalPipeline(dry_run=args.dry_run)
    
    if args.mode == "initial":
        success = pipeline.run_initial_load()
    elif args.mode == "incremental":
        target_date = date.fromisoformat(args.date) if args.date else None
        success = pipeline.run_incremental_update(target_date)
    
    if success:
        print("✅ Pipeline completed successfully!")
    else:
        print("❌ Pipeline failed!")
        exit(1)

if __name__ == "__main__":
    main()

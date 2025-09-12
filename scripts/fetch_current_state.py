#!/usr/bin/env python3
"""
Fetch current state data from DeFiLlama PoolsOld API endpoint.
This replaces the old metadata approach with enhanced current state data.
"""

import polars as pl
from datetime import date
from src.datasources.defillama.yieldpools.pools_old import YieldPoolsCurrentState
from src.datasources.defillama.yieldpools.schemas import (
    validate_metadata_response,
    metadata_to_polars,
)

# Same target projects as fetch_yields.py
TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
}


def main():
    """Fetch current state data for all pools and filter by target projects"""
    print("🔄 Fetching current state data from DeFiLlama PoolsOld API...")

    # Fetch current state data for all pools
    current_state = YieldPoolsCurrentState.fetch()
    print(f"✅ Fetched {current_state.df.shape[0]} total pools")

    # Validate the data using our enhanced schema
    try:
        # Convert the DataFrame back to the format expected by our schema
        # This validates that the data structure matches our expectations
        metadata_dict = {
            "pools": [
                {
                    "pool": row["pool"],
                    "protocol_slug": row["protocol_slug"],
                    "chain": row["chain"],
                    "symbol": row["symbol"],
                    "underlying_tokens": row["underlying_tokens"],
                    "reward_tokens": row["reward_tokens"],
                    "timestamp": row["timestamp"],
                    "tvl_usd": row["tvl_usd"],
                    "apy": row["apy"],
                    "apy_base": row["apy_base"],
                    "apy_reward": row["apy_reward"],
                    "pool_old": row["pool_old"],
                }
                for row in current_state.df.iter_rows(named=True)
            ]
        }

        # Validate using our enhanced schema
        validated_metadata = validate_metadata_response(metadata_dict)
        print(f"✅ Schema validation passed for {len(validated_metadata.pools)} pools")

    except Exception as validation_error:
        print(f"❌ Schema validation failed: {validation_error}")
        print("Continuing with unvalidated data...")

    # Filter for target projects
    filtered_state = current_state.filter_by_projects(TARGET_PROJECTS)
    print(f"🎯 Filtered to {filtered_state.df.shape[0]} pools from target projects")

    # Show project distribution
    project_counts = (
        filtered_state.df.select("protocol_slug")
        .group_by("protocol_slug")
        .len()
        .sort("len", descending=True)
    )
    print("\n📊 Project distribution:")
    for row in project_counts.iter_rows(named=True):
        print(f"  {row['protocol_slug']}: {row['len']} pools")

    # Save to JSON for inspection and future use
    today = date.today().strftime("%Y-%m-%d")
    json_path = f"output/current_state_{today}.json"
    filtered_state.to_json(json_path)
    print(f"\n💾 Saved enhanced metadata to {json_path}")

    # Also save as Parquet for future use (much smaller file size)
    parquet_path = f"output/current_state_{today}.parquet"
    filtered_state.to_parquet(parquet_path)
    print(f"💾 Saved enhanced metadata to {parquet_path}")

    # Show some sample data
    print(f"\n📋 Sample data (first 3 pools):")
    sample_df = filtered_state.df.head(3)
    for row in sample_df.iter_rows(named=True):
        print(
            f"  {row['symbol']} ({row['chain']}) - TVL: ${row['tvl_usd']:,.0f}, APY: {row['apy']:.2f}%"
        )


if __name__ == "__main__":
    main()

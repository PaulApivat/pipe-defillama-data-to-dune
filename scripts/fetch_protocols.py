#!/usr/bin/env python3
"""
Fetch protocol data from DeFiLlama /protocols API endpoint.
This script fetches protocol-level TVL data and filters for target projects.
"""

import polars as pl
from pathlib import Path
import json
from src.datasources.defillama.tvl.protocol import ProtocolData


# Target projects - focusing on PancakeSwap variants
TARGET_PROJECTS = {
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
}

# Additional protocols for context (optional)
ADDITIONAL_PROTOCOLS = {
    "curve-dex",
    "aerodrome-slipstream",
    "aerodrome-v1",
    "uniswap-v3",
    "uniswap-v2",
}

# Combine all target protocols
ALL_TARGET_PROJECTS = TARGET_PROJECTS | ADDITIONAL_PROTOCOLS


def main():
    """Fetch protocol data for target projects"""
    print("🔄 Fetching protocol data from DeFiLlama /protocols API...")

    try:
        # Fetch all protocol data
        protocol_data = ProtocolData.fetch()
        print(f"✅ Fetched {protocol_data.df.shape[0]} total protocols")

        # Filter for target projects
        filtered_protocols = protocol_data.filter_by_projects(ALL_TARGET_PROJECTS)
        print(
            f"🎯 Filtered to {filtered_protocols.df.shape[0]} protocols from target projects"
        )

        # Show which protocols were found
        found_protocols = set(filtered_protocols.df["slug"].to_list())
        missing_protocols = ALL_TARGET_PROJECTS - found_protocols

        print(f"\n📊 Found protocols:")
        for slug in sorted(found_protocols):
            protocol_info = filtered_protocols.get_single_protocol(slug)
            tvl = protocol_info.get("tvl", 0) or 0
            print(f"  ✅ {slug}: ${tvl:,.0f} TVL")

        if missing_protocols:
            print(f"\n⚠️  Missing protocols: {', '.join(sorted(missing_protocols))}")

        # Create output directory
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # Save to JSON for inspection and future use
        json_output_path = output_dir / "protocols.json"
        filtered_protocols.to_json(json_output_path)
        print(f"\n💾 Saved protocol data to {json_output_path}")

        # Save as Parquet for better performance
        parquet_output_path = output_dir / "protocols.parquet"
        filtered_protocols.to_parquet(parquet_output_path)
        print(f"💾 Saved protocol data to {parquet_output_path}")

        # Generate TVL summary
        tvl_summary = filtered_protocols.get_tvl_summary()
        print(f"\n📈 TVL Summary (Top 10):")
        for row in tvl_summary.head(10).iter_rows(named=True):
            tvl = row["tvl"] or 0
            print(f"  {row['slug']}: ${tvl:,.0f}")

        # Generate chain breakdown
        chain_breakdown = filtered_protocols.get_chain_breakdown()
        if chain_breakdown.height > 0:
            print(f"\n🌐 Chain TVL Breakdown:")
            chain_summary = (
                chain_breakdown.group_by("chain")
                .agg(
                    [
                        pl.col("tvl").sum().alias("total_tvl"),
                        pl.col("protocol_slug").n_unique().alias("protocol_count"),
                    ]
                )
                .sort("total_tvl", descending=True)
            )

            for row in chain_summary.iter_rows(named=True):
                print(
                    f"  {row['chain']}: ${row['total_tvl']:,.0f} ({row['protocol_count']} protocols)"
                )

        # Show sample data for PancakeSwap protocols specifically
        pancakeswap_protocols = filtered_protocols.filter_by_projects(TARGET_PROJECTS)
        if pancakeswap_protocols.df.height > 0:
            print(f"\n🥞 PancakeSwap Protocol Details:")
            for row in pancakeswap_protocols.df.iter_rows(named=True):
                print(f"\n  Protocol: {row['name']} ({row['slug']})")
                print(f"    Symbol: {row['symbol']}")
                print(f"    TVL: ${row['tvl']:,.0f}" if row["tvl"] else "    TVL: N/A")

                # Show chain breakdown if available
                if row["chainTvls"]:
                    try:
                        chain_tvls = json.loads(row["chainTvls"])
                        if chain_tvls:
                            print(f"    Chain TVL:")
                            for chain, tvl in sorted(
                                chain_tvls.items(),
                                key=lambda x: x[1] or 0,
                                reverse=True,
                            ):
                                if tvl and tvl > 0:
                                    print(f"      {chain}: ${tvl:,.0f}")
                    except (json.JSONDecodeError, TypeError):
                        pass

        print(f"\n✅ Protocol data fetch completed successfully!")

    except Exception as e:
        print(f"❌ Error fetching protocol data: {e}")
        raise


if __name__ == "__main__":
    main()

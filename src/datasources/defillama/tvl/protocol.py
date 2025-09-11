from dataclasses import dataclass
from datetime import date
import polars as pl
import json
from src.coreutils.request import new_session, get_data
from src.datasources.defillama.yieldpools.schemas import (
    protocols_to_polars,
)

PROTOCOLS_ENDPOINT = "https://api.llama.fi/protocols"


@dataclass
class ProtocolData:
    """Protocol data from DeFiLlama /protocols API"""

    df: pl.DataFrame

    @classmethod
    def fetch(cls) -> "ProtocolData":
        """Fetch protocol data from /protocols endpoint"""
        # Create a new session for this fetch operation
        session = new_session()
        try:
            response = get_data(session, PROTOCOLS_ENDPOINT)
            return cls.of(data=response, process_dt=date.today())
        finally:
            # Clean up the session
            session.close()

    @classmethod
    def of(cls, data: list[dict], process_dt: date) -> "ProtocolData":
        """Create instance from raw API data"""
        records = []

        for protocol in data:
            # Map API fields to our schema with proper None handling and type conversion
            row = {
                "id": str(protocol.get("id", "")),
                "name": str(protocol.get("name", "")),
                "address": str(protocol.get("address", "")),
                "symbol": str(protocol.get("symbol", "")),
                "category": str(protocol.get("category", "")),
                "chains": (
                    json.dumps(protocol.get("chains", []))
                    if protocol.get("chains")
                    else None
                ),
                "slug": str(protocol.get("slug", "")),
                "tvl": (
                    float(protocol.get("tvl"))
                    if protocol.get("tvl") is not None
                    else None
                ),
                "chainTvls": json.dumps(protocol.get("chainTvls") or {}),
            }

            records.append(row)

        # Use explicit schema to avoid inference issues
        from ..yieldpools.schemas import PROTOCOL_SCHEMA

        return cls(df=pl.DataFrame(records, schema=PROTOCOL_SCHEMA))

    def filter_by_projects(self, target_projects: set[str]) -> "ProtocolData":
        """Filter protocols by target project slugs"""
        filtered_df = self.df.filter(pl.col("slug").is_in(target_projects))
        return ProtocolData(df=filtered_df)

    def get_single_protocol(self, slug: str) -> dict:
        """Get data for a single protocol by slug"""
        protocol_row = self.df.filter(pl.col("slug") == slug)
        if protocol_row.height == 0:
            raise ValueError(f"Protocol {slug} not found in data")

        # Return as dict for easy access
        return protocol_row.to_dicts()[0]

    def to_json(self, filepath: str) -> None:
        """Save to JSON file"""
        self.df.write_json(filepath)

    def to_parquet(self, filepath: str) -> None:
        """Save to Parquet file"""
        self.df.write_parquet(filepath)

    def get_tvl_summary(self) -> pl.DataFrame:
        """Get TVL summary statistics by protocol"""
        return (
            self.df.select(["slug", "name", "tvl"])
            .filter(pl.col("tvl").is_not_null())
            .sort("tvl", descending=True)
        )

    def get_chain_breakdown(self) -> pl.DataFrame:
        """Get chain TVL breakdown for protocols with chain data"""
        # Parse chainTvls JSON strings and create chain breakdown
        chain_data = []

        for row in self.df.iter_rows(named=True):
            if row["chainTvls"]:
                try:
                    chain_tvls = json.loads(row["chainTvls"])
                    for chain, tvl in chain_tvls.items():
                        if tvl and tvl > 0:  # Only include chains with positive TVL
                            chain_data.append(
                                {
                                    "protocol_slug": row["slug"],
                                    "protocol_name": row["name"],
                                    "chain": chain,
                                    "tvl": float(tvl),
                                }
                            )
                except (json.JSONDecodeError, TypeError):
                    # Skip protocols with invalid chainTvls data
                    continue

        return (
            pl.DataFrame(chain_data)
            if chain_data
            else pl.DataFrame(
                schema={
                    "protocol_slug": pl.String(),
                    "protocol_name": pl.String(),
                    "chain": pl.String(),
                    "tvl": pl.Float64(),
                }
            )
        )

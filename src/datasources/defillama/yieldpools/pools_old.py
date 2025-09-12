from dataclasses import dataclass
from datetime import date
import polars as pl
from src.coreutils.request import new_session, get_data
from src.datasources.defillama.yieldpools.schemas import (
    validate_metadata_response,
    metadata_to_polars,
)

POOLS_OLD_ENDPOINT = "https://yields.llama.fi/poolsOld"


@dataclass
class YieldPoolsCurrentState:
    """Enhanced metadata + current state for yield pools from DeFiLlama PoolsOld API"""

    df: pl.DataFrame

    @classmethod
    def fetch(cls) -> "YieldPoolsCurrentState":
        """Fetch current state data from PoolsOld endpoint"""
        # Create a new session for this fetch operation
        session = new_session()
        try:
            response = get_data(session, POOLS_OLD_ENDPOINT)
            return cls.of(data=response["data"], process_dt=date.today())
        finally:
            # Clean up the session
            session.close()

    @classmethod
    def of(cls, data: list[dict], process_dt: date) -> "YieldPoolsCurrentState":
        """Create instance from raw API data"""
        records = []

        for pool in data:
            # Map API fields to our enhanced schema with proper None handling and type conversion
            row = {
                "pool": str(pool["pool"]),
                "protocol_slug": str(pool["project"]),
                "chain": str(pool["chain"]),
                "symbol": str(pool["symbol"]),
                "underlying_tokens": pool.get("underlyingTokens") or [],
                "reward_tokens": pool.get("rewardTokens") or [],
                "timestamp": str(pool["timestamp"]),
                "tvl_usd": (
                    float(pool.get("tvlUsd"))
                    if pool.get("tvlUsd") is not None
                    else None
                ),
                "apy": float(pool.get("apy")) if pool.get("apy") is not None else None,
                "apy_base": (
                    float(pool.get("apyBase"))
                    if pool.get("apyBase") is not None
                    else None
                ),
                "apy_reward": (
                    float(pool.get("apyReward"))
                    if pool.get("apyReward") is not None
                    else None
                ),
                "pool_old": str(pool.get("pool_old")) if pool.get("pool_old") else None,
            }

            records.append(row)

        # Use explicit schema to avoid inference issues
        from .schemas import METADATA_SCHEMA

        return cls(df=pl.DataFrame(records, schema=METADATA_SCHEMA))

    def filter_by_projects(self, target_projects: set[str]) -> "YieldPoolsCurrentState":
        """Filter pools by target project slugs"""
        filtered_df = self.df.filter(pl.col("protocol_slug").is_in(target_projects))
        return YieldPoolsCurrentState(df=filtered_df)

    def get_single_pool_metadata(self, pool_id: str) -> dict:
        """Get metadata for a single pool by ID"""
        pool_row = self.df.filter(pl.col("pool") == pool_id)
        if pool_row.height == 0:
            raise ValueError(f"Pool {pool_id} not found in current state data")

        # Return as dict for easy merging
        return pool_row.to_dicts()[0]

    def to_json(self, filepath: str) -> None:
        """Save to JSON file"""
        self.df.write_json(filepath)

    def to_parquet(self, filepath: str) -> None:
        """Save to Parquet file (for future use)"""
        self.df.write_parquet(filepath)

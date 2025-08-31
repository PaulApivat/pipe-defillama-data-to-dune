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
                "dt": str(process_dt),  # Ensure string format
                "pool": str(pool["pool"]),
                "protocol_slug": str(pool["project"]),
                "chain": str(pool["chain"]),
                "symbol": str(pool["symbol"]),
                "underlying_tokens": pool.get("underlyingTokens") or [],
                "reward_tokens": pool.get("rewardTokens") or [],
                "timestamp": str(pool["timestamp"]),
                "pool_meta": (
                    str(pool.get("poolMeta")) if pool.get("poolMeta") else None
                ),
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
                "il_7d": (
                    float(pool.get("il7d")) if pool.get("il7d") is not None else None
                ),
                "apy_base_7d": (
                    float(pool.get("apyBase7d"))
                    if pool.get("apyBase7d") is not None
                    else None
                ),
                "volume_usd_1d": (
                    float(pool.get("volumeUsd1d"))
                    if pool.get("volumeUsd1d") is not None
                    else None
                ),
                "volume_usd_7d": (
                    float(pool.get("volumeUsd7d"))
                    if pool.get("volumeUsd7d") is not None
                    else None
                ),
                "apy_base_inception": (
                    float(pool.get("apyBaseInception"))
                    if pool.get("apyBaseInception") is not None
                    else None
                ),
                "url": str(pool.get("url")) if pool.get("url") else None,
                "apy_pct_1d": (
                    float(pool.get("apyPct1D"))
                    if pool.get("apyPct1D") is not None
                    else None
                ),
                "apy_pct_7d": (
                    float(pool.get("apyPct7D"))
                    if pool.get("apyPct7D") is not None
                    else None
                ),
                "apy_pct_30d": (
                    float(pool.get("apyPct30D"))
                    if pool.get("apyPct30D") is not None
                    else None
                ),
                "apy_mean_30d": (
                    float(pool.get("apyMean30d"))
                    if pool.get("apyMean30d") is not None
                    else None
                ),
                "stablecoin": bool(pool["stablecoin"]),
                "il_risk": str(pool["ilRisk"]),
                "exposure": str(pool["exposure"]),
                "return_value": (
                    float(pool.get("return"))
                    if pool.get("return") is not None
                    else None
                ),
                "count": (
                    int(pool.get("count")) if pool.get("count") is not None else None
                ),
                "apy_mean_expanding": (
                    float(pool.get("apyMeanExpanding"))
                    if pool.get("apyMeanExpanding") is not None
                    else None
                ),
                "apy_std_expanding": (
                    float(pool.get("apyStdExpanding"))
                    if pool.get("apyStdExpanding") is not None
                    else None
                ),
                "mu": float(pool.get("mu")) if pool.get("mu") is not None else None,
                "sigma": (
                    float(pool.get("sigma")) if pool.get("sigma") is not None else None
                ),
                "outlier": (
                    bool(pool.get("outlier"))
                    if pool.get("outlier") is not None
                    else None
                ),
                "project_factorized": (
                    int(pool.get("project_factorized"))
                    if pool.get("project_factorized") is not None
                    else None
                ),
                "chain_factorized": (
                    int(pool.get("chain_factorized"))
                    if pool.get("chain_factorized") is not None
                    else None
                ),
                "predictions": (
                    str(pool.get("predictions")) if pool.get("predictions") else None
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

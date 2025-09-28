from dataclasses import dataclass
from datetime import date
import polars as pl
import logging
from src.coreutils.request import new_session, get_data
from src.datasources.defillama.yieldpools.schemas import (
    validate_metadata_response,
    metadata_to_polars,
)
from typing import Callable, Optional, Union
from functools import wraps

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
        from .schemas import CURRENT_STATE_SCHEMA

        return cls(df=pl.DataFrame(records, schema=CURRENT_STATE_SCHEMA))

    def __init__(self, df: pl.DataFrame):
        self.df = df
        self.logger = logging.getLogger(__name__)

    def pipe(self, *operations: Callable) -> "YieldPoolsCurrentState":
        """Chain multiple operations functionally"""
        result = self.df
        for operation in operations:
            result = operation(result)
        return YieldPoolsCurrentState(df=result)

    def filter_by_projects(self, target_projects: set[str]) -> "YieldPoolsCurrentState":
        """Filter pools by target project slugs"""
        filtered_df = self.df.filter(pl.col("protocol_slug").is_in(target_projects))
        return YieldPoolsCurrentState(df=filtered_df)

    def validate_schema(self, schema: pl.Schema) -> "YieldPoolsCurrentState":
        """Validate and cast DataFrame to specified schema w error handling and logging"""
        self.logger.info(f"Validating schema for {self.df.height} pools")
        try:
            validated_df = self.df.cast(schema)
            self.logger.info(f"✅ Schema validation passed for {self.df.height} pools")
            return YieldPoolsCurrentState(df=validated_df)
        except Exception as validation_error:
            self.logger.error(f"❌ Schema validation failed: {validation_error}")
            self.logger.warning("Continuing with unvalidated data...")
            return YieldPoolsCurrentState(
                df=self.df
            )  # Return original, unvalidated data

    def transform_pool_old(self) -> "YieldPoolsCurrentState":
        """Transform pool_old column to remove chain suffix"""
        self.logger.info("Transforming pool_old column")
        transformed_df = self.df.with_columns(
            [
                pl.when(
                    pl.col("pool_old").str.split("-").list.get(0).str.starts_with("0x")
                )
                .then(pl.col("pool_old").str.split("-").list.get(0))
                .when(
                    pl.col("pool_old").str.split("-").list.get(1).str.starts_with("0x")
                )
                .then(pl.col("pool_old").str.split("-").list.get(1))
                .otherwise(pl.col("pool_old"))
                .alias("pool_old_clean")
            ]
        )
        return YieldPoolsCurrentState(df=transformed_df)

    def sort_by_tvl(self, descending: bool = True) -> "YieldPoolsCurrentState":
        """Sort pools by TVL"""
        sorted_df = self.df.sort("tvl_usd", descending=descending)
        return YieldPoolsCurrentState(df=sorted_df)

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

    def to_duckdb_query(self, query: str) -> pl.DataFrame:
        """Execute DuckDB query on the DataFrame"""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.register("pools", self.df)
        return conn.execute(query).pl()

    def get_summary_stats(self) -> dict:
        """Get summary statistics using CURRENT_STATE_SCHEMA state"""
        from .schemas import CURRENT_STATE_SCHEMA

        stats = {}

        # Add basic statistics
        stats["total_pools"] = self.df.height
        stats["total_records"] = self.df.height

        # Add basic DataFrame info
        stats["columns"] = len(self.df.columns)
        stats["memory_usage"] = self.df.estimated_size()

        for field_name, field_type in CURRENT_STATE_SCHEMA.items():
            if field_name in self.df.columns:
                if field_type == pl.List(pl.String()):
                    # For list fields, get count statistics
                    stats[f"{field_name}_avg_count"] = self.df.select(
                        pl.col(field_name).list.len().mean()
                    ).item()
                    stats[f"{field_name}_max_count"] = self.df.select(
                        pl.col(field_name).list.len().max()
                    ).item()
                elif field_type in [pl.Float64(), pl.Int64()]:
                    # For numeric fields, get sum and mean
                    stats[f"{field_name}_sum"] = self.df.select(
                        pl.col(field_name).sum()
                    ).item()
                    stats[f"{field_name}_mean"] = self.df.select(
                        pl.col(field_name).mean()
                    ).item()
                else:
                    # For string fields, get unique count
                    stats[f"{field_name}_unique"] = self.df.select(
                        pl.col(field_name).n_unique()
                    ).item()

        return stats

    def update_scd2_dimensions(self, snap_date: date) -> pl.DataFrame:
        """Update SCD2 dimension table"""
        from .scd2_manager import SCD2Manager

        with SCD2Manager() as scd2_manager:
            # Register the current state data
            scd2_manager.register_dataframes(self.df, None)
            # Update SCD2 dimensions
            return scd2_manager.update_scd2_dimension_sql(snap_date)

    def save_scd2_dimensions(self, scd2_df: pl.DataFrame) -> None:
        """Save SCD2 dimensions to parquet file"""
        scd2_df.write_parquet("output/pool_dim_scd2.parquet")
        self.logger.info("✅ Saved SCD2 dimensions to output/pool_dim_scd2.parquet")

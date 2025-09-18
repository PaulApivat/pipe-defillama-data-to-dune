from dataclasses import dataclass
from datetime import date, datetime
import polars as pl
from src.coreutils.request import new_session, get_data
from src.datasources.defillama.yieldpools.schemas import TVL_SCHEMA
from typing import Callable, Optional, Union, List, Dict, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from src.datasources.defillama.yieldpools.current_state import (
        YieldPoolsCurrentState,
    )


@dataclass
class YieldPoolsTVLFact:
    """Historical TVL fact data for yield pools from Defillama API"""

    df: pl.DataFrame
    logger = logging.getLogger(__name__)

    def __init__(self, df: pl.DataFrame):
        """Initialize with DataFrame and setup logging"""
        self.df = df
        self.logger = logging.getLogger(__name__)

    def pipe(self, *operations: Callable) -> "YieldPoolsTVLFact":
        """Chain multiple operations functionally"""
        result = self.df
        for operation in operations:
            result = operation(result)
        return YieldPoolsTVLFact(df=result)

    @classmethod
    def load_metadata(cls, metadata_source: "YieldPoolsCurrentState") -> List[str]:
        """Load pool IDs from metadata for TVL fetching"""
        pool_ids = metadata_source.df.select("pool").to_series().to_list()
        cls.logger.info(f"Loaded {len(pool_ids)} pool IDs from metadata")
        return pool_ids

    @classmethod
    def load_existing_tvl(cls, filepath: str) -> "YieldPoolsTVLFact":
        """Load existing TVL data from file"""
        try:
            df = pl.read_parquet(filepath)
            cls.logger.info(
                f"Loaded existing TVL from {filepath} : {df.height} records"
            )
            return cls(df=df)
        except Exception as e:
            cls.logger.error(f"❌ Error loading TVL data from {filepath}: {e}")
            cls.logger.warning("Continuing with empty data...")
            return cls(df=pl.DataFrame(schema=TVL_SCHEMA))

    @classmethod
    def fetch_for_pool(cls, pool_id: str) -> "YieldPoolsTVLFact":
        """Fetch historical TVL data for a single pool"""
        url = f"https://yields.llama.fi/chart/{pool_id}"
        session = new_session()
        try:
            response = get_data(session, url)
            return cls.of(data=response["data"], pool_id=pool_id)
        finally:
            session.close()

    @classmethod
    def fetch_for_pools(cls, pool_ids: List[str]) -> "YieldPoolsTVLFact":
        """Fetch historical TVL data for multiple pools"""
        all_data = []
        failed_pools = []

        for i, pool_id in enumerate(pool_ids, start=1):
            try:
                cls.logger.info(f"Fetching TVL for pool {i}/{len(pool_ids)}: {pool_id}")
                pool_data = cls.fetch_for_pool(pool_id)
                all_data.append(pool_data.df)
            except Exception as e:
                cls.logger.error(f"❌ Error fetching data for pool {pool_id}: {e}")
                cls.logger.warning(f"Skipping pool {pool_id}...")
                failed_pools.append(pool_id)
                continue

        if not all_data:
            cls.logger.error("No TVL data could be fetched for any pools")
            return cls(df=pl.DataFrame(schema=TVL_SCHEMA))

        combined_df = pl.concat(all_data)
        cls.logger.info(f"Successfully fetched TVL data for {len(all_data)} pools")
        if failed_pools:
            cls.logger.warning(
                f"Failed to fetch TVL for {len(failed_pools)} pools: {failed_pools}"
            )

        return cls(df=combined_df)

    @classmethod
    def fetch_tvl_for_metadata_pools(
        cls, metadata_source: "YieldPoolsCurrentState"
    ) -> "YieldPoolsTVLFact":
        """Fetch TVL data for all pools in metadata"""
        pool_ids = cls.load_metadata(metadata_source)
        return cls.fetch_for_pools(pool_ids)

    @classmethod
    def fetch_incremental_tvl(
        cls, metadata_source: "YieldPoolsCurrentState", existing_tvl_file: str = None
    ) -> "YieldPoolsTVLFact":
        """Fetch incremental TVL data (new pools only)"""
        # load existing TVL data if available
        existing_tvl = (
            cls.load_existing_tvl(existing_tvl_file) if existing_tvl_file else None
        )

        # get all pool IDs from metadata
        all_pool_ids = cls.load_metadata(metadata_source)

        if existing_tvl and existing_tvl.df.height > 0:
            # get existing pool ids
            existing_pool_ids = (
                existing_tvl.df.select("pool_id").unique().to_series().to_list()
            )

            # find new pool ids
            new_pool_ids = [pid for pid in all_pool_ids if pid not in existing_pool_ids]

            if new_pool_ids:
                cls.logger.info(f"Found {len(new_pool_ids)} new pools to fetch TVL for")
                new_tvl = cls.fetch_for_pools(new_pool_ids)

                # combine with existing data
                if existing_tvl.df.height > 0:
                    combined_df = pl.concat([existing_tvl.df, new_tvl.df])
                    return cls(df=combined_df)
                else:
                    return new_tvl

            else:
                cls.logger.info("No new pools found, return existing TVL data")
                return existing_tvl

        else:
            # No existing data, fetch all
            cls.logger.info("No existing TVL data, fetching all pools")
            return cls.fetch_for_pools(all_pool_ids)

    @classmethod
    def of(cls, data: List[Dict], pool_id: str) -> "YieldPoolsTVLFact":
        """Create instance from raw API data"""
        records = []
        for point in data:
            row = {
                "timestamp": point["timestamp"],
                "tvl_usd": point["tvlUsd"],
                "apy": point["apy"],
                "apy_base": point["apyBase"],
                "apy_reward": point["apyReward"],
                "pool_id": pool_id,
            }
            records.append(row)

        return cls(df=pl.DataFrame(records, schema=TVL_SCHEMA))

    def filter_by_date_range(
        self, start_date: str, end_date: str
    ) -> "YieldPoolsTVLFact":
        """Filter TVL data by date range"""
        self.logger.info(
            f"Filtering TVL data by date range: {start_date} to {end_date}"
        )
        filtered_df = self.df.filter(
            (pl.col("timestamp") >= start_date) & (pl.col("timestamp") <= end_date)
        )
        return YieldPoolsTVLFact(df=filtered_df)

    def validate_schema(self, schema: pl.Schema = TVL_SCHEMA) -> "YieldPoolsTVLFact":
        """Validate and cast DataFrame to specified schema"""
        self.logger.info(f"Validating schema for {self.df.height} records")
        try:
            validated_df = self.df.cast(schema)
            self.logger.info(
                f"✅ TVL schema validation passed for {self.df.height} records"
            )
            return YieldPoolsTVLFact(df=validated_df)
        except Exception as validation_error:
            self.logger.error(f"❌ TVL schema validation failed: {validation_error}")
            self.logger.warning("Continuing with unvalidated data...")
            return YieldPoolsTVLFact(df=self.df)

    def sort_by_timestamp(self, descending: bool = True) -> "YieldPoolsTVLFact":
        """Sort TVL data by timestamp"""
        self.logger.info(f"Sorting TVL data by timestamp (descending={descending})")
        sorted_df = self.df.sort("timestamp", descending=descending)
        return YieldPoolsTVLFact(df=sorted_df)

    def select_columns(self, columns: List[str]) -> "YieldPoolsTVLFact":
        """Select specific columns"""
        self.logger.info(f"Selecting columns: {columns}")
        selected_df = self.df.select(columns)
        return YieldPoolsTVLFact(df=selected_df)

    def get_summary_stats(self) -> Dict:
        """Get summary stats using TVL_SCHEMA"""
        from .schemas import TVL_SCHEMA

        stats = {}

        # add basic stats
        stats["total_records"] = self.df.height
        stats["unique_pools"] = self.df.select(pl.col("pool_id").n_unique()).item()

        # add field specific stats contained in TVL_SCHEMA
        for field_name, field_type in TVL_SCHEMA.items():
            if field_name in self.df.columns:
                # for list fields, get count stats
                if field_type == pl.List(pl.String()):
                    stats[f"{field_name}_avg_count"] = self.df.select(
                        pl.col(field_name).list.len().mean()
                    ).item()
                    stats[f"{field_name}_max_count"] = self.df.select(
                        pl.col(field_name).list.len().max()
                    ).item()
                elif field_type in [pl.Float64(), pl.Int64()]:
                    # for numeric fields, get sum and mean
                    stats[f"{field_name}_sum"] = self.df.select(
                        pl.col(field_name).sum()
                    ).item()
                    stats[f"{field_name}_mean"] = self.df.select(
                        pl.col(field_name).mean()
                    ).item()
                else:
                    # for string fields, get unique count
                    stats[f"{field_name}_unique"] = self.df.select(
                        pl.col(field_name).n_unique()
                    ).item()

        # add date range stats for historical TVL data
        if "timestamp" in self.df.columns:
            stats["date_range"] = {
                "start": self.df.select(pl.col("timestamp").min()).item(),
                "end": self.df.select(pl.col("timestamp").max()).item(),
            }

        return stats

    def to_json(self, filepath: str) -> None:
        """Save to JSON file"""
        self.logger.info(f"Saving TVL data to JSON: {filepath}")
        self.df.write_json(filepath)

    def to_parquet(self, filepath: str) -> None:
        """Save to Parquet file"""
        self.logger.info(f"Saving TVL data to Parquet: {filepath}")
        self.df.write_parquet(filepath)

    def to_duckdb_query(self, query: str) -> pl.DataFrame:
        """Execute DuckDB query on the DataFrame"""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.register("tvl_data", self.df)
        return conn.execute(query).pl()

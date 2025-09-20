import polars as pl
import duckdb
from datetime import date, datetime
from typing import List, Dict, Any, Optional
from src.datasources.defillama.yieldpools.schemas import DAILY_METRICS_SCHEMA
from src.coreutils.logging import setup_logging


class SCD2Manager:
    """Functional SCD2 management using SQL operations to create tables and as-of joins"""

    def __init__(self):
        self.logger = setup_logging()
        self.conn = duckdb.connect(":memory:")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def register_dataframes(
        self,
        current_state_df: pl.DataFrame,
        existing_scd2_df: Optional[pl.DataFrame] = None,
    ):
        """Register DataFrames for SQL operations"""
        # Register current state data
        self.conn.register("current_state", current_state_df)

        # Register existing SCD2 data if provided
        if existing_scd2_df is not None:
            self.conn.register("existing_scd2", existing_scd2_df)
        else:
            # Create an empty table with the correct schema if no existing data
            empty_scd2 = pl.DataFrame(
                {
                    "pool_id": [],
                    "protocol_slug": [],
                    "chain": [],
                    "symbol": [],
                    "underlying_tokens": [],
                    "reward_tokens": [],
                    "timestamp": [],
                    "tvl_usd": [],
                    "apy": [],
                    "apy_base": [],
                    "apy_reward": [],
                    "pool_old": [],
                    "valid_from": [],
                    "valid_to": [],
                    "is_current": [],
                    "attrib_hash": [],
                    "is_active": [],
                }
            )
            self.conn.register("existing_scd2", empty_scd2)

    def update_scd2_dimension_sql(self, snap_date: date) -> pl.DataFrame:
        """Update SCD2 dimension table with new current state data using SQL"""
        self.logger.info(f"Updating SCD2 dimensions for snapshot date: {snap_date}")

        # SQL for SCD2 update
        sql = f"""
        WITH new_dims AS (
            SELECT
                pool as pool_id 
                , protocol_slug 
                , chain 
                , symbol 
                , underlying_tokens 
                , reward_tokens
                , timestamp
                , tvl_usd
                , apy 
                , apy_base
                , apy_reward
                , pool_old 
                , '{snap_date}'::DATE as valid_from 
                , '9999-12-31'::DATE as valid_to 
                , true as is_current 
                , md5(protocol_slug || '|' || chain || '|' || symbol || '|' || 
                    array_to_string(underlying_tokens, '|') || '|' || 
                    array_to_string(reward_tokens, '|') || '|' || 
                    coalesce(tvl_usd::text, '') || '|' || 
                    coalesce(apy::text, '') || '|' || 
                    coalesce(apy_base::text, '') || '|' || 
                    coalesce(apy_reward::text, '') || '|' || 
                    coalesce(pool_old, '')
                ) as attrib_hash
                , true as is_active
            FROM current_state
        ),

        changed_records AS (
            SELECT n.pool_id
            FROM new_dims n 
            JOIN existing_scd2 e ON n.pool_id = e.pool_id AND e.is_current = true
            WHERE n.attrib_hash != e.attrib_hash
        ),

        closed_existing AS (
            SELECT 
                pool_id, protocol_slug, chain, symbol, underlying_tokens, 
                reward_tokens, timestamp, tvl_usd, apy, apy_base, apy_reward, 
                pool_old, valid_from,
                CASE 
                    WHEN pool_id IN (SELECT pool_id FROM changed_records)
                    THEN '{snap_date}'::DATE 
                    ELSE valid_to 
                END as valid_to,
                CASE
                    WHEN pool_id IN (SELECT pool_id FROM changed_records) 
                    THEN false 
                    ELSE is_current 
                END as is_current,
                attrib_hash, 
                is_active
            FROM existing_scd2 
        )
        SELECT * FROM closed_existing
        UNION ALL
        SELECT * FROM new_dims 
        ORDER BY pool_id, valid_from 
        """

        return self.conn.execute(sql).pl()

    def create_daily_metrics_view_sql(self, tvl_df: pl.DataFrame) -> pl.DataFrame:
        """Create denormalized daily metrics using SQL as-of join"""
        self.logger.info("Creating daily metrics view with SQL as-of join")

        # Register TVL data
        self.conn.register("tvl_data", tvl_df)

        # SQL for as-of join
        sql = """
            WITH tvl_with_date AS (
                SELECT
                    pool_id
                    , tvl_usd 
                    , apy
                    , apy_base
                    , apy_reward
                    , timestamp::DATE as date
                FROM tvl_data
            )
            SELECT
                t.date as timestamp 
                , t.pool_id 
                , d.protocol_slug 
                , d.chain
                , d.symbol
                , t.tvl_usd
                , t.apy 
                , t.apy_base
                , t.apy_reward
            FROM tvl_with_date t
            JOIN existing_scd2 d ON (
                t.pool_id = d.pool_id AND 
                t.date >= d.valid_from AND 
                t.date < d.valid_to
            )
            ORDER BY t.date, t.pool_id
        """

        return self.conn.execute(sql).pl()

    def get_partition_dates(self, days_back: int = 7) -> List[date]:
        """Get date range for partition operations"""
        today = date.today()
        return [today - timedelta(days=i) for i in range(days_back)]

    def create_daily_metrics_partitions(
        self, tvl_df: pl.DataFrame, target_date: date
    ) -> pl.DataFrame:
        """Create daily metrics for specific date partiition"""
        self.logger.info(f"Creating daily metrics partition for {target_date}")

        # Filter TVL data for target date
        tvl_filtered = tvl_df.filter(
            pl.col("timestamp").str.strptime(pl.Date, "%Y-%m-%d") == target_date
        )

        if tvl_filtered.height == 0:
            self.logger.warning(f"No TVL data found for {target_date}")
            return pl.DataFrame(schema=DAILY_METRICS_SCHEMA)

        # Register filtered data
        self.conn.register("tvl_filtered", tvl_filtered)

        # create partition
        return self.create_daily_metrics_view_sql(tvl_filtered)

import polars as pl
import duckdb
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
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
                , '2022-01-01'::DATE as valid_from 
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

    def create_historical_facts_sql(
        self,
        tvl_df: pl.DataFrame,
        scd2_df: pl.DataFrame,
        target_date: Optional[date] = None,
    ) -> pl.DataFrame:
        """Create historical facts table - full historical or incremental for specific date"""
        if target_date:
            self.logger.info(
                "Creating historical facts for specific date: {target_date}"
            )
        else:
            self.logger.info(
                "Creating historical facts table with full historical data"
            )

        # Register TVL data and SCD2 dimensions
        self.conn.register("tvl_data", tvl_df)
        self.conn.register("existing_scd2", scd2_df)

        # SQL for historical data (full or incremental)
        if target_date:
            # Incremental: only data for specific date
            date_filter = "WHERE timestamp::DATE = $target_date"
            params = {"target_date": target_date}
        else:
            # Full historical: all data
            date_filter = ""
            params = {}

        # SQL for as-of join
        sql = f"""
            WITH tvl_with_date AS (
                SELECT
                    pool_id
                    , tvl_usd 
                    , apy
                    , apy_base
                    , apy_reward
                    , timestamp::DATE as date
                FROM tvl_data
                {date_filter}
            )
            SELECT
                t.date as timestamp 
                , SPLIT_PART(d.pool_old, '-', 1) as pool_old_clean
                , t.pool_id 
                , d.protocol_slug 
                , d.chain
                , d.symbol
                , t.tvl_usd
                , t.apy 
                , t.apy_base
                , t.apy_reward
                , d.valid_from
                , d.valid_to
                , d.is_current
                , d.attrib_hash
                , d.is_active
            FROM tvl_with_date t
            JOIN existing_scd2 d ON (
                t.pool_id = d.pool_id AND 
                t.date >= d.valid_from AND 
                t.date < d.valid_to
            )
            ORDER BY t.date, t.pool_id
        """

        if params:
            return self.conn.execute(sql, params).pl()
        else:
            return self.conn.execute(sql).pl()

    def get_partition_dates(self, days_back: int = 7) -> List[date]:
        """Get date range for partition operations"""
        today = date.today()
        return [today - timedelta(days=i) for i in range(days_back)]

    def create_historical_facts_partitions(
        self, tvl_df: pl.DataFrame, scd2_df: pl.DataFrame, days_back: int = 7
    ) -> List[pl.DataFrame]:
        """Create historical facts partition for last N days"""
        self.logger.info(
            f"Creating historical facts partition for last {days_back} days"
        )

        # get partition dates
        partition_dates = self.get_partition_dates(days_back)

        partitions = []
        for start_date, end_date in partition_dates:
            # Filter TVL data for this date range
            filtered_tvl = tvl_df.filter(
                (pl.col("timestamp").cast(pl.Date) >= start_date)
                & (pl.col("timestamp").cast(pl.Date) <= end_date)
            )

            if filtered_tvl.height > 0:
                # create historical facts for this partition
                historical_facts = self.create_historical_facts_sql(
                    filtered_tvl, scd2_df
                )
                partitions.append(historical_facts)

        return partitions

    def upsert_historical_facts_for_data(
        self, tvl_df: pl.DataFrame, scd2_df: pl.DataFrame, target_date: date
    ) -> pl.DataFrame:
        """Upsert historical facts for a specific date (idempotent)"""
        self.logger.info(f"Upserting historical facts for {target_date}")

        # Register TVL data and SCD2 dimensions
        self.conn.register("tvl_data", tvl_df)
        self.conn.register("existing_scd2", scd2_df)

        # SQL for incremental historical facts (just return the data for the target date)
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
                WHERE timestamp::DATE = $target_date
            )
            SELECT
                t.date as timestamp
                , SPLIT_PART(d.pool_old, '-', 1) as pool_old_clean
                , t.pool_id
                , d.protocol_slug
                , d.chain
                , d.symbol
                , t.tvl_usd
                , t.apy
                , t.apy_base
                , t.apy_reward
                , d.valid_from
                , d.valid_to
                , d.is_current
                , d.attrib_hash
                , d.is_active
            FROM tvl_with_date t
            JOIN existing_scd2 d ON (
                t.pool_id = d.pool_id AND 
                t.date >= d.valid_from AND 
                t.date < d.valid_to
            )
            ORDER BY t.date, t.pool_id
        """

        return self.conn.execute(sql, {"target_date": target_date}).pl()

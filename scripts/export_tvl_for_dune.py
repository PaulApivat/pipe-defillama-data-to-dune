"""
Export TVL data to CSV format optimized for Dune upload.
Converts data types and formats timestamps for Dune compatability. 
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import json


def export_tvl_for_dune():
    """Export TVL data to CSV for Dune upload"""
    print("🔄 Exporting TVL data for Dune Analytics...")

    conn = duckdb.connect(":memory:")

    try:
        # load TVL data
        tvl_query = """
            SELECT
                timestamp
                , tvlUsd
                , apy 
                , apyBase
                , apyReward
                , pool_id
            FROM read_parquet('output/tvl_data.parquet')
        """

        df = conn.execute(tvl_query).pl()
        print(f"✅ Loaded {df.shape[0]:,} TVL records")

        # data type conversions for Dune
        print("🔄 Converting data types for Dune compatibility...")

        # convert timestamps to Dune-comptable format
        df = df.with_columns(
            [
                pl.col("timestamp")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.3fZ")
                .dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # ✅ ISO 8601 with timezone
            ]
        )

        # convert numeric fields to INT256 (scale by 1e6 for precision)
        # This preserves decimal precision while avoiding integer overflow
        df = df.with_columns(
            [
                (pl.col("tvlUsd") * 1e6).cast(pl.Int64).alias("tvlUsd_scaled"),
                (pl.col("apy") * 1e6).cast(pl.Int64).alias("apy_scaled"),
                (pl.col("apyBase") * 1e6).cast(pl.Int64).alias("apyBase_scaled"),
                (pl.col("apyReward") * 1e6).cast(pl.Int64).alias("apyReward_scaled"),
            ]
        )

        # keep original values for reference
        df = df.with_columns(
            [
                pl.col("tvlUsd").alias("tvlUsd_original"),
                pl.col("apy").alias("apy_original"),
                pl.col("apyBase").alias("apyBase_original"),
                pl.col("apyReward").alias("apyReward_original"),
            ]
        )

        # ensure pool_id is string
        df = df.with_columns([pl.col("pool_id").cast(pl.Utf8)])

        export_df = df.select(
            [
                pl.col("timestamp"),
                pl.col("pool_id"),
                pl.col("tvlUsd_scaled").alias("tvlUsd_int256"),
                pl.col("apy_scaled").alias("apy_int256"),
                pl.col("apyBase_scaled").alias("apyBase_int256"),
                pl.col("apyReward_scaled").alias("apyReward_int256"),
                pl.col("tvlUsd_original").alias("tvlUsd_float"),
                pl.col("apy_original").alias("apy_float"),
                pl.col("apyBase_original").alias("apyBase_float"),
                pl.col("apyReward_original").alias("apyReward_float"),
            ]
        )

        # save to csv
        output_path = Path("output/tvl_data_for_dune.csv")
        export_df.write_csv(output_path)

        print(f"✅ Exported TVL data to {output_path}")
        print(f"   Rows: {export_df.shape[0]:,}")
        print(f"   File size: {output_path.stat().st_size / 1024**2:.2f} MB")

        # Display sample data
        print("\n📋 Sample TVL data:")
        print(export_df.head(3))

        # Display data types
        print(f"\n📊 Data types:")
        for col, dtype in export_df.schema.items():
            print(f"   {col}: {dtype}")

        return output_path

    finally:
        conn.close()


if __name__ == "__main__":
    export_tvl_for_dune()

"""
Export current state data to CSV format optimized for Dune upload.
Converts data types, handles list columns and formats timestamps for Dune. 
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime
import json


def transform_pool_old(df):
    """Transform pool_old column to remove chain suffix"""
    return df.with_columns(
        [pl.col("pool_old").str.split("-").list.get(0).alias("pool_old_clean")]
    )


def export_current_state_for_dune():
    """Export current state data to CSV for Dune upload"""
    print("🔄 Exporting current state data for Dune ...")

    conn = duckdb.connect(":memory:")

    # Get today's date for filename
    today = date.today().strftime("%Y-%m-%d")

    try:
        # Load current state data
        current_state_query = f"""
            SELECT 
                pool,
                protocol_slug,
                chain,
                symbol,
                underlying_tokens,
                reward_tokens,
                timestamp,
                tvl_usd,
                apy,
                apy_base,
                apy_reward,
                pool_old
            FROM read_parquet('output/current_state_{today}.parquet')
        """

        df = conn.execute(current_state_query).pl()
        df = transform_pool_old(df)
        print(f"✅ Loaded {df.shape[0]:,} current state records")

        # data type conversion
        print("🔄 Converting data types for Dune compatibility...")

        # convert timestamps
        df = df.with_columns(
            [
                # Convert timestamp to Dune-compatible format (keep timezone info)
                pl.col("timestamp")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.3fZ")
                .dt.strftime("%Y-%m-%dT%H:%M:%SZ"),  # ✅ ISO 8601 with timezone
            ]
        )

        # convert list columns to JSON strings
        df = df.with_columns(
            [
                pl.col("underlying_tokens")
                .map_elements(
                    lambda x: (
                        json.dumps(x.to_list() if hasattr(x, "to_list") else x)
                        if x is not None
                        else None
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("underlying_tokens_json"),
                pl.col("reward_tokens")
                .map_elements(
                    lambda x: (
                        json.dumps(x.to_list() if hasattr(x, "to_list") else x)
                        if x is not None
                        else None
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("reward_tokens_json"),
            ]
        )

        # create final export DataFrame with all columns
        base_columns = [
            "pool",
            "protocol_slug",
            "chain",
            "symbol",
            "underlying_tokens_json",
            "reward_tokens_json",
            "timestamp",
            "tvl_usd",
            "apy",
            "apy_base",
            "apy_reward",
            "pool_old",
            "pool_old_clean",
        ]

        # save to CSV
        export_df = df.select(base_columns)
        output_path = Path(f"output/current_state_for_dune_{today}.csv")
        export_df.write_csv(output_path)

        print(f"✅ Exported current state data to {output_path}")
        print(f"   Rows: {export_df.shape[0]:,}")
        print(f"   File size: {output_path.stat().st_size / 1024**2:.2f} MB")

        # Display sample data
        print("\n📋 Sample current state data:")
        sample_cols = [
            "timestamp",
            "pool",
            "protocol_slug",
            "chain",
            "symbol",
            "tvl_usd",
            "apy",
            "apy_base",
            "apy_reward",
            "pool_old",
            "pool_old_clean",
        ]
        sample_cols = [col for col in sample_cols if col in export_df.columns]
        print(export_df.select(sample_cols).head(3))

        # Display data types
        print(f"\n📊 Data types:")
        for col, dtype in export_df.schema.items():
            print(f"   {col}: {dtype}")

        return output_path

    finally:
        conn.close()


if __name__ == "__main__":
    export_current_state_for_dune()

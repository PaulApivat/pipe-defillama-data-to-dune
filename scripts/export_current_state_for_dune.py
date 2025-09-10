"""
Export current state data to CSV format optimized for Dune upload.
Converts data types, handles list columns and formats timestamps for Dune. 
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import json


def export_current_state_for_dune():
    """Export current state data to CSV for Dune upload"""
    print("🔄 Exporting current state data for Dune ...")

    conn = duckdb.connect(":memory:")

    try:
        # Load current state data
        current_state_query = """
            SELECT 
                dt,
                pool,
                protocol_slug,
                chain,
                symbol,
                underlying_tokens,
                reward_tokens,
                timestamp,
                pool_meta,
                tvl_usd,
                apy,
                apy_base,
                apy_reward,
                il_7d,
                apy_base_7d,
                volume_usd_1d,
                volume_usd_7d,
                apy_base_inception,
                url,
                apy_pct_1d,
                apy_pct_7d,
                apy_pct_30d,
                apy_mean_30d,
                stablecoin,
                il_risk,
                exposure,
                return_value,
                count,
                apy_mean_expanding,
                apy_std_expanding,
                mu,
                sigma,
                outlier,
                project_factorized,
                chain_factorized,
                predictions,
                pool_old
            FROM read_parquet('output/current_state.parquet')
        """

        df = conn.execute(current_state_query).pl()
        print(f"✅ Loaded {df.shape[0]:,} current state records")

        # data type conversion
        print("🔄 Converting data types for Dune compatibility...")

        # convert timestamps
        df = df.with_columns(
            [
                # Convert date to timestamp with timezone for Dune compatibility
                pl.col("dt")
                .str.strptime(pl.Date, "%Y-%m-%d")
                .dt.strftime("%Y-%m-%dT00:00:00Z"),  # ✅ ISO 8601 with timezone
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
                pl.col("predictions")
                .map_elements(
                    lambda x: (
                        json.dumps(x.to_dict() if hasattr(x, "to_dict") else x)
                        if x is not None
                        else None
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("predictions_json"),
            ]
        )

        # convert numeric fields to INT256 (scale by 1e6 for precision)
        numeric_fields = [
            "tvl_usd",
            "apy",
            "apy_base",
            "apy_reward",
            "il_7d",
            "apy_base_7d",
            "volume_usd_1d",
            "volume_usd_7d",
            "apy_base_inception",
            "apy_pct_1d",
            "apy_pct_7d",
            "apy_pct_30d",
            "apy_mean_30d",
            "return_value",
            "apy_mean_expanding",
            "apy_std_expanding",
            "mu",
            "sigma",
        ]

        # create scaled and original versions for numeric fields
        numeric_expressions = []
        for field in numeric_fields:
            if field in df.columns:
                numeric_expressions.extend(
                    [
                        (pl.col(field) * 1e6).cast(pl.Int64).alias(f"{field}_scaled"),
                        pl.col(field).alias(f"{field}_original"),
                    ]
                )

        if numeric_expressions:
            df = df.with_columns(numeric_expressions)

        # convert integer fields
        int_fields = ["count", "project_factorized", "chain_factorized"]
        int_expressions = []
        for field in int_fields:
            if field in df.columns:
                int_expressions.append(pl.col(field).cast(pl.Int64))

        # convert boolean fields
        bool_fields = ["stablecoin", "outlier"]
        bool_expressions = []
        for field in bool_fields:
            if field in df.columns:
                bool_expressions.append(pl.col(field).cast(pl.Boolean))

        if bool_expressions:
            df = df.with_columns(bool_expressions)

        # create final export DataFrame with all columns
        base_columns = [
            "dt",
            "pool",
            "protocol_slug",
            "chain",
            "symbol",
            "underlying_tokens_json",
            "reward_tokens_json",
            "timestamp",
            "pool_meta",
            "url",
            "il_risk",
            "exposure",
            "pool_old",
            "stablecoin",
            "outlier",
            "count",
            "project_factorized",
            "chain_factorized",
            "predictions_json",
        ]

        # add scaled numeric columns
        all_export_columns = base_columns.copy()
        for field in numeric_fields:
            if field in df.columns:
                all_export_columns.extend([f"{field}_scaled", f"{field}_original"])

        # Filter to only existing columns and create select expressions with proper naming
        select_expressions = []
        for col in all_export_columns:
            if col in df.columns:
                # Rename scaled columns to _int256 and original columns to _float for consistency
                if col.endswith("_scaled"):
                    base_name = col.replace("_scaled", "")
                    select_expressions.append(pl.col(col).alias(f"{base_name}_int256"))
                elif col.endswith("_original"):
                    base_name = col.replace("_original", "")
                    select_expressions.append(pl.col(col).alias(f"{base_name}_float"))
                else:
                    select_expressions.append(pl.col(col))

        export_df = df.select(select_expressions)

        # save to CSV
        output_path = Path("output/current_state_for_dune.csv")
        export_df.write_csv(output_path)

        print(f"✅ Exported current state data to {output_path}")
        print(f"   Rows: {export_df.shape[0]:,}")
        print(f"   File size: {output_path.stat().st_size / 1024**2:.2f} MB")

        # Display sample data
        print("\n📋 Sample current state data:")
        sample_cols = [
            "dt",
            "pool",
            "protocol_slug",
            "chain",
            "symbol",
            "tvl_usd_float",
            "apy_float",
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

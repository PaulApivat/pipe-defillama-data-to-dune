"""
Transformation Layer Schemas

Schemas for transformed data structures used in the pipeline.
These represent the structure of data after transformation.
"""

import polars as pl

# Transformed schemas
CURRENT_STATE_SCHEMA = pl.Schema(
    [
        ("pool", pl.String()),
        ("protocol_slug", pl.String()),
        ("chain", pl.String()),
        ("symbol", pl.String()),
        ("underlying_tokens", pl.List(pl.String())),
        ("reward_tokens", pl.List(pl.String())),
        ("timestamp", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
        ("pool_old", pl.String()),
    ]
)

HISTORICAL_TVL_SCHEMA = pl.Schema(
    [
        ("timestamp", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
        ("pool_id", pl.String()),
    ]
)

# SCD2 schemas
POOL_DIM_SCD2_SCHEMA = pl.Schema(
    [
        ("pool_id", pl.String()),
        ("protocol_slug", pl.String()),
        ("chain", pl.String()),
        ("symbol", pl.String()),
        ("underlying_tokens", pl.List(pl.String())),
        ("reward_tokens", pl.List(pl.String())),
        ("timestamp", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
        ("pool_old", pl.String()),
        ("valid_from", pl.Date()),
        ("valid_to", pl.Date()),
        ("is_current", pl.Boolean()),
        ("attrib_hash", pl.String()),
        ("is_active", pl.Boolean()),
    ]
)

HISTORICAL_FACTS_SCHEMA = pl.Schema(
    [
        ("timestamp", pl.Date()),
        ("pool_old_clean", pl.String()),
        ("pool_id", pl.String()),
        ("protocol_slug", pl.String()),
        ("chain", pl.String()),
        ("symbol", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
        ("valid_from", pl.Date()),
        ("valid_to", pl.Date()),
        ("is_current", pl.Boolean()),
        ("attrib_hash", pl.String()),
        ("is_active", pl.Boolean()),
    ]
)

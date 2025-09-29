"""
Extract Layer Schemas

Raw data schemas for data coming from external APIs.
These represent the structure of data as it comes from DeFiLlama.
"""

import polars as pl

# Raw data schemas from DeFiLlama API
RAW_POOLS_SCHEMA = pl.Schema(
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

RAW_TVL_SCHEMA = pl.Schema(
    [
        ("timestamp", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
        ("pool_id", pl.String()),
    ]
)

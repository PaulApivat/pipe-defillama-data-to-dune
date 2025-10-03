"""
Transformation Layer Schemas

Simplified schemas for transformed data structures.
Since DeFiLlama dimensions are not slowly changing, we use simple schemas.
"""

import polars as pl

# Simplified schemas (no SCD2 complexity needed)
POOL_DIM_SCHEMA = pl.Schema(
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
    ]
)

HISTORICAL_FACTS_SCHEMA = pl.Schema(
    [
        ("timestamp", pl.Date()),
        ("pool_id", pl.Binary()),
        ("pool_id_defillama", pl.String()),
        ("protocol_slug", pl.String()),
        ("chain", pl.String()),
        ("symbol", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
    ]
)

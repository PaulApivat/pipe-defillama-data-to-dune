from datetime import datetime, date
from typing import List, Optional, Union, Dict, Any
import json
from pydantic import BaseModel, Field, validator


# =============================================================================
# Enhanced Metadata Schemas (Dimension Table + Current State)
# =============================================================================


class PoolMetadata(BaseModel):
    """Enhanced schema for pool metadata + current state from DeFiLlama PoolsOld API"""

    # Static fields (existing)
    pool: str = Field(..., description="Unique pool identifier")
    protocol_slug: str = Field(
        ..., description="Protocol identifier (e.g., 'curve-dex')"
    )
    chain: str = Field(..., description="Blockchain network (e.g., 'Ethereum', 'Base')")
    symbol: str = Field(..., description="Pool symbol (e.g., 'WETH-USDC')")
    underlying_tokens: Optional[List[Optional[str]]] = Field(
        default_factory=list,
        description="List of underlying token addresses (may contain None)",
    )
    reward_tokens: Optional[List[Optional[str]]] = Field(
        default_factory=list,
        description="List of reward token addresses (may contain None)",
    )

    # New current state fields
    timestamp: str = Field(..., description="ISO 8601 timestamp of current state")
    tvl_usd: Optional[float] = Field(
        None, description="Current Total Value Locked in USD"
    )
    apy: Optional[float] = Field(None, description="Current Annual Percentage Yield")
    apy_base: Optional[float] = Field(
        None, description="Current Base APY (without rewards)"
    )
    apy_reward: Optional[float] = Field(None, description="Current Reward APY")
    pool_old: Optional[str] = Field(None, description="Legacy pool identifier")

    @validator("underlying_tokens", "reward_tokens")
    def validate_token_lists(cls, v):
        """Ensure token lists are lists, convert None to empty list, filter out None values"""
        if v is None:
            return []
        # Filter out None values from the list
        return [token for token in v if token is not None]

    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Validate ISO 8601 timestamp format"""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except ValueError:
            raise ValueError("Timestamp must be in ISO 8601 format")

    @validator("tvl_usd", "apy", "apy_base", "apy_reward")
    def validate_numeric_fields(cls, v):
        """Validate numeric fields are non-negative if present"""
        if v is not None and v < 0:
            raise ValueError("Numeric fields must be non-negative")
        return v


class MetadataResponse(BaseModel):
    """Schema for the complete metadata response"""

    pools: List[PoolMetadata] = Field(..., description="List of pool metadata")

    @validator("pools")
    def validate_unique_pools(cls, v):
        """Ensure all pools have unique pool IDs"""
        pool_ids = [pool.pool for pool in v]
        if len(pool_ids) != len(set(pool_ids)):
            raise ValueError("All pools must have unique pool IDs")
        return v


# =============================================================================
# TVL Schemas (Fact Table) - Keep existing
# =============================================================================


class TVLDataPoint(BaseModel):
    """Schema for individual TVL data point from DeFiLlama API"""

    timestamp: str = Field(..., description="ISO 8601 timestamp")
    tvlUsd: Optional[float] = Field(None, description="Total Value Locked in USD")
    apy: Optional[float] = Field(None, description="Annual Percentage Yield")
    apyBase: Optional[float] = Field(None, description="Base APY (without rewards)")
    apyReward: Optional[float] = Field(None, description="Reward APY")
    pool_id: str = Field(..., description="Pool identifier (added during processing)")

    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Validate ISO 8601 timestamp format"""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except ValueError:
            raise ValueError("Timestamp must be in ISO 8601 format")

    @validator("tvlUsd", "apy", "apyBase", "apyReward")
    def validate_numeric_fields(cls, v):
        """Validate numeric fields are non-negative if present"""
        if v is not None and v < 0:
            raise ValueError("Numeric fields must be non-negative")
        return v


class TVLResponse(BaseModel):
    """Schema for the complete TVL response from DeFiLlama API"""

    status: str = Field(..., description="API response status")
    data: List[TVLDataPoint] = Field(..., description="List of TVL data points")

    @validator("status")
    def validate_status(cls, v):
        """Ensure status is 'success'"""
        if v != "success":
            raise ValueError('Status must be "success"')
        return v

    @validator("data")
    def validate_data_not_empty(cls, v):
        """Ensure data list is not empty"""
        if not v:
            raise ValueError("Data list cannot be empty")
        return v


# =============================================================================
# Polars Schema Equivalents (for compatibility)
# =============================================================================

import polars as pl

METADATA_SCHEMA = pl.Schema(
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

TVL_SCHEMA = pl.Schema(
    [
        ("timestamp", pl.String()),
        ("tvlUsd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apyBase", pl.Float64()),
        ("apyReward", pl.Float64()),
        ("pool_id", pl.String()),
    ]
)


# =============================================================================
# Protocol Schemas (for /protocols endpoint)
# =============================================================================


class ProtocolData(BaseModel):
    """Schema for individual protocol data from Defillama"""

    id: str = Field(..., description="Protocol unique identifier")
    name: str = Field(..., description="Protocol name")
    address: str = Field(..., description="Protocol token contract address")
    symbol: str = Field(..., description="Protocol symbol")
    category: Optional[str] = Field(None, description="Protocol category")
    chains: Optional[List[str]] = Field(
        default_factory=list, description="Supported chains"
    )
    slug: str = Field(..., description="Protocol slug")
    tvl: Optional[float] = Field(None, description="Total Value Locked in USD")
    chainTvls: Optional[Dict[str, float]] = Field(
        default_factory=dict, description="TVL breakdown by chain"
    )

    @validator("tvl")
    def validate_tvl(cls, v):
        """Validate TVL is non-negative if present"""
        if v is not None and v < 0:
            raise ValueError("TVL must be non-negative")
        return v

    @validator("chains")
    def validate_chains(cls, v):
        """Ensure chains is a list of strings if present"""
        if v is not None and not isinstance(v, list):
            raise ValueError("Chains must be a list")
        return v or []


# Polars Schema for Protocols
PROTOCOL_SCHEMA = pl.Schema(
    [
        ("id", pl.String()),
        ("name", pl.String()),
        ("address", pl.String()),
        ("symbol", pl.String()),
        ("category", pl.String()),
        ("chains", pl.String()),
        ("slug", pl.String()),
        ("tvl", pl.Float64()),
        ("chainTvls", pl.String()),  # JSON string for complex nested data
    ]
)


# =============================================================================
# Utility Functions
# =============================================================================


def validate_metadata_response(data: dict) -> MetadataResponse:
    """Validate and parse metadata response"""
    return MetadataResponse(**data)


def validate_tvl_response(data: dict) -> TVLResponse:
    """Validate and parse TVL response"""
    return TVLResponse(**data)


def metadata_to_polars(metadata: MetadataResponse) -> pl.DataFrame:
    """Convert validated metadata to Polars DataFrame"""
    records = [pool.dict() for pool in metadata.pools]
    return pl.DataFrame(records, schema=METADATA_SCHEMA)


def tvl_to_polars(tvl: TVLResponse) -> pl.DataFrame:
    """Convert validated TVL data to Polars DataFrame"""
    records = [point.dict() for point in tvl.data]
    return pl.DataFrame(records, schema=TVL_SCHEMA)


def protocols_to_polars(protocols: List[ProtocolData]) -> pl.DataFrame:
    """Convert validated protocols data to Polars DataFrame"""
    records = []
    for protocol in protocols:
        record = protocol.dict()
        record["chains"] = json.dumps(record["chains"]) if record["chains"] else None
        record["chainTvls"] = (
            json.dumps(record["chainTvls"]) if record["chainTvls"] else None
        )
        records.append(record)

    return pl.DataFrame(records, schema=PROTOCOL_SCHEMA)

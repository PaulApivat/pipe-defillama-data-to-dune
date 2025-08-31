from datetime import datetime, date
from typing import List, Optional, Union, Dict, Any
from pydantic import BaseModel, Field, validator


# =============================================================================
# Enhanced Metadata Schemas (Dimension Table + Current State)
# =============================================================================


class PoolMetadata(BaseModel):
    """Enhanced schema for pool metadata + current state from DeFiLlama PoolsOld API"""

    # Static fields (existing)
    dt: Union[str, date] = Field(
        ..., description="Date in YYYY-MM-DD format or datetime.date object"
    )
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
    pool_meta: Optional[str] = Field(None, description="Pool metadata/description")
    tvl_usd: Optional[float] = Field(
        None, description="Current Total Value Locked in USD"
    )
    apy: Optional[float] = Field(None, description="Current Annual Percentage Yield")
    apy_base: Optional[float] = Field(
        None, description="Current Base APY (without rewards)"
    )
    apy_reward: Optional[float] = Field(None, description="Current Reward APY")
    il_7d: Optional[float] = Field(None, description="Impermanent Loss over 7 days")
    apy_base_7d: Optional[float] = Field(None, description="Base APY over 7 days")
    volume_usd_1d: Optional[float] = Field(
        None, description="24-hour trading volume in USD"
    )
    volume_usd_7d: Optional[float] = Field(
        None, description="7-day trading volume in USD"
    )
    apy_base_inception: Optional[float] = Field(
        None, description="Base APY since inception"
    )
    url: Optional[str] = Field(None, description="Pool URL")

    # New analytical fields
    apy_pct_1d: Optional[float] = Field(
        None, description="APY percentage change over 1 day"
    )
    apy_pct_7d: Optional[float] = Field(
        None, description="APY percentage change over 7 days"
    )
    apy_pct_30d: Optional[float] = Field(
        None, description="APY percentage change over 30 days"
    )
    apy_mean_30d: Optional[float] = Field(None, description="Mean APY over 30 days")
    stablecoin: bool = Field(..., description="Whether pool contains stablecoins")
    il_risk: str = Field(..., description="Impermanent loss risk level")
    exposure: str = Field(..., description="Exposure type (single/multi)")
    return_value: Optional[float] = Field(
        None, alias="return", description="Return value"
    )
    count: Optional[int] = Field(None, description="Data point count")
    apy_mean_expanding: Optional[float] = Field(None, description="Expanding mean APY")
    apy_std_expanding: Optional[float] = Field(
        None, description="Expanding standard deviation APY"
    )
    mu: Optional[float] = Field(None, description="Mean value")
    sigma: Optional[float] = Field(None, description="Standard deviation")
    outlier: Optional[bool] = Field(None, description="Whether pool is an outlier")
    project_factorized: Optional[int] = Field(
        None, description="Project factorized value"
    )
    chain_factorized: Optional[int] = Field(None, description="Chain factorized value")
    predictions: Optional[str] = Field(
        None, description="ML predictions as JSON string"
    )
    pool_old: Optional[str] = Field(None, description="Legacy pool identifier")

    @validator("dt")
    def validate_date_format(cls, v):
        """Validate date is either string or datetime.date object"""
        if isinstance(v, date):
            return v.strftime("%Y-%m-%d")  # Convert to string
        elif isinstance(v, str):
            try:
                datetime.strptime(v, "%Y-%m-%d")
                return v
            except ValueError:
                raise ValueError("Date string must be in YYYY-MM-DD format")
        else:
            raise ValueError("Date must be string or datetime.date object")

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

    @validator(
        "tvl_usd", "apy", "apy_base", "apy_reward", "volume_usd_1d", "volume_usd_7d"
    )
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
        ("dt", pl.String()),
        ("pool", pl.String()),
        ("protocol_slug", pl.String()),
        ("chain", pl.String()),
        ("symbol", pl.String()),
        ("underlying_tokens", pl.List(pl.String())),
        ("reward_tokens", pl.List(pl.String())),
        ("timestamp", pl.String()),
        ("pool_meta", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
        ("il_7d", pl.Float64()),
        ("apy_base_7d", pl.Float64()),
        ("volume_usd_1d", pl.Float64()),
        ("volume_usd_7d", pl.Float64()),
        ("apy_base_inception", pl.Float64()),
        ("url", pl.String()),
        ("apy_pct_1d", pl.Float64()),
        ("apy_pct_7d", pl.Float64()),
        ("apy_pct_30d", pl.Float64()),
        ("apy_mean_30d", pl.Float64()),
        ("stablecoin", pl.Boolean()),
        ("il_risk", pl.String()),
        ("exposure", pl.String()),
        ("return_value", pl.Float64()),
        ("count", pl.Int64()),
        ("apy_mean_expanding", pl.Float64()),
        ("apy_std_expanding", pl.Float64()),
        ("mu", pl.Float64()),
        ("sigma", pl.Float64()),
        ("outlier", pl.Boolean()),
        ("project_factorized", pl.Int64()),
        ("chain_factorized", pl.Int64()),
        ("predictions", pl.String()),
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

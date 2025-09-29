"""
Local Storage - Load Layer

Pure functions for local file storage operations.
Handles Parquet, JSON, and other file formats.
"""

import polars as pl
import json
import os
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


def save_parquet(df: pl.DataFrame, filepath: str) -> str:
    """
    Save DataFrame to Parquet file

    Args:
        df: DataFrame to save
        filepath: Path to save file

    Returns:
        str: Path to saved file
    """
    logger.info(f"Saving DataFrame to Parquet: {filepath}")

    # Ensure directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Save to Parquet
    df.write_parquet(filepath)

    logger.info(f"Saved {df.height} records to {filepath}")
    return filepath


def save_json(df: pl.DataFrame, filepath: str) -> str:
    """
    Save DataFrame to JSON file

    Args:
        df: DataFrame to save
        filepath: Path to save file

    Returns:
        str: Path to saved file
    """
    logger.info(f"Saving DataFrame to JSON: {filepath}")

    # Ensure directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Convert to JSON
    data = df.to_dicts()

    # Save to JSON
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)

    logger.info(f"Saved {len(data)} records to {filepath}")
    return filepath


def load_parquet(filepath: str) -> pl.DataFrame:
    """
    Load DataFrame from Parquet file

    Args:
        filepath: Path to Parquet file

    Returns:
        pl.DataFrame: Loaded DataFrame
    """
    logger.info(f"Loading DataFrame from Parquet: {filepath}")

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Parquet file not found: {filepath}")

    df = pl.read_parquet(filepath)

    logger.info(f"Loaded {df.height} records from {filepath}")
    return df


def load_json(filepath: str) -> pl.DataFrame:
    """
    Load DataFrame from JSON file

    Args:
        filepath: Path to JSON file

    Returns:
        pl.DataFrame: Loaded DataFrame
    """
    logger.info(f"Loading DataFrame from JSON: {filepath}")

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"JSON file not found: {filepath}")

    with open(filepath, "r") as f:
        data = json.load(f)

    df = pl.DataFrame(data, strict=False, infer_schema_length=10000)

    logger.info(f"Loaded {df.height} records from {filepath}")
    return df


def save_current_state_data(
    df: pl.DataFrame, output_dir: str = "output"
) -> Dict[str, str]:
    """
    Save current state data to files

    Args:
        df: Current state DataFrame
        output_dir: Output directory

    Returns:
        Dict: Paths to saved files
    """
    today = date.today().strftime("%Y-%m-%d")

    parquet_path = f"{output_dir}/current_state_{today}.parquet"
    json_path = f"{output_dir}/current_state_{today}.json"

    return {"parquet": save_parquet(df, parquet_path), "json": save_json(df, json_path)}


def save_tvl_data(df: pl.DataFrame, output_dir: str = "output") -> Dict[str, str]:
    """
    Save TVL data to files

    Args:
        df: TVL DataFrame
        output_dir: Output directory

    Returns:
        Dict: Paths to saved files
    """
    today = date.today().strftime("%Y-%m-%d")

    parquet_path = f"{output_dir}/tvl_data_{today}.parquet"
    json_path = f"{output_dir}/tvl_data_{today}.json"

    return {"parquet": save_parquet(df, parquet_path), "json": save_json(df, json_path)}


def save_scd2_data(df: pl.DataFrame, output_dir: str = "output") -> str:
    """
    Save SCD2 data to file

    Args:
        df: SCD2 DataFrame
        output_dir: Output directory

    Returns:
        str: Path to saved file
    """
    parquet_path = f"{output_dir}/pool_dim_scd2.parquet"
    return save_parquet(df, parquet_path)


def save_historical_facts_data(df: pl.DataFrame, output_dir: str = "output") -> str:
    """
    Save historical facts data to file

    Args:
        df: Historical facts DataFrame
        output_dir: Output directory

    Returns:
        str: Path to saved file
    """
    today = date.today().strftime("%Y-%m-%d")
    parquet_path = f"{output_dir}/historical_facts_{today}.parquet"
    return save_parquet(df, parquet_path)


def file_exists(filepath: str) -> bool:
    """
    Check if file exists

    Args:
        filepath: Path to file

    Returns:
        bool: True if file exists
    """
    return os.path.exists(filepath)


def get_latest_file(pattern: str, directory: str = "output") -> Optional[str]:
    """
    Get the latest file matching a pattern

    Args:
        pattern: File pattern to match
        directory: Directory to search

    Returns:
        Optional[str]: Path to latest file or None
    """
    import glob

    search_pattern = os.path.join(directory, pattern)
    files = glob.glob(search_pattern)

    if not files:
        return None

    # Return the most recently modified file
    return max(files, key=os.path.getmtime)

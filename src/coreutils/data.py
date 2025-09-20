from pathlib import Path
from typing import Optional, Union, Dict, Any
from src.datasources.defillama.yieldpools.schemas import HISTORICAL_TVL_SCHEMA

import polars as pl
import json


class DataConverter:
    """Utility class for data format conversions and validation"""

    @staticmethod
    def json_to_parquet(
        json_path: Union[str, Path],
        parquet_path: Union[str, Path],
        schema: Optional[pl.Schema] = None,
    ) -> None:
        """Convert JSON file to Parquet format with optional schema validation

        Args:
            json_path: Path to input JSON file
            parquet_path: Path to output Parquet file
            schema: Optional Polars schema for validation
        """
        json_path = Path(json_path)
        parquet_path = Path(parquet_path)

        if not json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {json_path}")

        # Reas JSON data
        with open(json_path, "r") as f:
            data = json.load(f)

        # Convert to Polars DataFrame with TVL-specific handling
        if "tvl" in str(json_path).lower() and schema:
            # special handling for TVL data
            if isinstance(data, list):
                # check if list of list (nested structure)
                if data and isinstance(data[0], list):
                    # If nested, then flatten
                    flattened_data = []
                    for pool_data in data:
                        if isinstance(pool_data, list):
                            flattened_data.extend(pool_data)
                    df = pl.DataFrame(flattened_data, schema=schema)
                else:
                    df = pl.DataFrame(data, schema=schema)
            else:
                df = pl.DataFrame(data, schema=schema)
        elif schema:
            df = pl.DataFrame(data, schema=schema)
        else:
            df = pl.DataFrame(data)

        # Ensure output directory exists
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to Parquet
        df.write_parquet(parquet_path)
        print(f"✅ Converted {json_path} → {parquet_path}")
        print(f"   Rows: {df.shape[0]}, Columns: {df.shape[1]}")

    @staticmethod
    def parquet_to_json(
        parquet_path: Union[str, Path], json_path: Union[str, Path]
    ) -> None:
        """Convert Parquet file to JSON format

        Args:
            parquet_path: Path to input Parquet file
            json_path: Path to output JSON file
        """
        parquet_path = Path(parquet_path)
        json_path = Path(json_path)

        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        # Read Parquet file
        df = pl.read_parquet(parquet_path)

        # Ensure output directory exists
        json_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to JSON
        df.write_json(json_path)
        print(f"✅ Converted {parquet_path} → {json_path}")
        print(f"   Rows: {df.shape[0]}, Columns: {df.shape[1]}")

    @staticmethod
    def validate_parquet_schema(
        file_path: Union[str, Path], expected_schema: pl.Schema
    ) -> bool:
        """Validate that Parquet file matches expected schema

        Args:
            file_path: Path to Parquet file
            expected_schema: expected Polars schema

        Returns:
            True if schema matches, False otherwise
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            df = pl.read_parquet(file_path)
            actual_schema = df.schema

            # Compare schemas
            if actual_schema == expected_schema:
                print(f"✅ Schema validation passed for {file_path}")
                return True
            else:
                print(f"❌ Schema validation failed for {file_path}")
                print(f"   Expected schema: {expected_schema}")
                print(f"   Actual schema: {actual_schema}")
                return False

        except Exception as e:
            print(f"❌ Error validating schema for {file_path}: {e}")
            return False

    @staticmethod
    def get_file_info(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Get information about a data file

        Args:
            file_path: Path to data file

        Returns:
            Dictionary with file information
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Get file size
        file_size = file_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        # Read data based on file extension
        if file_path.suffix == ".parquet":
            df = pl.read_parquet(file_path)
        elif file_path.suffix == ".json":
            with open(file_path, "r") as f:
                data = json.load(f)

            print(f"Data type: {type(data)}")
            if isinstance(data, dict):
                print(f"Keys: {list(data.keys())[:5]}")
                print(f"First value type: {type(list(data.values())[0])}")

            # special handling for TVL data
            if "tvl" in str(file_path).lower():
                # TVL data is a list of records, not nested objects
                if isinstance(data, list):
                    # check if its a list of list (nested structure)
                    if data and isinstance(data[0], list):
                        # If nested, then flatten
                        flattened_data = []
                        for pool_data in data:
                            if isinstance(pool_data, list):
                                flattened_data.extend(pool_data)
                        df = pl.DataFrame(flattened_data, schema=HISTORICAL_TVL_SCHEMA)
                    else:
                        # already a flat list of records
                        df = pl.DataFrame(data, schema=HISTORICAL_TVL_SCHEMA)
            else:
                df = pl.DataFrame(data, strict=False)

        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

        return {
            "file_path": str(file_path),
            "file_size_mb": round(file_size_mb, 2),
            "rows": df.shape[0],
            "columns": df.shape[1],
            "schema": df.schema,
            "column_names": df.columns,
        }

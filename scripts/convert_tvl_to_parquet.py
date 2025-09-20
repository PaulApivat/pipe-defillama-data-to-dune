"""
Convert TVL data from JSON to Parquet format.
This script uses the DataConverter class from coreutils/data.py to convert tvl_data.json to parquet.
"""

from pathlib import Path
from src.coreutils.data import DataConverter
from src.datasources.defillama.yieldpools.schemas import HISTORICAL_TVL_SCHEMA


def main():
    """Convert TVL data from JSON to Parquet format"""
    print("Converting TVL data from JSON to Parquet format...")

    # Define file paths
    json_path = Path("output/tvl_data.json")
    parquet_path = Path("output/tvl_data.parquet")

    # Check if input file exists
    if not json_path.exists():
        print(f"❌ Input file not found: {json_path}")
        print(" Please run fetch_tvl.py first to generate the data")
        return

    try:
        # Get file information before conversion
        print(f"\n📊 Input file information:")
        json_info = DataConverter.get_file_info(json_path)
        print(f"  File: {json_info['file_path']}")
        print(f"  Size: {json_info['file_size_mb']:.2f} MB")
        print(f"  Rows: {json_info['rows']}")
        print(f"  Columns: {json_info['columns']}")

        # Convert JSON To Parquet with schema validation
        print(f"\n🔄 Converting to Parquet format...")
        DataConverter.json_to_parquet(
            json_path=json_path, parquet_path=parquet_path, schema=HISTORICAL_TVL_SCHEMA
        )

        # Get file information after conversion
        print(f"\n📊 Output file information:")
        parquet_info = DataConverter.get_file_info(parquet_path)
        print(f"  File: {parquet_info['file_path']}")
        print(f"  Size: {parquet_info['file_size_mb']:.2f} MB")
        print(f"  Rows: {parquet_info['rows']}")
        print(f"  Columns: {parquet_info['columns']}")

        # Calculate compression ratio / size reduction
        compression_ratio = json_info["file_size_mb"] / parquet_info["file_size_mb"]
        print(f"\n Compression ratio: {compression_ratio:.1f}x smaller")

        # Validate schema
        print(f"\n Validating schema...")
        schema_valid = DataConverter.validate_parquet_schema(
            file_path=parquet_path, expected_schema=HISTORICAL_TVL_SCHEMA
        )

        if schema_valid:
            print(f"✅ Conversion completed and validation passed")
            print(f"✅ JSON: {json_path} ({json_info['file_size_mb']} MB)")
            print(f"✅ Parquet: {parquet_path} ({parquet_info['file_size_mb']} MB)")
        else:
            print(f"\n❌ Schema validation failed")

    except Exception as e:
        print(f"\n❌ Conversion failed: {e}")
        raise


if __name__ == "__main__":
    main()

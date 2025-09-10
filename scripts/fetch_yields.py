import polars as pl
from src.datasources.defillama.yieldpools.metadata import YieldPoolsMetadata
from src.datasources.defillama.yieldpools.schemas import (
    validate_metadata_response,
    metadata_to_polars,
)

TARGET_PROJECTS = {
    "curve-dex",
    "pancakeswap-amm",
    "pancakeswap-amm-v3",
    "aerodrome-slipstream",
    "aerodrome-v1",
}


def main():
    # fetch metadata for all pools
    metadata = YieldPoolsMetadata.fetch()

    # Validate the metadata using our schema before processing
    try:
        # Convert the DataFrame back to the format expected by our schema
        # This validates that the data structure matches our expectations
        metadata_dict = {
            "pools": [
                {
                    "dt": row["dt"],
                    "pool": row["pool"],
                    "protocol_slug": row["protocol_slug"],
                    "chain": row["chain"],
                    "symbol": row["symbol"],
                    "underlying_tokens": row["underlying_tokens"],
                    "reward_tokens": row["reward_tokens"],
                }
                for row in metadata.df.iter_rows(named=True)
            ]
        }

        # Validate using our schema
        validated_metadata = validate_metadata_response(metadata_dict)
        print(f"✅ Schema validation passed for {len(validated_metadata.pools)} pools")

    except Exception as validation_error:
        print(f"❌ Schema validation failed: {validation_error}")
        print("Continuing with unvalidated data...")

    # filter for target projects
    df = metadata.df.filter(pl.col("protocol_slug").is_in(TARGET_PROJECTS))

    # select only relevant columns
    df = df.select(
        [
            "dt",
            "pool",  # pool_id
            "protocol_slug",  # Project
            "chain",  # Chain
            "symbol",  # Pool symbol
            "underlying_tokens",  # Token addresses
            "reward_tokens",
        ]
    )

    # save to JSON for inspection
    df.write_json("output/metadata.json")

    print(f"Saved {df.shape[0]} rows to output/metadata.json")
    print(
        metadata.df.select("protocol_slug")
        .filter(pl.col("protocol_slug").str.contains("curve|pancakeswap|aerodrome"))
        .unique()
    )


if __name__ == "__main__":
    main()

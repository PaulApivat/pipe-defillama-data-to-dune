import polars as pl
from src.datasources.defillama.yieldpools.metadata import YieldPoolsMetadata

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

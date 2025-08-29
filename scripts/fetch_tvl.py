import polars as pl
import requests
import time
from pathlib import Path
import json
from typing import List, Tuple, Set, Any


def load_metadata():
    """Load the metadata from the output/metadata.json file"""
    metadata_path = Path("output/metadata.json")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with open(metadata_path, "r") as f:
        data = json.load(f)

    return pl.DataFrame(data)


def load_existing_tvl(output_dir: Path) -> Tuple[List[Any], Set[str]]:
    """Load existing tvl_data.json if present and return (data, pool_id_set)."""
    tvl_output_path = output_dir / "tvl_data.json"
    if not tvl_output_path.exists():
        return [], set()

    with open(tvl_output_path, "r") as f:
        try:
            existing = json.load(f)
        except json.JSONDecodeError:
            # Corrupt or partial file; treat as empty to avoid blocking progress
            existing = []

    existing_pool_ids: Set[str] = set()
    for pool_blob in existing:
        if isinstance(pool_blob, list) and len(pool_blob) > 0:
            pid = pool_blob[0].get("pool_id")
            if pid:
                existing_pool_ids.add(pid)
        elif isinstance(pool_blob, dict):
            pid = pool_blob.get("pool_id")
            if pid:
                existing_pool_ids.add(pid)

    return existing, existing_pool_ids


def fetch_pool_tvl(pool_id: str, max_retries: int = 3) -> dict:
    """Fetch TVL data for a specific pool from the DeFiLlama API

    Returns a list of TVL data points with the following structure:
    [
        {
            "timestamp": "2024-01-01T00:00:00.000Z",
            "tvlUsd": 1500000000,
            "apy": 5.2,
            "apyBase": 3.1,
            "apyReward": 2.1,
            "pool_id": "747c1d2a-c668-4682-b9f9-296708a3dd90"
        }
    ]

    This creates a fact table that can be joined with the metadata dimension table.
    """
    url = f"https://yields.llama.fi/chart/{pool_id}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            response_data = response.json()

            # Handle the API response structure
            if response_data.get("status") == "success" and "data" in response_data:
                tvl_data = response_data["data"]

                # Add pool_id to each data point
                for item in tvl_data:
                    item["pool_id"] = pool_id

                return tvl_data
            else:
                print(f"Unexpected API response structure for pool {pool_id}")
                return {
                    "pool_id": pool_id,
                    "error": "Unexpected API response structure",
                }

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(
                    f"Failed to fetch data for pool {pool_id} after {max_retries} attempts: {e}"
                )
                return {"pool_id": pool_id, "error": str(e)}
            else:
                print(f"Attempt {attempt + 1} failed for pool {pool_id}, retrying...")
                time.sleep(2**attempt)  # Exponential backoff

        except Exception as e:
            print(f"Unexpected error fetching data for pool {pool_id}: {e}")
            return {"pool_id": pool_id, "error": str(e)}


def main():
    """Main function to fetch TVL data for all pools"""
    print("Loading metadata...")
    metadata_df = load_metadata()

    print(f"Found {metadata_df.shape[0]} pools to fetch TVL data for")

    # Create output directory if it doesn't exist
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # Load existing results (resume capability)
    all_tvl_data, existing_pool_ids = load_existing_tvl(output_dir)
    if existing_pool_ids:
        print(
            f"Resume mode: found {len(existing_pool_ids)} pools already fetched. Skipping them."
        )

    successful_fetches = 0
    failed_fetches = 0
    newly_fetched_pool_ids: Set[str] = set()

    for idx, row in enumerate(metadata_df.iter_rows(named=True)):
        pool_id = row["pool"]
        protocol = row["protocol_slug"]
        chain = row["chain"]
        symbol = row["symbol"]

        if pool_id in existing_pool_ids:
            print(
                f"[{idx + 1}/{metadata_df.shape[0]}] Skipping already fetched {protocol} - {symbol} ({chain}) - {pool_id}"
            )
            continue

        print(
            f"[{idx + 1}/{metadata_df.shape[0]}] Fetching TVL for {protocol} - {symbol} ({chain}) - {pool_id}"
        )

        tvl_data = fetch_pool_tvl(pool_id)

        if "error" not in tvl_data:
            successful_fetches += 1
            newly_fetched_pool_ids.add(pool_id)
            all_tvl_data.append(tvl_data)
        else:
            failed_fetches += 1

        # Add a small delay to be respectful to the API
        time.sleep(0.1)

    print(f"\nFetching complete!")
    print(f"Successful (new this run): {successful_fetches}")
    print(f"Failed (this run): {failed_fetches}")

    # Save all TVL data to a single file (existing + new)
    tvl_output_path = output_dir / "tvl_data.json"
    with open(tvl_output_path, "w") as f:
        json.dump(all_tvl_data, f, indent=2)

    print(f"Saved TVL data to {tvl_output_path}")

    # Also save individual pool files for newly fetched pools only
    individual_dir = output_dir / "tvl_individual"
    individual_dir.mkdir(exist_ok=True)

    for pool_data in all_tvl_data:
        # Only write files for pools fetched in this run
        target_pool_id = None
        if isinstance(pool_data, list) and len(pool_data) > 0:
            target_pool_id = pool_data[0].get("pool_id")
        elif isinstance(pool_data, dict):
            target_pool_id = pool_data.get("pool_id")

        if not target_pool_id or target_pool_id not in newly_fetched_pool_ids:
            continue

        individual_path = individual_dir / f"{target_pool_id}.json"
        with open(individual_path, "w") as f:
            json.dump(pool_data, f, indent=2)

    if newly_fetched_pool_ids:
        print(
            f"Saved individual pool files to {individual_dir} for {len(newly_fetched_pool_ids)} new pools"
        )
    else:
        print("No new pools fetched; individual files not updated.")


if __name__ == "__main__":
    main()

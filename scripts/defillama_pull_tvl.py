import requests
import pandas as pd
from datetime import datetime
import time
import os
from dotenv import load_dotenv
from dune_client.client import DuneClient


# Endpoint for protocol details
PROTOCOL_DETAILS_ENDPOINT = "https://api.llama.fi/protocol/{slug}"

# TVL Cutoff Date (human readable format)
TVL_CUTOFF_DATE = "2020-01-01"

# Function to initialize Dune client from environment variables
def get_dune_client():
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path)
    return DuneClient.from_env()

# Function to delete a table from Dune
def delete_existing_table(dune, namespace, table_name):
    try:
        dune.delete_table(namespace=namespace, table_name=table_name)
        print(f"Successfully deleted the table: {table_name}")
    except Exception as e:
        print(f"Failed to delete the table: {table_name}. Error: {e}")

def table_cutoff_date():
    """Parse the human readable cutoff date for filtering TVL data."""
    return datetime.strptime(TVL_CUTOFF_DATE, "%Y-%m-%d").date()


def fetch_protocol_data(slug: str) -> dict:
    """Fetch data for a single protocol with retry logic."""
    url = PROTOCOL_DETAILS_ENDPOINT.format(slug=slug)
    attempts = 0
    max_attempts = 10

    while attempts < max_attempts:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad status
            return response.json()
        except requests.RequestException as e:
            attempts += 1
            print(f"Attempt {attempts} failed: {e}")
            if attempts == max_attempts:
                raise


def process_tvl_data(data: dict, cutoff_date: datetime) -> pd.DataFrame:
    """Extract TVL data for a protocol and convert it into a Pandas DataFrame."""
    tvl_records = []

    # Loop through chains' TVL data
    chain_tvls = data.get("chainTvls", {})
    for chain, chain_data in chain_tvls.items():
        for tvl_entry in chain_data.get("tvl", []):
            dateval = datetime.utcfromtimestamp(tvl_entry["date"]).date()
            
            if dateval < cutoff_date:
                continue  # Filter out dates older than the cutoff

            protocol_name = data.get("name", "").split()[0].lower()
            protocol_version = data.get("name", "").split()[1].lower() if len(data.get("name", "").split()) > 1 else "unknown"
            
            tvl_records.append(
                {
                    "protocol_slug": protocol_name,
                    "protocol_version": protocol_version,
                    "chain": chain,
                    "date": dateval,
                    "total_tvl_usd": float(tvl_entry.get("totalLiquidityUSD", 0)),
                }
            )

    # Convert to Pandas DataFrame
    return pd.DataFrame(tvl_records)


def save_to_csv(df: pd.DataFrame, filename: str):
    """Save Pandas DataFrame to CSV file."""
    df.to_csv(filename, index=False)
    print(f"Data successfully saved to {filename}")


if __name__ == "__main__":
    SLUGS = ['uniswap-v1', 'uniswap-v2', 'uniswap-v3']
    dune = get_dune_client()
    start_time = time.time()

    all_tvl_data = []

    for slug in SLUGS:
        # Fetch and process data
        print(f"Fetching data for protocol: {slug}")
        raw_data = fetch_protocol_data(slug)

        print("Processing TVL data...")
        cutoff = table_cutoff_date()
        tvl_df = process_tvl_data(raw_data, cutoff)

        # Append data to the list
        all_tvl_data.append(tvl_df)

    # Concatenate all dataframes into one
    combined_tvl_df = pd.concat(all_tvl_data, ignore_index=True)

    # # Save combined data to a single CSV file
    # output_file = "./uploads/uniswap_tvl_defillama.csv"
    # print(f"Saving combined data to {output_file}...")
    # save_to_csv(combined_tvl_df, output_file)

    # Convert DataFrame to CSV string
    combined_tvl_csv = combined_tvl_df.to_csv(index=False)

    # Step to delete the existing table before uploading new data
    delete_existing_table(dune, "uniswap_fnd", "dataset_uniswap_tvl_defillama")

    # Upload to Dune
    upload_success = dune.upload_csv(
        table_name="uniswap_tvl_defillama",
        data=combined_tvl_csv,
        is_private=True
    )
    print("Upload Successful:", upload_success)


    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    print(f"Process completed in {int(minutes)} minutes and {int(seconds)} seconds.")

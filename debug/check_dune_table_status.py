#!/usr/bin/env python3
"""
Check the current status of the Dune table
"""

import os
import sys
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def check_dune_table_status():
    """Check the current status of the Dune table"""

    print("🔍 Checking Dune Table Status")
    print("=" * 40)

    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        print("❌ DUNE_API_KEY not found")
        return

    base_url = "https://api.dune.com/api/v1"
    namespace = "uniswap_fnd"
    table_name = "prod_defillama_historical_facts"

    # Try to query the table for row count
    query_url = f"{base_url}/query"
    query_data = {
        "name": "Check table row count",
        "query_sql": f"SELECT COUNT(*) as row_count FROM {namespace}.{table_name} LIMIT 1",
    }

    headers = {"X-DUNE-API-KEY": api_key}

    try:
        print(f"🔍 Querying table: {namespace}.{table_name}")
        response = requests.post(query_url, json=query_data, headers=headers)

        print(f"📊 Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            row_count = result.get("result", {}).get("rows", [[0]])[0][0]
            print(f"📈 Row Count: {row_count:,}")

            if row_count > 0:
                print("✅ Table has data - this is NOT a first run")
                return False
            else:
                print("🆕 Table is empty - this IS a first run")
                return True
        else:
            print(f"❌ Query failed: {response.text}")
            return None

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


if __name__ == "__main__":
    status = check_dune_table_status()
    if status is True:
        print("\n✅ Table is empty - first run detected")
    elif status is False:
        print("\n❌ Table has data - subsequent run detected")
    else:
        print("\n❓ Could not determine table status")

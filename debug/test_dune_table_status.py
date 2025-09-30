#!/usr/bin/env python3
"""
Test what status code Dune API returns for non-existent table
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def test_table_status():
    """Test what status code we get for a non-existent table"""

    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        print("❌ DUNE_API_KEY not found")
        return

    # Test with a non-existent table name
    namespace = "uniswap_fnd"
    table_name = "non_existent_table_12345"

    url = f"https://api.dune.com/api/v1/table/{namespace}/{table_name}"
    headers = {"X-Dune-API-Key": api_key}

    print(f"🔍 Testing URL: {url}")
    print(f"🔑 Using API key: {api_key[:10]}...")

    try:
        response = requests.get(url, headers=headers)
        print(f"📊 Status Code: {response.status_code}")
        print(f"📝 Response Text: {response.text}")

        if response.status_code == 404:
            print("✅ 404 - Table doesn't exist (expected)")
        elif response.status_code == 405:
            print("⚠️  405 - Method not allowed (unexpected)")
        elif response.status_code == 200:
            print("✅ 200 - Table exists (unexpected)")
        else:
            print(f"❓ {response.status_code} - Unknown status")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    test_table_status()

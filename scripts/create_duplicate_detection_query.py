#!/usr/bin/env python3
"""
Create a duplicate detection query in Dune for checking existing data
"""

import os
import sys
import requests
from datetime import date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def create_duplicate_detection_query():
    """Create a query in Dune for duplicate detection"""

    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        print("❌ DUNE_API_KEY not found")
        return None

    # Query configuration
    query_data = {
        "name": "DeFiLlama Duplicate Detection",
        "description": "Check if data exists for a specific date to prevent duplicates",
        "query_sql": """
-- Check if data exists for a specific date
SELECT COUNT(*) as row_count 
FROM dune.uniswap_fnd.test_run_defillama_historical_facts 
WHERE timestamp = date '{{date}}'
        """.strip(),
        "parameters": [
            {
                "key": "date",
                "type": "text",
                "description": "Date to check for existing data (YYYY-MM-DD format)",
                "value": "2025-10-05",
            }
        ],
        "tags": ["duplicate-detection", "defillama", "pipeline"],
        "is_private": True,
    }

    headers = {"X-DUNE-API-KEY": api_key, "Content-Type": "application/json"}

    url = "https://api.dune.com/api/v1/query"

    try:
        print("🔍 Creating duplicate detection query in Dune...")
        response = requests.post(url, json=query_data, headers=headers)
        response.raise_for_status()

        result = response.json()
        query_id = result.get("query_id")

        if query_id:
            print(f"✅ Successfully created duplicate detection query!")
            print(f"📊 Query ID: {query_id}")
            print(f"🔗 Query URL: https://dune.com/queries/{query_id}")
            print(f"")
            print(f"📝 Next steps:")
            print(f"1. Update _get_duplicate_detection_query_id() in dune_uploader.py")
            print(f"2. Set query_id = {query_id}")
            print(f"3. Test the duplicate detection")

            return query_id
        else:
            print(f"❌ No query ID returned: {result}")
            return None

    except Exception as e:
        print(f"❌ Error creating query: {e}")
        if hasattr(e, "response"):
            print(f"Response: {e.response.text}")
        return None


def test_query_execution(query_id: int):
    """Test the created query"""

    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        print("❌ DUNE_API_KEY not found")
        return False

    # Execute the query
    execute_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
    headers = {"X-DUNE-API-KEY": api_key, "Content-Type": "application/json"}

    params = {"performance": "medium"}

    try:
        print(f"🧪 Testing query execution...")
        response = requests.post(execute_url, json=params, headers=headers)
        response.raise_for_status()

        result = response.json()
        execution_id = result.get("execution_id")

        if execution_id:
            print(f"✅ Query executed successfully!")
            print(f"📊 Execution ID: {execution_id}")
            print(
                f"🔗 Results URL: https://api.dune.com/api/v1/execution/{execution_id}/results"
            )
            return True
        else:
            print(f"❌ No execution ID returned: {result}")
            return False

    except Exception as e:
        print(f"❌ Error executing query: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Creating Duplicate Detection Query in Dune")
    print("=" * 50)

    # Create the query
    query_id = create_duplicate_detection_query()

    if query_id:
        print(f"\n🧪 Testing Query Execution")
        print("=" * 30)
        test_query_execution(query_id)

        print(f"\n📋 Implementation Steps:")
        print(f"1. Update src/load/dune_uploader.py")
        print(f"2. Set _get_duplicate_detection_query_id() to return {query_id}")
        print(f"3. Test the pipeline with duplicate detection")
    else:
        print(f"\n❌ Failed to create query")

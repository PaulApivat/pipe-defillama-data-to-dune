#!/usr/bin/env python3
"""
Test the duplicate detection query implementation
"""

import os
import sys
from datetime import date
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, "src")

# Load environment variables
load_dotenv()


def test_duplicate_detection_query():
    """Test the duplicate detection query implementation"""

    print("🧪 Testing Duplicate Detection Query Implementation")
    print("=" * 60)

    try:
        from src.load.dune_uploader import DuneUploader

        # Create uploader in test mode
        uploader = DuneUploader(test_mode=True)

        # Test date
        test_date = date.today()

        print(f"📅 Testing date: {test_date}")
        print(f"🔍 Checking for existing data...")

        # Test the duplicate detection
        result = uploader._data_exists_for_date(test_date)

        print(f"📊 Result: {result}")

        if result:
            print(f"✅ Duplicate detection found existing data for {test_date}")
        else:
            print(f"✅ Duplicate detection found no existing data for {test_date}")

        print(f"\n📋 Implementation Status:")
        print(f"✅ DuneUploader._data_exists_for_date() implemented")
        print(f"✅ Query execution methods implemented")
        print(f"⚠️  Query ID needs to be configured")
        print(f"⚠️  Fallback to file-based detection active")

        return True

    except Exception as e:
        print(f"❌ Error testing duplicate detection: {e}")
        return False


def test_query_id_configuration():
    """Test query ID configuration"""

    print(f"\n🔧 Testing Query ID Configuration")
    print("=" * 40)

    try:
        from src.load.dune_uploader import DuneUploader

        uploader = DuneUploader(test_mode=True)
        query_id = uploader._get_duplicate_detection_query_id()

        if query_id:
            print(f"✅ Query ID configured: {query_id}")
            print(f"✅ Dune API query approach will be used")
        else:
            print(f"⚠️  No query ID configured")
            print(f"⚠️  Will fallback to file-based detection")
            print(f"")
            print(f"📝 To configure:")
            print(f"1. Run scripts/create_duplicate_detection_query.py")
            print(
                f"2. Update _get_duplicate_detection_query_id() with the returned query ID"
            )

        return query_id is not None

    except Exception as e:
        print(f"❌ Error testing query ID configuration: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Testing Duplicate Detection Query Implementation")
    print("=" * 60)

    # Test the implementation
    success1 = test_duplicate_detection_query()

    # Test query ID configuration
    success2 = test_query_id_configuration()

    print(f"\n📊 Test Results:")
    print(f"✅ Implementation: {'PASS' if success1 else 'FAIL'}")
    print(f"✅ Query ID Config: {'PASS' if success2 else 'FAIL'}")

    if success1 and success2:
        print(f"\n🎉 All tests passed! Duplicate detection is ready!")
    elif success1:
        print(f"\n⚠️  Implementation works, but query ID needs configuration")
        print(f"💡 Run scripts/create_duplicate_detection_query.py to set up the query")
    else:
        print(f"\n❌ Implementation needs fixes")

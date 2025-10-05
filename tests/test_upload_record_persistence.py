#!/usr/bin/env python3
"""
Test upload record persistence locally
"""

import os
import sys
from datetime import date, datetime

# Add src to path
sys.path.insert(0, "src")


def test_upload_record_persistence():
    """Test that upload records persist locally"""
    print("🧪 Testing Upload Record Persistence")
    print("=" * 50)

    # Test date
    test_date = date.today()
    date_str = test_date.strftime("%Y-%m-%d")
    upload_record = f"output/cache/uploaded_{date_str}.txt"

    # Ensure cache directory exists
    os.makedirs("output/cache", exist_ok=True)

    # Clean up any existing record
    if os.path.exists(upload_record):
        os.remove(upload_record)
        print(f"🧹 Cleaned up existing record")

    # Create upload record
    print(f"\n📝 Creating upload record: {upload_record}")
    with open(upload_record, "w") as f:
        f.write(f"Uploaded 100 records for {test_date} at {datetime.now()}")

    # Verify it exists
    if os.path.exists(upload_record):
        print(f"✅ Upload record created successfully")
        print(f"📁 File size: {os.path.getsize(upload_record)} bytes")

        # Read content
        with open(upload_record, "r") as f:
            content = f.read()
            print(f"📄 Content: {content}")
    else:
        print(f"❌ Upload record was not created")
        return False

    # Test DuneUploader detection
    print(f"\n🔍 Testing DuneUploader detection...")
    try:
        from src.load.dune_uploader import DuneUploader

        uploader = DuneUploader(test_mode=True)
        result = uploader._data_exists_for_date(test_date)
        print(f"🔍 _data_exists_for_date({test_date}) returned: {result}")

        if result:
            print("✅ Local file-based duplicate detection works!")
            return True
        else:
            print("❌ Local file-based duplicate detection failed!")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = test_upload_record_persistence()
    if success:
        print(f"\n🎉 SUCCESS: Local upload record persistence works!")
        print(f"💡 You can use this for local duplicate detection!")
    else:
        print(f"\n❌ FAILED: Local upload record persistence doesn't work!")

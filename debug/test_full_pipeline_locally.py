#!/usr/bin/env python3
"""
Test the full pipeline locally to see what's happening
"""

import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import PipelineOrchestrator

# Load environment variables
load_dotenv()


def test_full_pipeline_locally():
    """Test the full pipeline locally"""

    print("🔍 Testing Full Pipeline Locally")
    print("=" * 40)

    try:
        # Create pipeline instance (dry_run=True for safety)
        pipeline = PipelineOrchestrator(dry_run=True)

        print("🔄 Running daily update...")
        success = pipeline.run_daily_update()

        if success:
            print("✅ Pipeline completed successfully!")
        else:
            print("❌ Pipeline failed!")

        return success

    except Exception as e:
        print(f"❌ Pipeline failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_full_pipeline_locally()
    print(f"\n{'✅ Test passed' if success else '❌ Test failed'}")

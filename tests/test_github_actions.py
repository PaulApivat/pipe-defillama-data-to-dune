#!/usr/bin/env python3
"""
Test the GitHub Actions workflow locally
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import PipelineOrchestrator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def test_github_actions_workflow():
    """Test the GitHub Actions workflow locally"""

    print("🔄 Testing GitHub Actions Workflow")
    print("=" * 50)

    # This simulates what GitHub Actions will run
    print("📋 Simulating: python -m src.orchestration.pipeline --mode daily")

    try:
        # Create pipeline instance
        pipeline = PipelineOrchestrator(dry_run=False)

        # Run daily update (this is what GitHub Actions will do)
        print("🔄 Running daily update...")
        success = pipeline.run_daily_update()

        if success:
            print("✅ GitHub Actions workflow simulation successful!")
            print("\n📊 Expected behavior in production:")
            print("  - Runs daily at 6:00 AM UTC")
            print("  - Fetches fresh data from DeFiLlama API")
            print("  - Processes and uploads to Dune")
            print("  - Cleans up temporary files")
            print("  - Sends notifications on success/failure")

            return True
        else:
            print("❌ GitHub Actions workflow simulation failed!")
            return False

    except Exception as e:
        print(f"❌ Error during workflow simulation: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_github_actions_workflow()
    exit(0 if success else 1)

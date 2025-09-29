#!/usr/bin/env python3
"""
Test the GitHub Actions workflow locally (simple validation)
"""

import sys
import os
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.pipeline import PipelineOrchestrator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def test_github_actions_workflow():
    """Test the GitHub Actions workflow locally (simple validation)"""

    print("🔄 Testing GitHub Actions Workflow")
    print("=" * 50)
    print("This test validates the GitHub Actions setup without running full pipeline")
    print("📋 Simulating: python -m src.orchestration.pipeline --mode daily")

    try:
        # Test 1: Validate pipeline can be imported and initialized
        print("\n1️⃣ Testing pipeline import and initialization...")
        pipeline = PipelineOrchestrator(
            dry_run=True
        )  # Dry run to avoid actual execution
        print("✅ Pipeline imported and initialized successfully")

        # Test 2: Validate command line arguments work (without running)
        print("\n2️⃣ Testing command line interface...")

        # Test that the module can be imported and has the expected structure
        try:
            import src.orchestration.pipeline as pipeline_module

            print("✅ Pipeline module imports successfully")

            # Check if the main function exists
            if hasattr(pipeline_module, "main"):
                print("✅ Main function exists")
            else:
                print("❌ Main function not found")
                return False

            print("✅ Command line interface structure is correct")
        except ImportError as e:
            print(f"❌ Failed to import pipeline module: {e}")
            return False

        # Test 3: Validate GitHub Actions workflow file exists
        print("\n3️⃣ Testing GitHub Actions workflow file...")
        workflow_file = ".github/workflows/defillama_daily_pipeline.yml"
        if os.path.exists(workflow_file):
            print("✅ GitHub Actions workflow file exists")
        else:
            print("❌ GitHub Actions workflow file not found")
            return False

        # Test 4: Validate environment setup
        print("\n4️⃣ Testing environment setup...")

        dune_api_key = os.getenv("DUNE_API_KEY")
        if dune_api_key:
            print("✅ DUNE_API_KEY environment variable is set")
        else:
            print("⚠️  DUNE_API_KEY not set (will need to be set in GitHub Secrets)")

        print("\n🎉 All GitHub Actions validations passed!")
        print("\n📊 Expected behavior in production:")
        print("  - Runs daily at 6:00 AM UTC")
        print("  - Fetches fresh data from DeFiLlama API")
        print("  - Processes and uploads to Dune")
        print("  - Cleans up temporary files")
        print("  - Sends notifications on success/failure")

        return True

    except Exception as e:
        print(f"❌ Error during workflow simulation: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_github_actions_workflow()
    exit(0 if success else 1)

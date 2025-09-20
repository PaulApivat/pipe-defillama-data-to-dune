
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from unittest.mock import Mock, patch, MagicMock
from src.scheduler.scd2_scheduler import SCD2Scheduler


class TestSCD2Scheduler(unittest.TestCase):
    def setUp(self):
        # Mock the DuneUploader to avoid API key requirement
        with patch("src.scheduler.scd2_scheduler.DuneUploader") as mock_dune_uploader:
            mock_dune_uploader.return_value = MagicMock()
            self.scheduler = SCD2Scheduler()

    @patch("src.scheduler.scd2_scheduler.fetch_current_state")
    def test_weekly_dimension_update(self, mock_fetch):
        """Test weekly dimension update without API calls"""

        # Mock the data
        mock_current_state = Mock()
        mock_scd2_df = Mock()
        mock_fetch.return_value = (mock_current_state, mock_scd2_df)

        # Run the function
        self.scheduler.run_weekly_dimension_update()

        # Verify calls
        mock_fetch.assert_called_once()
        # Verify that upload_dimension_data was called on the mock
        self.scheduler.dune_uploader.upload_dimension_data.assert_called_once_with(
            mock_scd2_df
        )

    @patch("src.scheduler.scd2_scheduler.fetch_tvl_with_daily_metrics")
    def test_daily_fact_update(self, mock_fetch_tvl):
        """Test daily fact update without API calls"""

        # Mock the data
        mock_tvl_data = Mock()
        mock_daily_metrics = Mock()
        mock_fetch_tvl.return_value = (mock_tvl_data, mock_daily_metrics)

        # Run the function
        self.scheduler.run_daily_fact_update()

        # Verify calls
        mock_fetch_tvl.assert_called_once()
        # Verify that upload_daily_metrics_partition was called on the mock
        self.scheduler.dune_uploader.upload_daily_metrics_partition.assert_called_once()


if __name__ == "__main__":
    unittest.main()

"""
DeFiLlama API Client - Pure I/O Operations

This module handles all external API calls to DeFiLlama with no business logic.
Returns raw data structures that can be processed by the transform layer.
"""

import requests
import time
from typing import Dict, List, Any, Optional
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging

logger = logging.getLogger(__name__)

# API Endpoints
POOLS_OLD_ENDPOINT = "https://yields.llama.fi/poolsOld"
CHART_ENDPOINT_TEMPLATE = "https://yields.llama.fi/chart/{pool_id}"

# Rate limiting
REQUEST_DELAY = 0.1  # 100ms between requests


class DeFiLlamaAPIClient:
    """Pure API client for DeFiLlama endpoints"""

    def __init__(self, request_delay: float = REQUEST_DELAY):
        self.request_delay = request_delay
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy"""
        session = requests.Session()

        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def get_pools_old(self) -> List[Dict[str, Any]]:
        """
        Fetch current state data from DeFiLlama PoolsOld API

        Returns:
            List[Dict]: Raw pool data from API
        """
        logger.info(f"Fetching from {POOLS_OLD_ENDPOINT}")
        start_time = time.time()

        try:
            response = self.session.get(POOLS_OLD_ENDPOINT, timeout=30)
            response.raise_for_status()

            data = response.json()
            elapsed = time.time() - start_time

            logger.info(f"Fetched from {POOLS_OLD_ENDPOINT}: {elapsed:.2f} seconds")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching pools data: {e}")
            raise

    def get_chart_data(self, pool_id: str) -> Dict[str, Any]:
        """
        Fetch historical TVL data for a specific pool

        Args:
            pool_id: Pool identifier

        Returns:
            Dict: Raw chart data from API
        """
        url = CHART_ENDPOINT_TEMPLATE.format(pool_id=pool_id)
        logger.debug(f"Fetching from {url}")
        start_time = time.time()

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()
            elapsed = time.time() - start_time

            logger.debug(f"Fetched from {url}: {elapsed:.2f} seconds")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching chart data for {pool_id}: {e}")
            raise

    def get_chart_data_batch(self, pool_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch historical TVL data for multiple pools with rate limiting

        Args:
            pool_ids: List of pool identifiers

        Returns:
            Dict[str, Dict]: Mapping of pool_id to chart data
        """
        results = {}

        for i, pool_id in enumerate(pool_ids, 1):
            try:
                logger.info(f"Fetching TVL for pool {i}/{len(pool_ids)}: {pool_id}")
                chart_data = self.get_chart_data(pool_id)
                results[pool_id] = chart_data

                # Rate limiting
                if i < len(pool_ids):
                    time.sleep(self.request_delay)

            except Exception as e:
                logger.error(f"Failed to fetch data for pool {pool_id}: {e}")
                # Continue with other pools
                continue

        return results


# Convenience functions for direct use
def get_pools_old() -> List[Dict[str, Any]]:
    """Convenience function to get pools data"""
    client = DeFiLlamaAPIClient()
    return client.get_pools_old()


def get_chart_data(pool_id: str) -> Dict[str, Any]:
    """Convenience function to get chart data"""
    client = DeFiLlamaAPIClient()
    return client.get_chart_data(pool_id)


def get_chart_data_batch(pool_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Convenience function to get chart data for multiple pools"""
    client = DeFiLlamaAPIClient()
    return client.get_chart_data_batch(pool_ids)

import time
from urllib3.util.retry import Retry
import requests
from requests.adapters import HTTPAdapter
from typing import Any, Dict, Optional


DEFAULT_RETRY_STRATEGY = Retry(
    total=5,  # Total number of retries
    backoff_factor=2,  # The backoff factor (2 seconds, then 4, 8...)
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)


def new_session() -> requests.Session:
    """Create a new requests session with retry strategy"""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=DEFAULT_RETRY_STRATEGY)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set default headers
    session.headers.update(
        {"User-Agent": "pipe-defillama-data-to-dune/1.0", "Accept": "application/json"}
    )

    return session


def get_data(
    session: requests.Session,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    retry_attempts: int = 3,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    rate_limit_delay: float = 0.1,
) -> Dict[str, Any]:
    """Helper function to fetch data from a URL with retries and rate limiting.

    Args:
        session: HTTP session to use
        url: URL to fetch
        headers: Optional headers (defaults to JSON content type)
        retry_attempts: Number of retry attempts
        params: Optional query parameters
        timeout: Request timeout in seconds
        rate_limit_delay: Delay between requests in seconds

    Returns:
        Parsed JSON response

    Raises:
        requests.RequestException: On HTTP errors
        ValueError: On invalid JSON responses
    """
    headers = headers or {"Content-Type": "application/json"}

    # Simple rate limiting
    if hasattr(get_data, "_last_request_time"):
        elapsed = time.time() - get_data._last_request_time
        if elapsed < rate_limit_delay:
            time.sleep(rate_limit_delay - elapsed)

    get_data._last_request_time = time.time()

    # Retry logic
    for attempt in range(retry_attempts):
        try:
            start = time.time()
            response = session.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()

            if response.status_code != 200:
                raise requests.RequestException(
                    f"status={response.status_code}, url={url!r}"
                )

            print(f"Fetched from {url}: {time.time() - start:.2f} seconds")
            return response.json()

        except (requests.RequestException, ValueError) as e:
            if attempt == retry_attempts - 1:
                # Last attempt failed
                if isinstance(e, requests.RequestException):
                    raise requests.RequestException(
                        f"HTTP request failed for {url}: {str(e)}"
                    ) from e
                else:
                    raise ValueError(
                        f"Invalid JSON response from {url}: {str(e)}"
                    ) from e
            else:
                # Retry with exponential backoff
                wait_time = 2**attempt
                print(
                    f"Attempt {attempt + 1} failed for {url}, retrying in {wait_time}s..."
                )
                time.sleep(wait_time)

    # This should never be reached, but just in case
    raise requests.RequestException(
        f"Failed to fetch {url} after {retry_attempts} attempts"
    )


def get_json_simple(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """Simple GET request with JSON parsing - no retries or rate limiting"""
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()

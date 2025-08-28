import requests


def get_data(url: str, params: dict | None = None) -> dict:
    """Fetch data from URL and return JSON response."""
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

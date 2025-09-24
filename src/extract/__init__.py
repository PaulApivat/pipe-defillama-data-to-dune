"""
Extract Layer - Pure I/O to External APIs

This layer handles all external data fetching with no business logic.
- No imports from transform or load layers
- Pure functions that return raw data
- Handles API rate limiting, retries, error handling
"""

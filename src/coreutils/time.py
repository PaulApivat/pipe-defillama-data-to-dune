from datetime import datetime, timezone


def dt_fromtimestamp(ts: int) -> str:
    """Convert UNIX timestamp to YYYY-MM-DD string (UTC)."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")

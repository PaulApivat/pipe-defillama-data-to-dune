from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env


def env_get(key: str, default: str | None = None) -> str | None:
    """Get environment variable or return default."""
    return os.getenv(key, default)

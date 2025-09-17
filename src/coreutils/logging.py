import logging
from datetime import datetime


def setup_logging(level=logging.INFO):
    """Setup basic logging configuration"""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(
                f"logs/pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
            ),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


def log_function_call(func_name: str, **kwargs):
    """Log function calls with parameters"""
    logger = logging.getLogger(__name__)
    logger.info(f"Calling {func_name} with params: {kwargs}")

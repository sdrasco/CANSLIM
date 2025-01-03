# utils/logging_utils.py

import logging

def configure_logging():
    """Sets up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,  # Set the default logging level
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Suppress HTTPX and httpcore log entries
    httpx_logger = logging.getLogger("httpx")
    httpx_logger.setLevel(logging.ERROR)
    httpcore_logger = logging.getLogger("httpcore")
    httpcore_logger.setLevel(logging.ERROR)
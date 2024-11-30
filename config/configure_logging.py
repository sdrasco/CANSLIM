import logging

def configure_logging():
    """Sets up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,  # Set the default logging level
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
from config.configure_logging import configure_logging
import logging
import os
from datetime import date, timedelta
from pathlib import Path

# Configure logging
configure_logging()

# Create a logger for this module
logger = logging.getLogger(__name__)

# Base directory for storing data
DATA_DIR = Path(__file__).parent.parent / "data"

# Date range for downloading data
START_DATE = date(2003, 9, 1)  # Full starting date
END_DATE = date.today() - timedelta(days=1)  # Set to yesterday

# Credentials for Polygon.io
POLYGON_S3_KEY = os.getenv("POLYGONIO_ACCESS_KEY")  # Access Key
POLYGON_S3_SECRET = os.getenv("POLYGONIO_SECRET_KEY")  # Secret Key
POLYGON_API_KEY = os.getenv("POLYGONIO_API_KEY")  # API Key

# Validate credentials
if not POLYGON_S3_KEY or not POLYGON_S3_SECRET:
    logger.warning("Polygon S3 credentials are not fully set.")

# S3-specific settings
POLYGON_S3_ENDPOINT = "https://files.polygon.io"
POLYGON_BUCKET = "flatfiles"
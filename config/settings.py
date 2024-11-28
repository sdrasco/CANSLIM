import logging
import os
from datetime import date, timedelta

# Configure basic logging.  show warning or higher for external modules.
logging.basicConfig(
    level=logging.WARNING,  
    format='%(message)s'
)

# Create a logger for this module
logger = logging.getLogger(__name__)

# Show info level logger events for this module
logger.setLevel(logging.INFO)

# Base directory for storing data
DATA_DIR = "./data/"

# Date range for downloading data
# Full starting date
START_DATE = date(2003, 9, 1)  # Start from September 1, 2003

# set the end date to yesterday
END_DATE = date.today() - timedelta(days=1)

# Credentials for Polygon.io
POLYGON_S3_KEY = os.getenv("POLYGONIO_ACCESS_KEY")  # Access Key
POLYGON_S3_SECRET = os.getenv("POLYGONIO_SECRET_KEY")  # Secret Key
POLYGON_API_KEY = os.getenv("POLYGONIO_API_KEY")  # API Key 

# S3-specific settings
POLYGON_S3_ENDPOINT = "https://files.polygon.io"
POLYGON_BUCKET = "flatfiles"
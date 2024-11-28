import os

# Base directory for storing data
DATA_DIR = "./data/"

# Date range for downloading data
START_YEAR = 2003
END_YEAR = 2024
START_MONTH = 9  # Start from September 2003
END_MONTH = 11   # End at November 2024

# Credentials for Polygon.io
POLYGON_S3_KEY = os.getenv("POLYGONIO_ACCESS_KEY")  # Access Key
POLYGON_S3_SECRET = os.getenv("POLYGONIO_SECRET_KEY")  # Secret Key
POLYGON_API_KEY = os.getenv("POLYGONIO_API_KEY")  # API Key (not currently used)

# S3-specific settings
POLYGON_S3_ENDPOINT = "https://files.polygon.io"
POLYGON_BUCKET = "flatfiles"
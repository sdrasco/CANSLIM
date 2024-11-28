import os
import boto3
import logging
from datetime import timedelta
from botocore.config import Config
from config.settings import (
    POLYGON_S3_KEY,
    POLYGON_S3_SECRET,
    POLYGON_S3_ENDPOINT,
    POLYGON_BUCKET,
    DATA_DIR,
    START_DATE,
    END_DATE,
)

# Configure basic logging.  show warning or higher for external modules.
logging.basicConfig(
    level=logging.WARNING,  
    format='%(message)s'
)

# Create a logger for this module
logger = logging.getLogger(__name__)

# Show info level logger events for this module
logger.setLevel(logging.INFO)

def fetch_data():
    """
    Downloads missing data from Polygon.io's flat file system.
    Local files are checked before querying the server.
    """
    # Initialize the S3 session and client
    session = boto3.Session(
        aws_access_key_id=POLYGON_S3_KEY,
        aws_secret_access_key=POLYGON_S3_SECRET,
    )

    s3 = session.client(
        's3',
        endpoint_url=POLYGON_S3_ENDPOINT,
        config=Config(signature_version='s3v4'),
    )

    # Define base directory and make sure it exists
    base_data_dir = os.path.join(DATA_DIR, "us_stocks_sip")
    os.makedirs(base_data_dir, exist_ok=True)

    # Generate a list of all expected files
    expected_files = generate_expected_files(START_DATE, END_DATE)

    # Find missing files by comparing to local files
    missing_files = find_missing_files(expected_files, DATA_DIR)

    # Download missing files if they exist on the server
    for file_key in missing_files:
        local_file_path = os.path.join(DATA_DIR, file_key)

        # Create necessary directories
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        try:
            # Check if the file exists on the server
            s3.head_object(Bucket=POLYGON_BUCKET, Key=file_key)

            # Download the file
            logger.info(f"Downloading {file_key} to {local_file_path}")
            s3.download_file(POLYGON_BUCKET, file_key, local_file_path)
        except s3.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Silently skip holidays
                continue
            else:
                # Log and raise unexpected errors
                logger.error(f"Error checking or downloading file: {file_key}")
                raise

def generate_expected_files(start_date, end_date):
    """
    Generate a list of all expected file paths based on the date range,
    excluding Saturdays, Sundays, fixed holidays, and future dates.
    """
    from datetime import timedelta

    # List of fixed stock holidays to exclude (MM-DD format)
    # We could do better, but a decent start here and the others will be caught.
    fixed_holidays = [
        "01-01",  # New Year's Day
        "07-04",  # Independence Day
        "12-25",  # Christmas Day
    ]

    expected_files = []

    current_date = start_date
    while current_date <= end_date:
        weekday = current_date.weekday()  # 0 = Monday, 6 = Sunday
        mm_dd = f"{current_date.month:02d}-{current_date.day:02d}"

        # Skip Saturdays (5) and Sundays (6)
        if weekday >= 5:
            current_date += timedelta(days=1)
            continue

        # Skip fixed holidays
        if mm_dd in fixed_holidays:
            current_date += timedelta(days=1)
            continue

        # Add the file path for valid trading days
        file_path = (
            f"us_stocks_sip/day_aggs_v1/{current_date.year:04d}/"
            f"{current_date.month:02d}/{current_date.year:04d}-"
            f"{current_date.month:02d}-{current_date.day:02d}.csv.gz"
        )
        expected_files.append(file_path)

        current_date += timedelta(days=1)

    return expected_files


def find_missing_files(expected_files, data_dir):
    """
    Identify files that are missing locally by comparing to expected files.
    """
    missing_files = []

    for file_key in expected_files:
        local_file_path = os.path.join(data_dir, file_key)
        if not os.path.exists(local_file_path):
            missing_files.append(file_key)

    return missing_files
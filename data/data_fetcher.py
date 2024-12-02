import os
import boto3
import logging
import gzip
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from botocore.config import Config
from botocore.exceptions import ClientError
from config.configure_logging import configure_logging
from config.settings import (
    POLYGON_S3_KEY,
    POLYGON_S3_SECRET,
    POLYGON_S3_ENDPOINT,
    POLYGON_BUCKET,
    DATA_DIR,
    START_DATE,
    END_DATE,
)

# Configure logging
configure_logging()

# Create a logger for this module
logger = logging.getLogger(__name__)

def fetch_data():
    """
    Downloads missing data from Polygon.io's flat file system and saves them directly in Feather format.
    Validates the Feather files against the original CSV data to ensure correctness.
    """
    # Initialize the S3 session and client
    session = boto3.Session(
        aws_access_key_id=POLYGON_S3_KEY,
        aws_secret_access_key=POLYGON_S3_SECRET,
    )

    s3 = session.client(
        "s3",
        endpoint_url=POLYGON_S3_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )

    # Define the Feather data directory and ensure it exists
    feather_data_dir = Path(DATA_DIR) / "us_stocks_sip/day_aggs_feather"
    feather_data_dir.mkdir(parents=True, exist_ok=True)

    # Adjust START_DATE based on local Feather files
    start_date = adjust_start_date(START_DATE, feather_data_dir)

    # Generate a list of all expected files
    expected_files = generate_expected_files(start_date, END_DATE)

    # Find missing files by comparing to Feather files
    missing_files = find_missing_files(expected_files, feather_data_dir)

    # Download and process missing files
    for file_key in missing_files:
        feather_file_path = os.path.join(
            feather_data_dir, file_key.replace(".csv.gz", ".feather")
        )

        # Create necessary directories
        os.makedirs(os.path.dirname(feather_file_path), exist_ok=True)

        try:
            # Check if the file exists on the server
            s3.head_object(Bucket=POLYGON_BUCKET, Key=file_key)

            # Download the file
            logger.info(f"Downloading {file_key}")
            with gzip.open(s3.get_object(Bucket=POLYGON_BUCKET, Key=file_key)["Body"], "rb") as f_in:
                original_df = pd.read_csv(f_in)

            # Convert the data to Feather format
            logger.info(f"Saving data as Feather: {feather_file_path}")
            original_df.to_feather(feather_file_path)

            # Validation: Reload Feather and compare
            validate_conversion(original_df, feather_file_path)

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                # Silently skip holidays or missing files
                logger.warning(f"File not found on server: {file_key} (404 error)")
                continue
            else:
                # Log and raise unexpected errors
                logger.error(f"Error checking or downloading file: {file_key} - {e}")
                raise

        except Exception as e:
            # Log any unexpected errors during the download and processing
            logger.error(f"Unexpected error for file {file_key}: {e}")
            raise

def validate_conversion(original_df, feather_file_path):
    """
    Validates that the Feather file matches the original CSV data.
    Compares the original DataFrame to the one loaded from the Feather file.
    """
    logger.info(f"Validating Feather file: {feather_file_path}")
    try:
        # Load the Feather file
        converted_df = pd.read_feather(feather_file_path)

        # Check if the dataframes are equal
        if not original_df.equals(converted_df):
            logger.error(f"Validation failed for {feather_file_path}: Data mismatch.")
            raise ValueError(f"Data mismatch in {feather_file_path}")
        logger.info(f"Validation passed for {feather_file_path}")
    except Exception as e:
        logger.error(f"Validation error for {feather_file_path}: {e}")
        raise


def adjust_start_date(start_date, feather_data_dir):
    """
    Adjust the START_DATE to the date of the most recent local Feather file.
    If no files are found, return the original start_date.
    """
    latest_date = None

    # Traverse the Feather directory and find the most recent file by date in the filename
    for root, _, files in os.walk(feather_data_dir):
        for file_name in files:
            if file_name.endswith(".feather"):
                try:
                    # Extract the date from the file name (e.g., YYYY-MM-DD.feather)
                    file_date_str = file_name.split(".")[0]
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                    if not latest_date or file_date > latest_date:
                        latest_date = file_date
                except ValueError:
                    continue  # Skip files that don't match the expected format

    # Return the day after the most recent file date, or the original start_date if no files exist
    if latest_date:
        logger.info(f"Most recent local file date: {latest_date.strftime('%Y-%m-%d')}")
        return latest_date + timedelta(days=1)
    else:
        logger.info("No local files found. Fetching whole collection.")
        return start_date


def generate_expected_files(start_date, end_date):
    """
    Generate a list of all expected file paths based on the date range,
    excluding Saturdays, Sundays, fixed holidays, and future dates.
    """
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

        if weekday >= 5 or mm_dd in fixed_holidays:
            current_date += timedelta(days=1)
            continue

        file_path = (
            f"us_stocks_sip/day_aggs_v1/{current_date.year:04d}/"
            f"{current_date.month:02d}/{current_date.year:04d}-"
            f"{current_date.month:02d}-{current_date.day:02d}.csv.gz"
        )
        expected_files.append(file_path)
        current_date += timedelta(days=1)

    return expected_files

def find_missing_files(expected_files, feather_data_dir):
    """
    Identify files that are missing locally by comparing to expected Feather files.
    """
    feather_data_dir = Path(feather_data_dir)  # Ensure it's a Path object
    missing_files = []

    for file_key in expected_files:
        # Remove the 'us_stocks_sip/day_aggs_v1/' prefix
        relative_file_key = file_key.replace("us_stocks_sip/day_aggs_v1/", "")
        # Convert to Feather file path
        relative_feather_path = relative_file_key.replace(".csv.gz", ".feather")
        # Construct the full local path
        local_feather_path = feather_data_dir / relative_feather_path
        # Check if the local Feather file exists
        if not local_feather_path.exists():
            missing_files.append(file_key)

    return missing_files
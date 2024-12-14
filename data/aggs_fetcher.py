# data.aggs_fetcher

import os
import boto3
import logging
import gzip
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from botocore.config import Config
from botocore.exceptions import ClientError
from config.settings import (
    POLYGON_S3_KEY,
    POLYGON_S3_SECRET,
    POLYGON_S3_ENDPOINT,
    POLYGON_BUCKET,
    DATA_DIR,
    FLAT_FILES_START_DATE,
    END_DATE,
)
from utils.logging_utils import configure_logging 

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

class AggregatesFetcher:
    def __init__(self):
        self.feather_data_dir = Path(DATA_DIR) / "us_stocks_sip/day_aggs_feather"
        self.start_date = FLAT_FILES_START_DATE
        self.end_date = END_DATE

        # S3 session setup
        self.s3_session = boto3.Session(
            aws_access_key_id=POLYGON_S3_KEY,
            aws_secret_access_key=POLYGON_S3_SECRET,
        )
        self.s3_client = self.s3_session.client(
            "s3",
            endpoint_url=POLYGON_S3_ENDPOINT,
            config=Config(signature_version="s3v4"),
        )

    def adjust_start_date(self):
        latest_date = None
        for root, _, files in os.walk(self.feather_data_dir):
            for file_name in files:
                if file_name.endswith(".feather"):
                    try:
                        file_date_str = file_name.split(".")[0]
                        file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                        if not latest_date or file_date > latest_date:
                            latest_date = file_date
                    except ValueError:
                        continue

        if latest_date:
            logger.info(f"Most recent local file date: {latest_date.strftime('%Y-%m-%d')}")
            return latest_date + timedelta(days=1)
        else:
            logger.info("No local files found. Fetching the full range.")
            return self.start_date

    def generate_expected_files(self):
        fixed_holidays = ["01-01", "07-04", "12-25"]
        expected_files = []
        current_date = self.start_date

        while current_date <= self.end_date:
            if current_date.weekday() < 5:  # Skip weekends
                mm_dd = f"{current_date.month:02d}-{current_date.day:02d}"
                if mm_dd not in fixed_holidays:
                    file_path = (
                        f"us_stocks_sip/day_aggs_v1/{current_date.year:04d}/"
                        f"{current_date.month:02d}/{current_date.year:04d}-"
                        f"{current_date.month:02d}-{current_date.day:02d}.csv.gz"
                    )
                    expected_files.append(file_path)
            current_date += timedelta(days=1)

        return expected_files

    def find_missing_files(self, expected_files):
        feather_files = {str(file.relative_to(self.feather_data_dir)) for file in self.feather_data_dir.rglob("*.feather")}
        missing_files = [
            file_key
            for file_key in expected_files
            if file_key.replace(".csv.gz", ".feather") not in feather_files
        ]
        return missing_files

    def fetch_file(self, file_key):
        feather_file_path = self.feather_data_dir / file_key.replace(".csv.gz", ".feather")
        feather_file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Check if the file exists on the server
            self.s3_client.head_object(Bucket=POLYGON_BUCKET, Key=file_key)

            # Download and process the file
            with gzip.open(self.s3_client.get_object(Bucket=POLYGON_BUCKET, Key=file_key)["Body"], "rb") as f_in:
                original_df = pd.read_csv(f_in)

            # Save as Feather format
            original_df.to_feather(feather_file_path)
            logger.info(f"Successfully saved {feather_file_path}")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                # Silently skip missing files, assuming they correspond to holidays
                pass
            else:
                logger.error(f"Error checking or downloading file: {file_key} - {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error for file {file_key}: {e}")
            raise

    def fetch_flat_files(self):
        self.feather_data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"FEATHER_DATA_DIR resolved to: {self.feather_data_dir}")

        self.start_date = self.adjust_start_date()
        expected_files = self.generate_expected_files()
        missing_files = self.find_missing_files(expected_files)

        if not missing_files:
            logger.info("No missing files to fetch. All data is up to date.")
            return

        for file_key in missing_files:
            self.fetch_file(file_key)

    def run(self):
        try:
            logger.info("Starting aggregation data fetching workflow...")
            self.fetch_flat_files()
            logger.info("Aggregation data fetching workflow completed successfully.")
        except Exception as e:
            logger.error(f"An error occurred during the aggregation data fetching workflow: {e}")
            raise
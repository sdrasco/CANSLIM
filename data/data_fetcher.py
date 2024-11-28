# data_fetcher.py
import os
import boto3
from botocore.config import Config
from config.settings import (
    POLYGON_S3_KEY,
    POLYGON_S3_SECRET,
    POLYGON_S3_ENDPOINT,
    POLYGON_BUCKET,
    DATA_DIR,
    START_YEAR,
    END_YEAR,
    START_MONTH,
    END_MONTH,
)

def fetch_data():
    """
    Downloads data from Polygon.io's flat file system, skipping files that already exist.
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

    # Base directory for data
    base_data_dir = os.path.join(DATA_DIR, "us_stocks_sip")
    os.makedirs(base_data_dir, exist_ok=True)

    # Download files for the specified date range
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            # Skip months outside the specified range
            if year == START_YEAR and month < START_MONTH:
                continue
            if year == END_YEAR and month > END_MONTH:
                continue

            prefix_with_date = f"us_stocks_sip/day_aggs_v1/{year:04d}/{month:02d}/"
            print(f"Searching for files in: {prefix_with_date}")

            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=POLYGON_BUCKET, Prefix=prefix_with_date):
                for obj in page.get('Contents', []):
                    file_key = obj['Key']
                    local_file_path = os.path.join(DATA_DIR, file_key)

                    # Check if the file already exists locally
                    if os.path.exists(local_file_path):
                        print(f"File already exists: {local_file_path}")
                        continue

                    # Create directories as needed
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                    # Download the file
                    print(f"Downloading {file_key} to {local_file_path}")
                    s3.download_file(POLYGON_BUCKET, file_key, local_file_path)
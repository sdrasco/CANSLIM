import os
import boto3
from botocore.config import Config

# Initialize a session credentials
ACCESS_KEY = os.getenv("POLYGONIO_ACCESS_KEY")
SECRET_KEY = os.getenv("POLYGONIO_SECRET_KEY")
session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Create a client 
s3 = session.client(
    's3',
    endpoint_url='https://files.polygon.io',
    config=Config(signature_version='s3v4'),
)

# Initialize a paginator for listing objects
paginator = s3.get_paginator('list_objects_v2')

# Choose the appropriate prefix 
prefix = 'us_stocks_sip'  

# List objects using the selected prefix
for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix):
    for obj in page.get('Contents', []):
        print(obj['Key'])

# Specify the bucket name
bucket_name = 'flatfiles'

# Specify the S3 object key name
object_key = 'us_stocks_sip/trades_v1/2024/03/2024-03-07.csv.gz'  # Example path

# Specify the local file path to save the downloaded file
local_file_path = './' + object_key.split('/')[-1]

# Download the file
s3.download_file(bucket_name, object_key, local_file_path)
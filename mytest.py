import pandas as pd

file_path = "/Users/sdrasco/Desktop/BackBoard/data/us_stocks_sip/day_aggs_feather/processed_data.feather"

try:
    # Load the Feather file
    df = pd.read_feather(file_path)
    print("File loaded successfully.")
    print(f"Columns: {df.columns}")
    print(f"Data types:\n{df.dtypes}")
    print(f"First few rows:\n{df.head()}")
except Exception as e:
    print(f"Error reading Feather file: {e}")
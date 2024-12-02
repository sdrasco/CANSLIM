# main.py

import os
import logging
from data.data_fetcher import fetch_data
from data.data_processor import process_data
from config.settings import DATA_DIR, START_DATE, END_DATE
from config.configure_logging import configure_logging

# Configure logging
configure_logging()

# Create a logger for this module
logger = logging.getLogger(__name__)

def main():
    # Define paths
    base_data_dir = os.path.join(DATA_DIR, "us_stocks_sip/day_aggs_feather")
    processed_data_path = os.path.join(DATA_DIR, "processed_data.feather")
    
    # Fetch data
    logger.info(f"Fetching data for the range: {START_DATE} to {END_DATE}.")
    fetch_data()
    
    # Process data
    logger.info("Processing collected data.")
    processed_data = process_data(base_data_dir, output_path=processed_data_path)
    
    if not processed_data.empty:
        logger.info("Data processed successfully.")
        # Proceed with strategy evaluation, backtesting, etc.
        # For example:
        # signals = run_strategy(processed_data)
        # backtest_results = backtest(signals)
    else:
        logger.error("Processed data is empty. Please check for errors in data processing.")

if __name__ == "__main__":
    main()
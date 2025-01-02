""" 
preprocess.py

Can be executed directly or imported as a module.
Handles the data preparation steps:
  1) Optionally remove old local data files
  2) Fetch daily aggregates
  3) Process aggregates
  4) Fetch financials

Helps isolate time-consuming tasks that rarely change.
"""

import logging
import os
from pathlib import Path

from config.settings import DATA_DIR
from data.aggs_fetcher import AggregatesFetcher
from data.aggs_processor import AggregatesProcessor
from data.financials_fetcher import FinancialsFetcher
from utils.logging_utils import configure_logging

def run_preprocessing(remove_old_files=True):
    """
    1) Optionally remove old local data files
    2) Fetch daily aggregates
    3) Process aggregates
    4) Fetch financials
    """
    configure_logging()
    logger = logging.getLogger(__name__)

    try:
        if remove_old_files:
            files_to_remove = [
                "financials.feather",
                "proxies.feather",
                "top_stocks_tickersymbols.csv",
                "top_stocks.feather"
            ]
            for filename in files_to_remove:
                file_path = DATA_DIR / filename
                if file_path.exists():
                    os.remove(file_path)
            logger.info(f"Removed old files: {files_to_remove}")
        else:
            logger.info("Skipping removal of old data files.")

        logger.info("Fetching aggregates data...")
        fetcher = AggregatesFetcher()
        fetcher.run()
        logger.info("Aggregates data fetched.")

        logger.info("Processing aggregates data...")
        aggs_processor = AggregatesProcessor(
            base_dir=DATA_DIR / "us_stocks_sip" / "day_aggs_feather",
            output_path=DATA_DIR / "processed_data.feather"
        )
        aggs_processor.process()
        logger.info("Aggregates data processed.")

        logger.info("Fetching financials data...")
        financials_fetcher = FinancialsFetcher()
        financials_fetcher.run()
        logger.info("Financials data fetched.")

    except Exception as e:
        logger.error(f"An error occurred during preprocessing: {e}", exc_info=True)
        raise

def main():
    """
    Allows this script to be run directly from the command line.
    Calls run_preprocessing() with default behavior.
    """
    run_preprocessing(remove_old_files=True)

if __name__ == "__main__":
    main()
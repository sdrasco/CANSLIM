# main.py

import logging
from data.aggs_fetcher import AggregatesFetcher
from data.aggs_processor import AggregatesProcessor
from data.financials_fetcher import FinancialsFetcher
from config.settings import DATA_DIR

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # Step 1: Fetch Aggregates Data
        logger.info("Step 1: Fetching aggregates data...")
        fetcher = AggregatesFetcher()
        fetcher.run()
        logger.info("Step 1 completed: Aggregates data fetched.")

        # Step 2: Process Aggregates Data
        logger.info("Step 2: Processing aggregates data...")
        aggs_processor = AggregatesProcessor(
            base_dir=DATA_DIR / "us_stocks_sip/day_aggs_feather",
            output_path=DATA_DIR / "us_stocks_sip/day_aggs_feather/processed_data.feather",
            top_n_tickers=1000,  # Adjust this as needed
        )
        aggs_processor.process()
        logger.info("Step 2 completed: Aggregates data processed.")

        # Step 3: Fetch Financials Data
        logger.info("Step 3: Fetching financials data...")
        financials_fetcher = FinancialsFetcher()
        financials_fetcher.run()
        logger.info("Step 3 completed: Financials data fetched.")

    except Exception as e:
        logger.error(f"An error occurred in the pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
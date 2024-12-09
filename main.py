# main.py

import logging
import pandas as pd
from data.aggs_fetcher import AggregatesFetcher
from data.aggs_processor import AggregatesProcessor
from data.financials_fetcher import FinancialsFetcher
from config.settings import DATA_DIR, NUM_TICKERS
from utils.logging_utils import configure_logging
from data.canslim_calculator import calculate_canslim_indicators

# Configure logging
configure_logging()
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
            output_path=DATA_DIR / "processed_data.feather",
            top_n_tickers=NUM_TICKERS,
        )
        aggs_processor.process()
        logger.info("Step 2 completed: Aggregates data processed.")

        # Step 3: Fetch Financials Data
        logger.info("Step 3: Fetching financials data...")
        financials_fetcher = FinancialsFetcher()
        financials_fetcher.run()
        logger.info("Step 3 completed: Financials data fetched.")

        # Step 4: Calculate CANSLIM Indicators
        logger.info("Step 4: Calculating CANSLIM indicators...")

        # Load the data produced by steps 2 and 3
        top_stocks_path = DATA_DIR / "top_stocks.feather"
        market_proxy_path = DATA_DIR / "market_proxy.feather"
        financials_path = DATA_DIR / "financials.feather"

        if not (top_stocks_path.exists() and market_proxy_path.exists() and financials_path.exists()):
            logger.error("Required data files for CANSLIM calculation are missing.")
            return

        top_stocks_df = pd.read_feather(top_stocks_path)
        market_proxy_df = pd.read_feather(market_proxy_path)
        financials_df = pd.read_feather(financials_path)

        # Compute CANSLIM indicators
        market_proxy_df, top_stocks_df, financials_df = calculate_canslim_indicators(
            market_proxy_df, top_stocks_df, financials_df
        )

        # Save the updated DataFrames
        top_stocks_df.reset_index(drop=True).to_feather(top_stocks_path)
        market_proxy_df.reset_index(drop=True).to_feather(market_proxy_path)
        # financials_df is unchanged, so no need to resave unless you want to.

        logger.info("Step 4 completed: CANSLIM indicators calculated and saved.")

    except Exception as e:
        logger.error(f"An error occurred in the pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
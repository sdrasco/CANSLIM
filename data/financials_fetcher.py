# data.financials_fetcher.py

import asyncio
import logging
import pandas as pd
import httpx
from pathlib import Path
from config.settings import DATA_DIR, START_DATE, END_DATE, POLYGON_API_KEY
from utils.logging_utils import configure_logging
import traceback

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# File paths
FINANCIALS_FILE = Path(DATA_DIR) / "financials.feather"

class FinancialsFetcher:
    """
    Fetch financial data for tickers using Polygon.io's API.
    """
    def __init__(self):
        self.tickers = self.load_tickers_from_data()

    async def fetch_financials_for_ticker(self, session, ticker, timeframe):
        """
        Fetch financials for a single ticker asynchronously.

        Parameters:
            session (httpx.AsyncClient): HTTPX async session.
            ticker (str): The ticker symbol to fetch financials for.
            timeframe (str): "quarterly" or "annual".
        
        Returns:
            pd.DataFrame: A DataFrame containing the financial data for the ticker.
        """
        params = {
            "ticker": ticker,
            "period_of_report_date.gte": START_DATE.strftime("%Y-%m-%d"),
            "period_of_report_date.lt": END_DATE.strftime("%Y-%m-%d"),
            "timeframe": timeframe,
            "order": "asc",
            "limit": 100,
            "apiKey": POLYGON_API_KEY,
        }
        url = "https://api.polygon.io/vX/reference/financials"
        try:
            full_url = f"{url}?{'&'.join(f'{key}={value}' for key, value in params.items())}"
            logger.debug(f"Fetching financials for {ticker}: {full_url}")
            response = await session.get(url, params=params)
            response.raise_for_status()
            results = response.json().get("results", [])
            if not results:
                logger.warning(f"No financial data returned for ticker {ticker}.")
            return pd.DataFrame(results)
        except Exception as e:
            logger.error(f"Error fetching financials for {ticker}: {e}")
            logger.debug(f"Full URL for {ticker}: {full_url}")
            return pd.DataFrame()

    async def fetch_all_financials(self):
        """
        Fetch financials for all tickers asynchronously.

        Returns:
            pd.DataFrame: Combined DataFrame containing financial data for all tickers.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                self.fetch_financials_for_ticker(session, ticker, "quarterly")
                for ticker in self.tickers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log exceptions and collect successful results
        successful_results = []
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                logger.error(f"Task {i} failed with exception: {res}")
                logger.debug(f"Traceback for task {i}:\n{traceback.format_exc()}")
            elif isinstance(res, pd.DataFrame) and not res.empty:
                successful_results.append(res)

        if not successful_results:
            logger.error("No financial data was successfully fetched.")
        return pd.concat(successful_results, ignore_index=True) if successful_results else pd.DataFrame()

    def save_financials(self, financials):
        """
        Save fetched financials to a Feather file.

        Parameters:
            financials (pd.DataFrame): DataFrame to save.
        """
        if not financials.empty:
            try:
                FINANCIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
                financials.to_feather(FINANCIALS_FILE)
                logger.info(f"Financials saved to {FINANCIALS_FILE}")
            except Exception as e:
                logger.error(f"Error saving financials to {FINANCIALS_FILE}: {e}")
                logger.debug(f"Traceback:\n{traceback.format_exc()}")

    def load_tickers_from_data(self):
        """
        Load tickers from the processed aggregates data.

        Returns:
            list: List of unique tickers.
        """
        processed_data_path = Path(DATA_DIR) / "processed_data.feather"
        try:
            if not processed_data_path.exists():
                logger.error(f"Processed aggregates data not found at {processed_data_path}")
                return []
            tickers = pd.read_feather(processed_data_path)["ticker"].unique().tolist()
            logger.info(f"Loaded {len(tickers)} tickers from processed data.")
            return tickers
        except Exception as e:
            logger.error(f"Error loading tickers from {processed_data_path}: {e}")
            logger.debug(f"Traceback:\n{traceback.format_exc()}")
            return []

    def run(self):
        """
        Execute the financials fetching workflow.
        """
        if not self.tickers:
            logger.error("No tickers available to fetch financials.")
            return
        financials = asyncio.run(self.fetch_all_financials())
        self.save_financials(financials)
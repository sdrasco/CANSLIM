# data.financials_fetcher.py

import asyncio
import logging
import pandas as pd
import httpx
from pathlib import Path
from config.settings import DATA_DIR, START_DATE, END_DATE, POLYGON_API_KEY

# Configure logging
logging.basicConfig(level=logging.INFO)
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
        Fetch financials for a single ticker.
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
            response = await session.get(url, params=params)
            response.raise_for_status()
            return pd.DataFrame(response.json().get("results", []))
        except Exception as e:
            logger.error(f"Error fetching financials for {ticker}: {e}")
            return pd.DataFrame()

    async def fetch_all_financials(self):
        """
        Fetch financials for all tickers asynchronously.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                self.fetch_financials_for_ticker(session, ticker, "quarterly")
                for ticker in self.tickers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        financials = pd.concat(
            [res for res in results if isinstance(res, pd.DataFrame) and not res.empty],
            ignore_index=True,
        )
        return financials

    def save_financials(self, financials):
        """
        Save fetched financials to a Feather file.
        """
        if not financials.empty:
            FINANCIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
            financials.to_feather(FINANCIALS_FILE)
            logger.info(f"Financials saved to {FINANCIALS_FILE}")

    def load_tickers_from_data(self):
        """
        Load tickers from the processed aggregates data.
        """
        processed_data_path = Path(DATA_DIR) / "us_stocks_sip/day_aggs_feather/processed_data.feather"
        if not processed_data_path.exists():
            logger.error(f"Processed aggregates data not found at {processed_data_path}")
            return []
        return pd.read_feather(processed_data_path)["ticker"].unique().tolist()

    def run(self):
        """
        Execute the financials fetching workflow.
        """
        if not self.tickers:
            logger.error("No tickers available to fetch financials.")
            return
        financials = asyncio.run(self.fetch_all_financials())
        self.save_financials(financials)
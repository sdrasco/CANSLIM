# data/financials_fetcher.py

import asyncio
import logging
import pandas as pd
import httpx
from pathlib import Path
from config.settings import DATA_DIR, START_DATE, END_DATE, POLYGON_API_KEY
from utils.logging_utils import configure_logging
import traceback
import numpy as np

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

FINANCIALS_FILE = Path(DATA_DIR) / "financials.feather"
TOP_STOCKS_FEATHER = Path(DATA_DIR) / "top_stocks.feather"
TOP_STOCKS_TICKERS_CSV = Path(DATA_DIR) / "top_stocks_tickersymbols.csv"

# Limit the number of concurrent requests
MAX_CONCURRENT_REQUESTS = 100

class FinancialsFetcher:
    """
    Fetch financial data for tickers using Polygon.io's API (quarterly).
    Extract fields: ticker, timeframe, fiscal_period, fiscal_year, end_date, diluted_eps.
    """

    def __init__(self):
        self.tickers = self._load_tickers()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    def _load_tickers(self):
        """
        Load tickers from top_stocks_tickersymbols.csv.
        """
        if not TOP_STOCKS_TICKERS_CSV.exists():
            logger.error(f"Top stocks ticker symbols file not found at {TOP_STOCKS_TICKERS_CSV}")
            return []

        try:
            tickers_df = pd.read_csv(TOP_STOCKS_TICKERS_CSV)
            tickers = tickers_df["ticker"].unique().tolist()
            logger.info(f"Loaded {len(tickers)} tickers from {TOP_STOCKS_TICKERS_CSV}")
            return tickers
        except Exception as e:
            logger.error(f"Error loading tickers from {TOP_STOCKS_TICKERS_CSV}: {e}")
            logger.debug(f"Traceback:\n{traceback.format_exc()}")
            return []

    async def _fetch_financials_for_ticker(self, session, ticker, timeframe="quarterly", limit=100):
        """
        Fetch financials for a single ticker asynchronously, handling pagination.
        Extract required fields.
        """
        async with self.semaphore:
            url = "https://api.polygon.io/vX/reference/financials"
            params = {
                "ticker": ticker,
                "period_of_report_date.gte": START_DATE.strftime("%Y-%m-%d"),
                "period_of_report_date.lt": END_DATE.strftime("%Y-%m-%d"),
                "timeframe": timeframe,
                "order": "asc",
                "limit": limit,
                "apiKey": POLYGON_API_KEY,
            }

            all_results = []
            page_count = 0

            while True:
                try:
                    response = await session.get(url, params=params)
                    response.raise_for_status()
                    json_data = response.json()

                    results = json_data.get("results", [])
                    if results:
                        all_results.extend(results)
                    else:
                        # No results means done
                        break

                    next_url = json_data.get("next_url")
                    if not next_url:
                        break

                    page_count += 1
                    logger.debug(f"Fetched page {page_count} for ticker {ticker}, {len(results)} results.")

                    from urllib.parse import urlparse, parse_qs
                    parsed = urlparse(next_url)
                    next_params = parse_qs(parsed.query)
                    flat_params = {k: v[0] for k, v in next_params.items()}
                    if "apiKey" not in flat_params:
                        flat_params["apiKey"] = POLYGON_API_KEY
                    params = flat_params

                except Exception as e:
                    logger.error(f"Error fetching financials for {ticker}: {e}")
                    logger.debug(f"Params used: {params}")
                    logger.debug(f"Traceback:\n{traceback.format_exc()}")
                    break

            if not all_results:
                logger.warning(f"No financial data returned for ticker {ticker}.")
                return pd.DataFrame()

            # Parse results to get the needed columns
            records = []
            for r in all_results:
                r_tickers = r.get("tickers", [ticker])
                timeframe = r.get("timeframe", None)
                fiscal_period = r.get("fiscal_period", None)
                fiscal_year = r.get("fiscal_year", None)
                end_date = r.get("end_date", None)  # Keep as end_date

                financials = r.get("financials", {})
                income_statement = financials.get("income_statement", {})
                eps_obj = income_statement.get("diluted_earnings_per_share", {})
                diluted_eps = eps_obj.get("value", np.nan)
                if diluted_eps is None:
                    diluted_eps = np.nan

                for tkr in r_tickers:
                    if tkr in self.tickers:
                        records.append({
                            "ticker": tkr,
                            "timeframe": timeframe,
                            "fiscal_period": fiscal_period,
                            "fiscal_year": fiscal_year,
                            "end_date": end_date,
                            "diluted_eps": diluted_eps
                        })

            df = pd.DataFrame(records)
            if "end_date" in df.columns and not df.empty:
                df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

            return df

    async def _fetch_all_financials(self):
        """
        Fetch financials for all tickers asynchronously.
        """
        if not self.tickers:
            logger.error("No tickers available to fetch financials.")
            return pd.DataFrame()

        async with httpx.AsyncClient() as session:
            tasks = [self._fetch_financials_for_ticker(session, t, "quarterly") for t in self.tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_results = []
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                logger.error(f"Task {i} encountered an exception: {res}")
                logger.debug(f"Traceback:\n{traceback.format_exc()}")
            elif isinstance(res, pd.DataFrame) and not res.empty:
                successful_results.append(res)

        if not successful_results:
            logger.error("No financial data was successfully fetched.")
            return pd.DataFrame()

        combined_df = pd.concat(successful_results, ignore_index=True)
        logger.info(f"Combined financials data has {len(combined_df)} records.")
        return combined_df

    def _save_financials(self, financials: pd.DataFrame):
        """
        Save fetched financials to a Feather file.
        """
        if financials.empty:
            logger.warning("No financials data to save. Output file will not be created.")
            return

        try:
            FINANCIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
            financials.to_feather(FINANCIALS_FILE)
            logger.info(f"Financials saved to {FINANCIALS_FILE}")
        except Exception as e:
            logger.error(f"Error saving financials to {FINANCIALS_FILE}: {e}")
            logger.debug(f"Traceback:\n{traceback.format_exc()}")

    def _prune_top_stocks(self, financials: pd.DataFrame):
        """
        After fetching financials, remove any tickers from the top_stocks
        files that do not appear in the financials dataset.
        """
        if not TOP_STOCKS_FEATHER.exists() or not TOP_STOCKS_TICKERS_CSV.exists():
            logger.warning("Top stocks files not found. Skipping pruning step.")
            return

        try:
            top_stocks_df = pd.read_feather(TOP_STOCKS_FEATHER)
            tickers_df = pd.read_csv(TOP_STOCKS_TICKERS_CSV)

            financial_tickers = set(financials["ticker"].unique()) if not financials.empty else set()

            filtered_top_stocks_df = top_stocks_df[top_stocks_df["ticker"].isin(financial_tickers)].copy()
            filtered_tickers_df = tickers_df[tickers_df["ticker"].isin(financial_tickers)].copy()

            original_ticker_count = len(tickers_df["ticker"].unique())
            filtered_ticker_count = len(filtered_tickers_df["ticker"].unique())

            filtered_top_stocks_df.reset_index(drop=True).to_feather(TOP_STOCKS_FEATHER)
            filtered_tickers_df.to_csv(TOP_STOCKS_TICKERS_CSV, index=False)

            logger.info(
                f"Pruned top stocks and tickers: Reduced tickers from {original_ticker_count} to {filtered_ticker_count} after checking financials data."
            )

        except Exception as e:
            logger.error(f"Error pruning top stocks after financials fetch: {e}")
            logger.debug(f"Traceback:\n{traceback.format_exc()}")

    def run(self):
        if not self.tickers:
            logger.error("No tickers available to fetch financials.")
            return

        logger.info("Starting financials fetching workflow...")
        financials = asyncio.run(self._fetch_all_financials())
        self._save_financials(financials)
        self._prune_top_stocks(financials)
        logger.info("Financials fetching workflow completed.")


if __name__ == "__main__":
    fetcher = FinancialsFetcher()
    fetcher.run()
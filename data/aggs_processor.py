import pandas as pd
import logging
import asyncio
import httpx
from pathlib import Path
from config.settings import DATA_DIR, MARKET_PROXY, MONEY_MARKET_PROXY, POLYGON_API_KEY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

class AggregatesProcessor:
    def __init__(self, base_dir, output_path=None, top_n_tickers=1000):
        """
        Initialize the AggregatesProcessor.

        Parameters:
            base_dir (Path): Base directory containing input Feather files.
            output_path (Path): Path to save the processed Feather file.
            top_n_tickers (int): Number of top tickers to select by average daily volume.
        """
        self.base_dir = Path(base_dir)
        self.output_path = Path(DATA_DIR) / "processed_data.feather" if output_path is None else Path(output_path)
        self.top_n_tickers = top_n_tickers
        self.data = pd.DataFrame()

    def load_and_combine_data(self):
        """
        Load and combine data from Feather files in the base directory.
        """
        data_frames = []
        for file_path in self.base_dir.rglob("*.feather"):
            try:
                df = pd.read_feather(file_path)
                df["date"] = pd.to_datetime(file_path.stem, errors="coerce")  # Infer date from file name
                if df["date"].isna().all():
                    logger.warning(f"Invalid date format inferred from file name: {file_path.stem}")
                data_frames.append(df)
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
        self.data = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
        logger.info(f"Combined data shape: {self.data.shape}")

    def validate_data(self):
        """
        Validate that the dataset contains required columns.

        Returns: True if valid, False otherwise.
        """
        required_columns = {"ticker", "volume", "open", "close", "high", "low", "window_start", "transactions", "date"}
        missing_columns = required_columns - set(self.data.columns)
        if missing_columns:
            logger.error(f"Missing columns: {missing_columns}")
            return False
        logger.info(f"Data valid: No missing columns.")
        return True

    def clean_data(self):
        """
        Sanity check for data: check for missing values and exclude known test tickers.
        Logs the number of rows excluded for missing tickers and test ticker patterns.
        """
        # Check for missing numeric values
        numeric_cols = self.data.select_dtypes(include=["number"]).columns
        if self.data[numeric_cols].isnull().any().any():
            logger.error("Data is missing values. Clean failed.")
            return False

        # Initialize counts for logging
        initial_row_count = len(self.data)
        missing_ticker_count = 0
        test_ticker_excluded_count = 0

        if "ticker" in self.data.columns:
            # Handle missing or NaN tickers
            missing_ticker_count = self.data["ticker"].isna().sum()
            self.data = self.data[self.data["ticker"].notna()]  # Drop rows with NaN tickers

            # Exclude known test tickers using pattern matching
            test_ticker_pattern = r"^Z.*ZZT$"
            before_test_ticker_exclusion = len(self.data)
            self.data = self.data[~self.data["ticker"].str.match(test_ticker_pattern, na=False)]
            test_ticker_excluded_count = before_test_ticker_exclusion - len(self.data)

            # Log the results
            if missing_ticker_count > 0:
                logger.info(f"Excluded {missing_ticker_count} rows with missing or empty tickers.")
            if test_ticker_excluded_count > 0:
                logger.info(f"Excluded {test_ticker_excluded_count} rows with test tickers matching pattern '{test_ticker_pattern}'.")
            if missing_ticker_count == 0 and test_ticker_excluded_count == 0:
                logger.info("No rows excluded for missing or test tickers.")
        else:
            logger.warning("Column 'ticker' does not exist in the data. Skipping ticker-related exclusions.")

        # Log the overall cleaning result
        final_row_count = len(self.data)
        total_excluded_count = initial_row_count - final_row_count
        logger.info(f"Data cleaning completed: {total_excluded_count} rows excluded in total.")
        logger.info(f"Final dataset contains {final_row_count} rows.")
        return True

    async def fetch_ticker_type(self, session, ticker, max_retries=3):
        """
        Fetch the type of a ticker asynchronously. Treat anything not explicitly 'CS' as not 'CS'.
        Handles 404 Not Found by treating the ticker as non-'CS' without logging an error.
        Implements retry with exponential backoff for rate limits and network errors.

        Parameters:
            session (httpx.AsyncClient): The HTTP client session.
            ticker (str): The ticker symbol to fetch.
            max_retries (int): Maximum number of retry attempts.

        Returns:
            str or None: The ticker if it's of type 'CS', otherwise None.
        """
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
        params = {"apiKey": POLYGON_API_KEY}

        for attempt in range(1, max_retries + 1):
            try:
                # Hide API key in logs for security
                safe_params = params.copy()
                safe_params.pop("apiKey", None)
                logger.debug(f"Fetching type for ticker: {ticker} | URL: {url} | Params: {safe_params} | [API Key Hidden]")

                response = await session.get(url, params=params, timeout=10)

                if response.status_code == 200:
                    json_response = response.json()
                    results = json_response.get("results", {})
                    if isinstance(results, dict) and results:
                        ticker_type = results.get("type")
                        if ticker_type == "CS":
                            return ticker
                        else:
                            # Ticker exists but is not of type 'CS'
                            return None
                    else:
                        # Unexpected response format or empty results
                        return None

                elif response.status_code == 404:
                    # Ticker not found; treat as not 'CS' without logging as an error
                    return None

                elif response.status_code == 429:
                    logger.error(f"Rate limit exceeded when fetching ticker: {ticker}. Status Code: {response.status_code}")
                    if attempt < max_retries:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.info(f"Retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                        continue  # Retry the request
                    else:
                        logger.error(f"Max retries reached for ticker: {ticker}. Skipping.")
                        return None

                else:
                    logger.error(f"Failed to fetch ticker type for {ticker}. Status Code: {response.status_code} | Response Text: {response.text}")
                    return None

            except httpx.TimeoutException:
                logger.error(f"Request timed out when fetching ticker type for {ticker}.")
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    continue  # Retry the request
                else:
                    logger.error(f"Max retries reached for ticker: {ticker}. Skipping.")
                    return None

            except httpx.RequestError as e:
                logger.error(f"Network error occurred while fetching ticker type for {ticker}: {e}")
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    continue  # Retry the request
                else:
                    logger.error(f"Max retries reached for ticker: {ticker}. Skipping.")
                    return None

            except Exception as e:
                logger.error(f"Unexpected error fetching type for {ticker}: {e}")
                return None

        return None

    async def filter_top_n_to_common_stocks(self, ranked_tickers):
        """
        Filter ranked tickers to include only the top common stocks (CS).

        Parameters:
            ranked_tickers (list): List of tickers ranked by average daily volume.

        Returns:
            list: List of top common stock tickers.
        """
        common_stocks = []
        async with httpx.AsyncClient() as session:
            counter = 0
            for ticker in ranked_tickers:
                if len(common_stocks) >= self.top_n_tickers:
                    break  # Stop once we have enough common stocks

                result = await self.fetch_ticker_type(session, ticker)
                if result:
                    common_stocks.append(result)
                    counter += 1
                    logger.info(f"{ticker} is common stock {counter} out of {self.top_n_tickers}.")

        logger.info(f"Selected {len(common_stocks)} common stocks from ranked tickers.")
        return common_stocks

    def select_top_tickers(self):
        """
        Reduce to largest NUM_TICKERS average daily dollar volume (volume * avg price) common stocks.
        """
        if self.data.empty:
            logger.error("Aggregates data is empty. Cannot select top tickers.")
            return

        # Define the path for the ranked tickers Feather file
        ranked_tickers_path = self.output_path.parent / "ranked_tickers_cash_value.feather"

        # Rank tickers 
        ranked_tickers = []
        self.data["avg_price"] = (self.data["high"] + self.data["low"]) / 2
        self.data["cash_value"] = self.data["volume"] * self.data["avg_price"]
        ticker_stats = (
            self.data.groupby("ticker")["cash_value"]
            .agg(total_cash_value="sum", active_days="count")
            .reset_index()
        )
        ticker_stats["ADDV"] = ticker_stats["total_cash_value"] / ticker_stats["active_days"]
        ranked_tickers = ticker_stats.sort_values("ADDV", ascending=False)["ticker"].tolist()
        logger.info(f"Ranked {len(ranked_tickers)} tickers.")

        # Filter to top common stocks
        self.top_stocks = asyncio.run(self.filter_top_n_to_common_stocks(ranked_tickers))

    def save_processed_data(self):
        """
        Save the processed data into three Feather files:
        - Top stocks
        - Market proxy
        - Money market proxy
        """
        if not self.output_path:
            logger.error("Output path is not set. Cannot save processed data.")
            return

        # Define file paths
        top_stocks_path = self.output_path.parent / "top_stocks.feather"
        market_proxy_path = self.output_path.parent / "market_proxy.feather"
        money_market_proxy_path = self.output_path.parent / "money_market_proxy.feather"

        try:
            # Save top stocks
            if hasattr(self, "top_stocks") and self.top_stocks:
                # Save the full top stocks data as Feather
                top_stocks_df = self.data[self.data["ticker"].isin(self.top_stocks)]
                top_stocks_df.reset_index(drop=True).to_feather(top_stocks_path)
                logger.info(f"Top stocks saved to {top_stocks_path}")

                # Save the ticker symbols as an alphabetically ordered CSV
                top_stocks_tickers = sorted(self.top_stocks)
                tickers_csv_path = self.output_path.parent / "top_stocks_tickersymbols.csv"
                pd.DataFrame({"ticker": top_stocks_tickers}).to_csv(tickers_csv_path, index=False)
                logger.info(f"Top stock ticker symbols saved to {tickers_csv_path}")
            else:
                logger.error("Top stocks list is missing or empty. Skipping save for top stocks.")

            # Save market proxy
            market_proxy_df = self.data[self.data["ticker"] == MARKET_PROXY]
            if not market_proxy_df.empty:
                market_proxy_df.reset_index(drop=True).to_feather(market_proxy_path)
                logger.info(f"Market proxy saved to {market_proxy_path}")
            else:
                logger.error(f"No data found for market proxy '{MARKET_PROXY}'. Skipping save.")

            # Save money market proxy
            money_market_proxy_df = self.data[self.data["ticker"] == MONEY_MARKET_PROXY]
            if not money_market_proxy_df.empty:
                money_market_proxy_df.reset_index(drop=True).to_feather(money_market_proxy_path)
                logger.info(f"Money market proxy saved to {money_market_proxy_path}")
            else:
                logger.error(f"No data found for money market proxy '{MONEY_MARKET_PROXY}'. Skipping save.")

        except Exception as e:
            logger.error(f"Failed to save processed data: {e}")

    def process(self):
        """
        Run the complete processing pipeline. Checks if processed files already exist, 
        validates top stocks and proxy data before deciding to reprocess.
        """
        # Define file paths
        top_stocks_path = self.output_path.parent / "top_stocks.feather"
        market_proxy_path = self.output_path.parent / "market_proxy.feather"
        money_market_proxy_path = self.output_path.parent / "money_market_proxy.feather"

        # Check if processed data files exist
        if all(path.exists() for path in [top_stocks_path, market_proxy_path, money_market_proxy_path]):
            try:
                # Validate top stocks file
                top_stocks_data = pd.read_feather(top_stocks_path)
                unique_tickers = top_stocks_data["ticker"].nunique()
                if unique_tickers != self.top_n_tickers:
                    logger.warning(
                        f"Top stocks file has {unique_tickers} unique tickers but expected {self.top_n_tickers}. Reprocessing."
                    )
                    raise ValueError("Invalid top stocks file.")

                # Validate market proxy file
                market_proxy_data = pd.read_feather(market_proxy_path)
                if MARKET_PROXY not in market_proxy_data["ticker"].unique():
                    logger.warning(f"Market proxy file is missing the expected ticker '{MARKET_PROXY}'. Reprocessing.")
                    raise ValueError("Invalid market proxy file.")

                # Validate money market proxy file
                money_market_proxy_data = pd.read_feather(money_market_proxy_path)
                if MONEY_MARKET_PROXY not in money_market_proxy_data["ticker"].unique():
                    logger.warning(f"Money market proxy file is missing the expected ticker '{MONEY_MARKET_PROXY}'. Reprocessing.")
                    raise ValueError("Invalid money market proxy file.")

                # If all validations pass, skip processing
                logger.info(
                    f"Processed data already valid. {unique_tickers} unique tickers in top stocks and proxies "
                    f"'{MARKET_PROXY}' (market) and '{MONEY_MARKET_PROXY}' (money market) found. Skipping processing."
                )
                return

            except Exception as e:
                logger.warning(f"Validation of existing processed data failed. Reprocessing: {e}")

        # If data needs to be processed
        logger.info("Starting aggregates processing pipeline.")
        self.load_and_combine_data()

        if self.data.empty:
            logger.error("No data to process.")
            return

        if not self.validate_data():
            logger.error("Data invalid.")
            return

        if not self.clean_data():
            logger.error("Data not clean.")
            return

        self.select_top_tickers()
        self.save_processed_data()

        logger.info("Aggregates data processed.")
# data/aggs_processor.py

import pandas as pd
import logging
import asyncio
import httpx
from pathlib import Path
from config.settings import DATA_DIR, MARKET_PROXY, MONEY_MARKET_PROXY, POLYGON_API_KEY
from utils.logging_utils import configure_logging
from data.corporate_actions_adjuster import adjust_for_corporate_actions

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

class AggregatesProcessor:
    def __init__(self, base_dir, output_path=None, top_n_tickers=1000):
        self.base_dir = Path(base_dir)
        self.output_path = Path(DATA_DIR) / "processed_data.feather" if output_path is None else Path(output_path)
        self.top_n_tickers = top_n_tickers
        self.data = pd.DataFrame()
        self.top_stocks = []

    def load_and_combine_data(self):
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
        required_columns = {"ticker", "volume", "open", "close", "high", "low", "window_start", "transactions", "date"}
        missing_columns = required_columns - set(self.data.columns)
        if missing_columns:
            logger.error(f"Missing columns: {missing_columns}")
            return False
        logger.info("Data valid: No missing columns.")
        return True

    def clean_data(self):
        numeric_cols = self.data.select_dtypes(include=["number"]).columns
        if self.data[numeric_cols].isnull().any().any():
            logger.error("Data is missing values. Clean failed.")
            return False

        initial_row_count = len(self.data)
        if "ticker" in self.data.columns:
            missing_ticker_count = self.data["ticker"].isna().sum()
            self.data = self.data[self.data["ticker"].notna()]

            test_ticker_pattern = r"(^Z.*ZZT$|^[A-Z]+TEST\.G$)"
            before_test_ticker_exclusion = len(self.data)
            self.data = self.data[~self.data["ticker"].str.match(test_ticker_pattern, na=False)]
            test_ticker_excluded_count = before_test_ticker_exclusion - len(self.data)

            if missing_ticker_count > 0:
                logger.info(f"Excluded {missing_ticker_count} rows with missing or empty tickers.")
            if test_ticker_excluded_count > 0:
                logger.info(f"Excluded {test_ticker_excluded_count} rows with test tickers '{test_ticker_pattern}'.")
            if missing_ticker_count == 0 and test_ticker_excluded_count == 0:
                logger.info("No rows excluded for missing or test tickers.")
        else:
            logger.warning("Column 'ticker' does not exist in the data. Skipping ticker-related exclusions.")

        final_row_count = len(self.data)
        total_excluded_count = initial_row_count - final_row_count
        logger.info(f"Data cleaning completed: {total_excluded_count} rows excluded in total.")
        logger.info(f"Final dataset contains {final_row_count} rows.")
        return True

    async def fetch_ticker_type(self, session, ticker, max_retries=3):
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
        params = {"apiKey": POLYGON_API_KEY}

        for attempt in range(1, max_retries + 1):
            try:
                response = await session.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    json_response = response.json()
                    results = json_response.get("results", {})
                    if isinstance(results, dict) and results:
                        ticker_type = results.get("type")
                        if ticker_type == "CS":
                            return ticker
                        else:
                            return None
                    else:
                        return None
                elif response.status_code == 404:
                    return None
                elif response.status_code == 429:
                    logger.error(f"Rate limit exceeded for {ticker}. Attempt {attempt}.")
                    if attempt < max_retries:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        return None
                else:
                    logger.error(f"Failed to fetch ticker type for {ticker}, status {response.status_code}.")
                    return None
            except (httpx.TimeoutException, httpx.RequestError) as e:
                logger.error(f"Network error for {ticker}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return None
            except Exception as e:
                logger.error(f"Unexpected error fetching type for {ticker}: {e}")
                return None
        return None

    async def filter_top_n_to_common_stocks(self, ranked_tickers):
        common_stocks = []
        async with httpx.AsyncClient() as session:
            for ticker in ranked_tickers:
                if len(common_stocks) >= self.top_n_tickers:
                    break
                result = await self.fetch_ticker_type(session, ticker)
                if result:
                    common_stocks.append(result)
                    logger.info(f"{ticker} is common stock {len(common_stocks)} of {self.top_n_tickers}.")
        logger.info(f"Selected {len(common_stocks)} common stocks.")
        return common_stocks

    def select_top_tickers(self):
        if self.data.empty:
            logger.error("Aggregates data is empty. Cannot select top tickers.")
            return
        
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

        self.top_stocks = asyncio.run(self.filter_top_n_to_common_stocks(ranked_tickers))

        # Ensure proxies are retained if they exist in the dataset
        for proxy in [MARKET_PROXY, MONEY_MARKET_PROXY]:
            if proxy in self.data["ticker"].unique() and proxy not in self.top_stocks:
                self.top_stocks.append(proxy)

        # Now reduce self.data to just the top_stocks and proxies
        if self.top_stocks:
            self.data = self.data[self.data["ticker"].isin(self.top_stocks)].copy()
            logger.info(f"Reduced self.data to {len(self.data)} rows for top {len(self.top_stocks)} tickers (including proxies).")
        else:
            logger.error("No top stocks selected. Data remains unchanged.")

    def adjust_for_corporate_actions(self):
        logger.info("Adjusting data for corporate actions...")
        self.data = adjust_for_corporate_actions(self.data)
        logger.info("Corporate actions adjustments completed.")

    def save_processed_data(self):
        if not self.output_path:
            logger.error("Output path is not set. Cannot save processed data.")
            return

        # Separate proxies and top_stocks
        proxies_mask = self.data["ticker"].isin([MARKET_PROXY, MONEY_MARKET_PROXY])
        proxies_df = self.data[proxies_mask].copy()
        top_stocks_df = self.data[~proxies_mask].copy()

        # Save top stocks
        top_stocks_path = self.output_path.parent / "top_stocks.feather"
        top_stocks_df.reset_index(drop=True).to_feather(top_stocks_path)
        logger.info(f"Top stocks saved to {top_stocks_path}")

        # Save ticker symbols for top stocks
        top_stocks_tickers = sorted(top_stocks_df["ticker"].unique().tolist())
        tickers_csv_path = self.output_path.parent / "top_stocks_tickersymbols.csv"
        pd.DataFrame({"ticker": top_stocks_tickers}).to_csv(tickers_csv_path, index=False)
        logger.info(f"Top stock ticker symbols saved to {tickers_csv_path}")

        # Save proxies
        proxies_path = self.output_path.parent / "proxies.feather"
        proxies_df.reset_index(drop=True).to_feather(proxies_path)
        logger.info(f"Proxies (market and money market) saved to {proxies_path}")

    def process(self):
        top_stocks_path = self.output_path.parent / "top_stocks.feather"
        proxies_path = self.output_path.parent / "proxies.feather"

        # If processed data files exist, validate them
        if top_stocks_path.exists() and proxies_path.exists():
            try:
                top_stocks_data = pd.read_feather(top_stocks_path)
                proxies_data = pd.read_feather(proxies_path)
                unique_tickers = top_stocks_data["ticker"].nunique()

                # Check if number of top stocks matches expected
                # Remember we might have fewer if no full top_n_tickers were found
                # but let's not raise errors here unnecessarily.
                # Instead, just log a warning if less than top_n_tickers found.
                if unique_tickers != self.top_n_tickers:
                    logger.warning(
                        f"Top stocks file has {unique_tickers} tickers; expected {self.top_n_tickers}. "
                        f"This may be due to availability. Reprocessing may be needed."
                    )

                # Check if proxies exist
                proxies_tickers = proxies_data["ticker"].unique()
                if MARKET_PROXY not in proxies_tickers:
                    logger.warning(f"Missing market proxy '{MARKET_PROXY}' in proxies. Reprocessing.")
                    raise ValueError("Invalid proxies file.")
                if MONEY_MARKET_PROXY not in proxies_tickers:
                    logger.warning(f"Missing money market proxy '{MONEY_MARKET_PROXY}' in proxies. Reprocessing.")
                    raise ValueError("Invalid proxies file.")

                logger.info(
                    f"Processed data already available. Found {unique_tickers} top stocks and "
                    f"proxies {MARKET_PROXY}, {MONEY_MARKET_PROXY} found. Skipping processing."
                )
                return
            except Exception as e:
                logger.warning(f"Validation of existing processed data failed. Reprocessing: {e}")

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
        self.adjust_for_corporate_actions()
        self.save_processed_data()

        logger.info("Aggregates data processed.")
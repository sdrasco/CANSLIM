import os
import pandas as pd
import numpy as np
import logging
import httpx
import asyncio
from pathlib import Path
from config.settings import DATA_DIR, START_DATE, END_DATE, POLYGON_API_KEY
from config.configure_logging import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, base_dir, output_path=None):
        self.base_dir = base_dir
        self.output_path = output_path
        self.data = pd.DataFrame()

    def load_and_combine_data(self):
        data_frames = []
        for file_path in Path(self.base_dir).rglob("*.feather"):
            df = self.load_file(file_path)
            if not df.empty:
                data_frames.append(df)
        self.data = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
        logger.info(f"Combined data shape: {self.data.shape}")

    def load_file(self, file_path):
        try:
            df = pd.read_feather(file_path)
            df['date'] = pd.to_datetime(file_path.stem)  # e.g., '2024-10-01'
            return df
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()

    def validate_data(self):
        required_columns = {'date', 'ticker', 'open', 'high', 'low', 'close', 'volume'}
        missing_columns = required_columns - set(self.data.columns)
        if missing_columns:
            logger.error(f"Missing columns: {missing_columns}")
            return False
        return True

    def clean_data(self):
        if not self.validate_data():
            logger.error("Data validation failed. Exiting cleaning step.")
            self.data = pd.DataFrame()
            return
        self.data.drop_duplicates(inplace=True)
        self.data.ffill(inplace=True)
        self.data.bfill(inplace=True)
        logger.info("Data cleaning completed.")

    def feature_engineering(self):
        if self.data.empty:
            logger.warning("No data for feature engineering.")
            return
        self.data.sort_values(by=['ticker', 'date'], inplace=True)
        self.data['50_MA'] = self.data.groupby('ticker')['close'].transform(lambda x: x.rolling(window=50).mean())
        self.data['200_MA'] = self.data.groupby('ticker')['close'].transform(lambda x: x.rolling(window=200).mean())
        self.data['50_Vol_Avg'] = self.data.groupby('ticker')['volume'].transform(lambda x: x.rolling(window=50).mean())
        self.data['Price_Change'] = self.data.groupby('ticker')['close'].pct_change().fillna(0)
        logger.info("Feature engineering completed.")

    async def fetch_data_for_ticker(self, ticker, endpoint, params, client):
        url = f"https://api.polygon.io{endpoint}"
        params["ticker"] = ticker
        params["apiKey"] = POLYGON_API_KEY
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data["results"]) if "results" in data else pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching {endpoint} for {ticker}: {e}")
            return pd.DataFrame()

    async def fetch_all_data(self, tickers, endpoint, params, max_concurrent_requests=100):
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        async with httpx.AsyncClient() as client:
            async def sem_fetch(ticker):
                async with semaphore:
                    return await self.fetch_data_for_ticker(ticker, endpoint, params, client)
            tasks = [sem_fetch(ticker) for ticker in tickers]
            return dict(zip(tickers, await asyncio.gather(*tasks, return_exceptions=True)))

    async def fetch_splits(self, tickers, max_concurrent_requests=10):
        return await self.fetch_all_data(tickers, "/v3/reference/splits", {"execution_date.gte": START_DATE, "execution_date.lte": END_DATE}, max_concurrent_requests)

    async def fetch_name_changes(self, tickers, max_concurrent_requests=10):
        """
        Fetch ticker name changes for multiple tickers concurrently with semaphore control.

        Parameters:
            tickers (list): List of ticker symbols.
            max_concurrent_requests (int): Maximum number of concurrent requests.

        Returns:
            dict: A dictionary mapping tickers to their fetched name change DataFrames.
        """
        semaphore = asyncio.Semaphore(max_concurrent_requests)

        async with httpx.AsyncClient() as client:
            async def sem_fetch_name_change(ticker):
                async with semaphore:
                    url = f"https://api.polygon.io/vX/reference/tickers/{ticker}/events?apiKey={POLYGON_API_KEY}"
                    try:
                        response = await client.get(url)
                        if response.status_code == 404:
                            # 404 is expected for tickers with no name changes
                            return pd.DataFrame()

                        response.raise_for_status()
                        data = response.json()

                        # Only log and return meaningful results
                        if data.get("results"):
                            valid_results = [
                                result for result in data["results"]
                                if all(key in result for key in ["old_ticker", "new_ticker", "event_date"])
                            ]
                            if valid_results:
                                logger.info(f"Found name changes for ticker {ticker}.")
                                return pd.DataFrame(valid_results)

                    except Exception as e:
                        logger.error(f"Unexpected error fetching name changes for {ticker}: {e}")

                    # Ignore tickers without valid name changes
                    return pd.DataFrame()

            tasks = [sem_fetch_name_change(ticker) for ticker in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        return dict(zip(tickers, results))

    async def fetch_dividends(self, tickers, max_concurrent_requests=10):
        return await self.fetch_all_data(tickers, "/v3/reference/dividends", {"ex_dividend_date.gte": START_DATE, "ex_dividend_date.lte": END_DATE}, max_concurrent_requests)

    def apply_splits(self, splits_dict):
        """
        Apply stock splits to the dataset.

        Parameters:
            splits_dict (dict): Dictionary of ticker splits dataframes.
        """
        for ticker, splits in splits_dict.items():
            if splits.empty:
                continue
            splits = splits.sort_values(by="execution_date", ascending=False)
            for _, split in splits.iterrows():
                split_date = pd.to_datetime(split['execution_date'])
                split_factor = float(split['split_to']) / float(split['split_from'])
                mask = (self.data['ticker'] == ticker) & (self.data['date'] < split_date)

                # Adjust volume and price columns
                self.data.loc[mask, 'volume'] = (self.data.loc[mask, 'volume'] / split_factor).round().astype('int64')
                self.data.loc[mask, ['open', 'high', 'low', 'close']] *= split_factor

                logger.info(f"Applied split for {ticker}: {split['split_from']} for {split['split_to']} on {split['execution_date']}.")

    def apply_name_changes(self, name_changes_dict):
        """
        Apply ticker name changes to the dataset.

        Parameters:
            name_changes_dict (dict): Dictionary of ticker name changes dataframes.
        """
        for ticker, name_changes in name_changes_dict.items():
            if name_changes.empty:
                continue
            for _, change in name_changes.iterrows():
                old_ticker = change['old_ticker']
                new_ticker = change['new_ticker']
                change_date = pd.to_datetime(change['event_date'])

                # Update ticker in the dataset
                mask = (self.data['ticker'] == old_ticker) & (self.data['date'] >= change_date)
                self.data.loc[mask, 'ticker'] = new_ticker

                logger.info(f"Applied ticker name change: {old_ticker} to {new_ticker} on {change_date}.")

    def apply_dividends(self, dividends_dict):
        """
        Apply dividend adjustments to the dataset.

        Parameters:
            dividends_dict (dict): Dictionary of ticker dividends dataframes.
        """
        for ticker, dividends in dividends_dict.items():
            if dividends.empty:
                continue
            for _, dividend in dividends.iterrows():
                ex_date = pd.to_datetime(dividend['ex_dividend_date'])
                dividend_amount = dividend['cash_amount']

                # Adjust prices before the ex-dividend date
                mask = (self.data['ticker'] == ticker) & (self.data['date'] < ex_date)
                self.data.loc[mask, ['open', 'high', 'low', 'close']] -= dividend_amount

                logger.info(f"Applied dividend adjustment for {ticker}: {dividend_amount} on {ex_date}.")

    def adjust_for_all_corporate_actions(self, batch_size=500, max_concurrent_requests=30):
        if self.data.empty:
            logger.warning("No data for corporate actions.")
            return
        tickers = np.sort(self.data['ticker'].unique())
        logger.info(f"Adjusting for {len(tickers)} tickers in batches of {batch_size}...")
        for start in range(0, len(tickers), batch_size):

            batch_tickers = tickers[start:start + batch_size]

            # Gather results by awaiting the coroutines
            async def gather_tasks():
                return await asyncio.gather(
                    self.fetch_splits(batch_tickers, max_concurrent_requests),
                    self.fetch_name_changes(batch_tickers, max_concurrent_requests),
                    self.fetch_dividends(batch_tickers, max_concurrent_requests),
                )

            splits, name_changes, dividends = asyncio.run(gather_tasks())

            # Apply adjustments
            self.apply_splits(splits)
            self.apply_name_changes(name_changes)
            self.apply_dividends(dividends)

            # Move logging here, inside the loop
            logger.info(f"Completed batch {start + 1} to {start + len(batch_tickers)}.")

        logger.info("Corporate actions adjustment complete.")

    def save_processed_data(self):
        if self.output_path:
            self.data.reset_index(drop=True).to_feather(self.output_path)
            logger.info(f"Data saved to {self.output_path}")

    def process(self):
        self.load_and_combine_data()
        if self.data.empty:
            logger.error("No data to process.")
            return
        self.clean_data()
        self.adjust_for_all_corporate_actions()
        self.feature_engineering()
        self.save_processed_data()
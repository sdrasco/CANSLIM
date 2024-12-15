# data/aggs_processor.py

import pandas as pd
import logging
from pathlib import Path
from config.settings import DATA_DIR, MARKET_PROXY, MONEY_MARKET_PROXY, START_DATE, END_DATE
from utils.logging_utils import configure_logging
from data.corporate_actions_adjuster import adjust_for_corporate_actions

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

SP500_SNAPSHOT_FILE = DATA_DIR / "sp_500_historic_snapshot.feather"

class AggregatesProcessor:
    def __init__(self, base_dir, output_path=None):
        self.base_dir = Path(base_dir)
        self.output_path = Path(DATA_DIR) / "processed_data.feather" if output_path is None else Path(output_path)
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

            if missing_ticker_count > 0:
                logger.info(f"Excluded {missing_ticker_count} rows with missing or empty tickers.")

        else:
            logger.warning("Column 'ticker' does not exist in the data. Skipping ticker-related exclusions.")

        final_row_count = len(self.data)
        total_excluded_count = initial_row_count - final_row_count
        logger.info(f"Data cleaning completed: {total_excluded_count} rows excluded in total.")
        logger.info(f"Final dataset contains {final_row_count} rows.")
        return True

    def select_top_stocks_from_sp500(self):
        """
        Load sp_500_historic_snapshot.feather, filter by [START_DATE, END_DATE],
        and collect all tickers that appear on any date in that range.
        """

        snapshot_path = SP500_SNAPSHOT_FILE
        if not snapshot_path.exists():
            logger.error(f"S&P 500 historic snapshot file not found: {snapshot_path}")
            return

        try:
            snap_df = pd.read_feather(snapshot_path)

            # Ensure date is a proper datetime type
            # If it's already datetime64, this line won't hurt.
            snap_df['date'] = pd.to_datetime(snap_df['date'], errors='coerce')

            # Convert START_DATE and END_DATE (which might be datetime.date) to Timestamp
            start_ts = pd.Timestamp(START_DATE)
            end_ts = pd.Timestamp(END_DATE)

            # Filter by date range
            # Now both snap_df['date'] and start_ts/end_ts are Timestamps
            snap_df = snap_df[(snap_df["date"] >= start_ts) & (snap_df["date"] <= end_ts)]

            if snap_df.empty:
                logger.warning("No S&P 500 data in the given backtest range. Selecting no top stocks.")
                self.top_stocks = []
                return

            # Collect all tickers from these rows
            all_tickers = set()
            for _, row in snap_df.iterrows():
                day_tickers_str = row["tickers"]
                if pd.isna(day_tickers_str):
                    continue
                day_tickers = day_tickers_str.split(",")
                day_tickers = [t.strip() for t in day_tickers if t.strip() != ""]
                all_tickers.update(day_tickers)

            self.top_stocks = list(all_tickers)

            # Ensure proxies are included if present in the dataset
            for proxy in [MARKET_PROXY, MONEY_MARKET_PROXY]:
                if proxy in self.data["ticker"].unique() and proxy not in self.top_stocks:
                    self.top_stocks.append(proxy)

            self.data = self.data[self.data["ticker"].isin(self.top_stocks)].copy()
            logger.info(f"Selected {len(self.top_stocks)} tickers from S&P 500 snapshot within backtest period.")
            logger.info(f"Reduced self.data to {len(self.data)} rows for these tickers.")
        except Exception as e:
            logger.error(f"Error processing S&P 500 snapshot: {e}")

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

                if MARKET_PROXY not in proxies_data["ticker"].unique():
                    logger.warning(f"Missing market proxy '{MARKET_PROXY}' in proxies. Reprocessing.")
                    raise ValueError("Invalid proxies file.")
                if MONEY_MARKET_PROXY not in proxies_data["ticker"].unique():
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

        self.select_top_stocks_from_sp500()
        self.adjust_for_corporate_actions()
        self.save_processed_data()

        logger.info("Aggregates data processed.")
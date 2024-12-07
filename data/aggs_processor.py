# data.aggs_processor

import pandas as pd
import logging
from pathlib import Path
from config.settings import DATA_DIR
from config.configure_logging import configure_logging

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
        self.output_path = output_path
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
                df["date"] = pd.to_datetime(file_path.stem)  # Infer date from file name
                data_frames.append(df)
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
        self.data = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
        logger.info(f"Combined data shape: {self.data.shape}")

    def validate_data(self):
        """
        Validate that the dataset contains required columns.

        Returns:
            bool: True if valid, False otherwise.
        """
        required_columns = {"date", "ticker", "volume", "open", "high", "low", "close"}
        missing_columns = required_columns - set(self.data.columns)
        if missing_columns:
            logger.error(f"Missing columns: {missing_columns}")
            return False
        return True

    def clean_data(self):
        """
        Clean the dataset by handling duplicates and filling missing values.
        """
        if not self.validate_data():
            logger.error("Data validation failed. Exiting cleaning step.")
            self.data = pd.DataFrame()
            return
        self.data.drop_duplicates(inplace=True)
        self.data.ffill(inplace=True)
        self.data.bfill(inplace=True)
        logger.info("Data cleaning completed.")

    def select_top_tickers_by_avg_volume(self):
        """
        Select the top N tickers by average daily volume and filter the dataset.

        This modifies the `self.data` attribute to contain only the selected tickers.
        """
        if self.data.empty:
            logger.error("Aggregates data is empty. Cannot select top tickers.")
            return
        ticker_stats = (
            self.data.groupby("ticker")["volume"]
            .agg(total_volume="sum", active_days="count")
            .reset_index()
        )
        ticker_stats["avg_daily_volume"] = ticker_stats["total_volume"] / ticker_stats["active_days"]
        top_tickers = ticker_stats.nlargest(self.top_n_tickers, "avg_daily_volume")["ticker"]
        self.data = self.data[self.data["ticker"].isin(top_tickers)]
        logger.info(f"Selected top {self.top_n_tickers} tickers by average daily volume.")

    def save_processed_data(self):
        """
        Save the processed data to a Feather file.
        """
        if self.output_path:
            self.data.reset_index(drop=True).to_feather(self.output_path)
            logger.info(f"Processed data saved to {self.output_path}")

    def process(self):
        """
        Run the complete processing pipeline.
        """
        self.load_and_combine_data()
        if self.data.empty:
            logger.error("No data to process.")
            return
        self.clean_data()
        self.select_top_tickers_by_avg_volume()
        self.save_processed_data()
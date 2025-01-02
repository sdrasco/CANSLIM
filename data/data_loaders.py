# data/data_loaders.py

import logging
import pandas as pd
from utils.logging_utils import configure_logging
from config.settings import DATA_DIR

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def load_proxies():
    """
    Load the proxies data (e.g., SPY and BIL) from a single feather file called proxies.feather.
    This file should contain a 'ticker' column to differentiate the MARKET_PROXY and MONEY_MARKET_PROXY.
    Returns a pandas DataFrame with columns such as:
    date, ticker, close, and any other relevant columns.
    """
    proxies_path = DATA_DIR / "proxies.feather"
    if not proxies_path.exists():
        logger.error(f"Proxies file not found: {proxies_path}")
        return pd.DataFrame()

    df = pd.read_feather(proxies_path)
    logger.info(f"Loaded proxies data from {proxies_path}, shape: {df.shape}")

    # Ensure expected columns are present (e.g., 'date', 'ticker', 'close')
    required_cols = {"date", "ticker", "close"}
    missing = required_cols - set(df.columns)
    if missing:
        logger.error(f"Proxies data missing required columns: {missing}")
        return pd.DataFrame()

    # set date to index
    df = convert_date_to_index(df)

    return df

def load_top_stocks():
    """
    Load the top stocks data from feather file and reindex by date as DatetimeIndex.
    """
    top_stocks_path = DATA_DIR / "top_stocks.feather"
    if not top_stocks_path.exists():
        logger.error(f"Top stocks file not found: {top_stocks_path}")
        return pd.DataFrame()

    # read in the file
    df = pd.read_feather(top_stocks_path)

    # set date to index
    df = convert_date_to_index(df)

    # log results
    logger.info(f"Loaded top stocks data from {top_stocks_path}, shape: {df.shape}, index type: {type(df.index)}")
    return df

def load_financials():
    """
    Load the financials data from feather file.
    Returns a pandas DataFrame that includes start_date, end_date, filing_date,
    timeframe, fiscal_period, fiscal_year, etc.
    """
    financials_path = DATA_DIR / "financials.feather"
    if not financials_path.exists():
        logger.error(f"Financials file not found: {financials_path}")
        return pd.DataFrame()

    df = pd.read_feather(financials_path)
    logger.info(f"Loaded financials data from {financials_path}, shape: {df.shape}")
    return df

def convert_date_to_index(df):
    # Convert date column to actual datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    # limit dates to the day (we don't do anything intraday).
    df["date"] = df["date"].dt.normalize()

    # Now set it as the index
    df = df.set_index("date", drop=True).sort_index()
    return df

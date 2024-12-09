# data/data_loaders.py

import logging
import pandas as pd
from config.settings import DATA_DIR

logger = logging.getLogger(__name__)

def load_market_proxy():
    """
    Load the market proxy data (e.g., SPY) from feather file.
    Returns a pandas DataFrame with columns such as:
    date, close, (and after calculations) M, 50_MA, 200_MA.
    """
    market_proxy_path = DATA_DIR / "market_proxy.feather"
    if not market_proxy_path.exists():
        logger.error(f"Market proxy file not found: {market_proxy_path}")
        return pd.DataFrame()

    df = pd.read_feather(market_proxy_path)
    logger.info(f"Loaded market proxy data from {market_proxy_path}, shape: {df.shape}")
    return df


def load_top_stocks():
    """
    Load the top stocks data from feather file.
    Returns a pandas DataFrame with daily price, volume, and CANSLI columns.
    """
    top_stocks_path = DATA_DIR / "top_stocks.feather"
    if not top_stocks_path.exists():
        logger.error(f"Top stocks file not found: {top_stocks_path}")
        return pd.DataFrame()

    df = pd.read_feather(top_stocks_path)
    logger.info(f"Loaded top stocks data from {top_stocks_path}, shape: {df.shape}")
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
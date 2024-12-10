# utils/calendar_utils.py

import logging
from datetime import date
import pandas as pd
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def get_quarter_end_dates(financials_df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Extract quarter-end dates from the financials for the given ticker.
    Assumes that each quarterly record in financials_df has an 'end_date' column 
    indicating the end of that fiscal period.

    Parameters:
        financials_df (pd.DataFrame): The financials data.
        ticker (str): The ticker symbol to filter for (e.g., "AAPL").

    Returns:
        pd.DataFrame: A DataFrame with at least one column 'end_date' (date) for each quarter end.
    """
    if "timeframe" not in financials_df.columns or "end_date" not in financials_df.columns:
        logger.error("financials_df missing required columns 'timeframe' or 'end_date'.")
        return pd.DataFrame()

    # Filter to the given ticker and quarterly data only
    df = financials_df[(financials_df["ticker"] == ticker) & (financials_df["timeframe"] == "quarterly")].copy()
    if df.empty:
        logger.warning(f"No quarterly financials found for ticker {ticker}.")
        return pd.DataFrame()

    # end_date should indicate the quarter-end date
    # Ensure end_date is a proper date/datetime type if not already
    if not pd.api.types.is_datetime64_any_dtype(df["end_date"]):
        df["end_date"] = pd.to_datetime(df["end_date"])

    # Keep only unique end_dates and sort
    quarter_end_dates = df["end_date"].drop_duplicates().sort_values().reset_index(drop=True)

    return quarter_end_dates.to_frame(name="end_date")


def get_rebalance_dates(market_proxy_df: pd.DataFrame, quarter_end_dates: pd.DataFrame) -> list:
    """
    Given a list of quarter-end dates and the market proxy's trading calendar (market_proxy_df),
    find the last trading day on or before each quarter-end. These will be the rebalancing dates.

    Parameters:
        market_proxy_df (pd.DataFrame): The market proxy data with a 'date' column (daily trading days).
        quarter_end_dates (pd.DataFrame): A DataFrame with a column 'end_date'.

    Returns:
        list: A list of rebalancing dates as Python date objects.
    """
    if "date" not in market_proxy_df.columns:
        logger.error("market_proxy_df missing 'date' column.")
        return []

    if not pd.api.types.is_datetime64_any_dtype(market_proxy_df["date"]):
        market_proxy_df["date"] = pd.to_datetime(market_proxy_df["date"])

    if "end_date" not in quarter_end_dates.columns:
        logger.error("quarter_end_dates DataFrame missing 'end_date' column.")
        return []

    # Sort market_proxy_df by date
    market_proxy_df = market_proxy_df.sort_values("date").reset_index(drop=True)

    rebalance_dates = []
    for ed in quarter_end_dates["end_date"]:
        # Find the last trading day <= end_date
        # We can do this by filtering market_proxy_df
        potential = market_proxy_df[market_proxy_df["date"] <= ed]
        if potential.empty:
            # If no trading day <= this end_date, then skip or take the earliest available?
            logger.warning(f"No trading day found on or before {ed}. Using earliest available day.")
            earliest = market_proxy_df["date"].iloc[0]
            rebalance_dates.append(earliest.date())
        else:
            last_trading_day = potential["date"].iloc[-1]
            rebalance_dates.append(last_trading_day.date())

    return rebalance_dates
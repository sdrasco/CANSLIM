# utils/calendar_utils.py

import logging
import pandas as pd
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def get_quarter_end_dates(financials_df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    (Unchanged)
    Extract quarter-end dates from the financials for the given ticker.
    """
    if "timeframe" not in financials_df.columns or "end_date" not in financials_df.columns:
        logger.error("financials_df missing required columns 'timeframe' or 'end_date'.")
        return pd.DataFrame()

    df = financials_df[(financials_df["ticker"] == ticker) & (financials_df["timeframe"] == "quarterly")].copy()
    if df.empty:
        logger.warning(f"No quarterly financials found for ticker {ticker}.")
        return pd.DataFrame()

    if not pd.api.types.is_datetime64_any_dtype(df["end_date"]):
        df["end_date"] = pd.to_datetime(df["end_date"])

    quarter_end_dates = df["end_date"].drop_duplicates().sort_values().reset_index(drop=True)
    return quarter_end_dates.to_frame(name="end_date")


def get_rebalance_dates(market_proxy_df: pd.DataFrame, frequency: str = "quarterly",
                        start_date=None, end_date=None) -> list:
    """
    Generate rebalance dates based on a given frequency and the trading calendar provided by market_proxy_df,
    restricted to the given start_date and end_date if provided.

    Supported frequencies:
    - "daily": Rebalance on every trading day
    - "weekly": Rebalance on the last trading day of each ISO week
    - "monthly": Rebalance on the last trading day of each month
    - "quarterly": Rebalance on the last trading day of each quarter
    - "yearly": Rebalance on the last trading day of each year

    If an unknown frequency is provided, defaults to quarterly.

    Parameters:
        market_proxy_df (pd.DataFrame): DataFrame with a 'date' column (trading days).
        frequency (str): The frequency at which to rebalance.
        start_date (date, optional): The start date of the backtest period.
        end_date (date, optional): The end date of the backtest period.

    Returns:
        list: A list of rebalance dates as Python date objects.
    """
    if "date" not in market_proxy_df.columns:
        logger.error("market_proxy_df missing 'date' column.")
        return []

    if not pd.api.types.is_datetime64_any_dtype(market_proxy_df["date"]):
        market_proxy_df["date"] = pd.to_datetime(market_proxy_df["date"])

    # Filter the market_proxy_df to the specified start and end dates if provided
    if start_date is not None:
        market_proxy_df = market_proxy_df[market_proxy_df["date"] >= pd.to_datetime(start_date)]
    if end_date is not None:
        market_proxy_df = market_proxy_df[market_proxy_df["date"] <= pd.to_datetime(end_date)]

    if market_proxy_df.empty:
        logger.warning("No trading days found in the specified period. No rebalance dates generated.")
        return []

    # Sort by date to ensure chronological order
    market_proxy_df = market_proxy_df.sort_values("date").reset_index(drop=True)

    frequency = frequency.lower()

    if frequency == "daily":
        # Rebalance every trading day
        return market_proxy_df["date"].dt.date.tolist()

    # For other frequencies, we need grouping
    market_proxy_df["year"] = market_proxy_df["date"].dt.year

    if frequency == "weekly":
        iso_info = market_proxy_df["date"].dt.isocalendar()
        market_proxy_df["week"] = iso_info.week
        group_cols = ["year", "week"]

    elif frequency == "monthly":
        market_proxy_df["month"] = market_proxy_df["date"].dt.month
        group_cols = ["year", "month"]

    elif frequency == "yearly":
        group_cols = ["year"]

    else:
        # Default to quarterly
        market_proxy_df["quarter"] = ((market_proxy_df["date"].dt.month - 1) // 3) + 1
        group_cols = ["year", "quarter"]

    grouped = market_proxy_df.groupby(group_cols, as_index=False)["date"].max()
    rebalance_dates = [d.date() for d in grouped["date"]]

    return rebalance_dates
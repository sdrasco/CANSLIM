# utils/calendar_utils.py

import logging
import pandas as pd
from utils.logging_utils import configure_logging

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

    df = financials_df[
        (financials_df["ticker"] == ticker) & (financials_df["timeframe"] == "quarterly")
    ].copy()
    if df.empty:
        logger.warning(f"No quarterly financials found for ticker {ticker}.")
        return pd.DataFrame()

    if not pd.api.types.is_datetime64_any_dtype(df["end_date"]):
        df["end_date"] = pd.to_datetime(df["end_date"])

    quarter_end_dates = df["end_date"].drop_duplicates().sort_values().reset_index(drop=True)
    return quarter_end_dates.to_frame(name="end_date")


def get_rebalance_dates(
    market_proxy_df: pd.DataFrame,
    frequency: str = "quarterly",
    start_date=None,
    end_date=None
) -> list:
    """
    Generate rebalance dates based on a given frequency and the trading calendar 
    provided by market_proxy_df, restricted to the given start_date and end_date if provided.

    The market_proxy_df is expected to have a DatetimeIndex representing trading days.

    Supported frequencies:
      - "daily"
      - "weekly"
      - "monthly"
      - "quarterly" (default)
      - "yearly"

    Returns:
      list: A list of rebalance dates as Python date objects.
    """
    # Check for DatetimeIndex
    if not isinstance(market_proxy_df.index, pd.DatetimeIndex):
        logger.error(
            "market_proxy_df must have a DatetimeIndex for get_rebalance_dates. "
            "Aborting."
        )
        return []

    # Floor timestamps to midnight (in case they're not already)
    market_proxy_df = market_proxy_df.copy()  # avoid mutating the caller's DataFrame
    market_proxy_df.index = market_proxy_df.index.floor("D")

    # Filter to start_date and end_date via the index
    if start_date is not None:
        market_proxy_df = market_proxy_df.loc[market_proxy_df.index >= pd.to_datetime(start_date)]
    if end_date is not None:
        market_proxy_df = market_proxy_df.loc[market_proxy_df.index <= pd.to_datetime(end_date)]

    if market_proxy_df.empty:
        logger.warning("No trading days found in the specified period. No rebalance dates generated.")
        return []

    # Convert the (filtered, floored) DatetimeIndex to a Series so we can group by year, month, etc.
    date_series = market_proxy_df.index.to_series().sort_values().rename("date")

    freq = frequency.lower()

    if freq == "daily":
        # Rebalance every trading day (simply return all trading dates)
        return date_series.dt.date.tolist()

    # Prepare columns for grouping
    # For weekly, use isocalendar() for year/week
    # For monthly, use year/month
    # For quarterly, use year/quarter
    # For yearly, just year
    df_dates = pd.DataFrame(date_series)
    df_dates["year"] = df_dates["date"].dt.year

    if freq == "weekly":
        iso_info = df_dates["date"].dt.isocalendar()
        df_dates["week"] = iso_info.week
        group_cols = ["year", "week"]

    elif freq == "monthly":
        df_dates["month"] = df_dates["date"].dt.month
        group_cols = ["year", "month"]

    elif freq == "yearly":
        group_cols = ["year"]

    else:
        # Default to quarterly
        df_dates["quarter"] = ((df_dates["date"].dt.month - 1) // 3) + 1
        group_cols = ["year", "quarter"]

    # For each group, pick the max date => last trading day of that grouping
    grouped = df_dates.groupby(group_cols, as_index=False)["date"].max()
    rebalance_dates = [d.date() for d in grouped["date"]]

    return rebalance_dates
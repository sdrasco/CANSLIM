# data/corporate_actions_adjuster.py

import logging
import pandas as pd
import warnings
from config.settings import DIVIDEND_ADJUSTMENT, TICKER_ADJUSTMENT
from data.splits_data_fetcher import fetch_splits_data
from data.dividends_data_fetcher import fetch_dividends_data
from data.ticker_events_data_fetcher import fetch_ticker_events
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)


def adjust_for_corporate_actions(data: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adjusting DataFrame for corporate actions.")
    tickers = data["ticker"].unique().tolist()
    logger.debug(f"Found {len(tickers)} unique tickers: {tickers}")

    splits_data = fetch_splits_data(tickers)
    data = apply_splits_adjustments(data, splits_data)

    if DIVIDEND_ADJUSTMENT:
        dividends_data = fetch_dividends_data(tickers)
        data = apply_dividends_adjustments(data, dividends_data)

    if TICKER_ADJUSTMENT:
        events_data = fetch_ticker_events(tickers)
        data = apply_ticker_events_adjustments(data, events_data)

    logger.info("Corporate actions adjustments completed.")
    return data

def apply_splits_adjustments(data: pd.DataFrame, splits_data: pd.DataFrame) -> pd.DataFrame:
    if splits_data.empty:
        logger.debug("No splits data found, no adjustments applied.")
        return data

    splits_data["execution_date"] = pd.to_datetime(splits_data["execution_date"])
    splits_data = splits_data.sort_values(["ticker", "execution_date"])

    for tkr in splits_data["ticker"].unique():
        tkr_splits = splits_data[splits_data["ticker"] == tkr]
        for _, row in tkr_splits.iterrows():
            exec_date = row["execution_date"]
            ratio = row["split_to"] / row["split_from"]
            mask = (data["ticker"] == tkr) & (data["date"] < exec_date)
            if data[mask].empty:
                continue
            data.loc[mask, ["open", "high", "low", "close"]] = data.loc[mask, ["open", "high", "low", "close"]] / ratio
            data.loc[mask, "volume"] = (data.loc[mask, "volume"] * ratio).round().astype(int)
            logger.debug(f"Applied {row['split_from']}-for-{row['split_to']} split on {tkr} as of {exec_date.date()}")
    return data

def apply_dividends_adjustments(data: pd.DataFrame, dividends_data: pd.DataFrame) -> pd.DataFrame:
    if dividends_data.empty:
        logger.debug("No dividends data found, no adjustments applied.")
        return data

    # Convert ex_dividend_date to datetime and sort
    dividends_data["ex_dividend_date"] = pd.to_datetime(dividends_data["ex_dividend_date"])
    dividends_data = dividends_data.sort_values(["ticker", "ex_dividend_date"])

    # For total-return adjustments, we apply dividends in ascending order.
    # For each ticker, cumulative adjustments may occur.
    for tkr in dividends_data["ticker"].unique():
        tkr_dividends = dividends_data[dividends_data["ticker"] == tkr]
        for _, row in tkr_dividends.iterrows():
            ex_date = row["ex_dividend_date"]
            dividend = row["cash_amount"]

            # Find ex_div_day close price
            day_mask = (data["ticker"] == tkr) & (data["date"] == ex_date)
            if data[day_mask].empty:
                # If no trading data for ex_div_date, skip adjustment
                logger.debug(f"No ex_div trading data for {tkr} on {ex_date.date()}, skipping dividend adjustment.")
                continue

            p_ex = data.loc[day_mask, "close"].iloc[0]
            if p_ex <= 0:
                logger.debug(f"Non-positive price for {tkr} on {ex_date.date()}, cannot adjust.")
                continue

            # Compute adjustment factor
            # After paying a dividend D, to get a total-return series:
            # We'll scale all historical prices before ex_date down by factor = (p_ex + D) / p_ex
            # This removes the drop on ex-date and makes it look like dividends were reinvested.
            factor = (p_ex + dividend) / p_ex

            # Adjust all historical prices before ex_date
            hist_mask = (data["ticker"] == tkr) & (data["date"] < ex_date)
            if not data[hist_mask].empty:
                data.loc[hist_mask, ["open", "high", "low", "close"]] = \
                    data.loc[hist_mask, ["open", "high", "low", "close"]] / factor
                logger.debug(
                    f"Applied dividend adjustment for {tkr} on {ex_date.date()} with D={dividend:.4f}, factor={factor:.6f}"
                )

    return data

def apply_ticker_events_adjustments(data: pd.DataFrame, events_data: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"ticker", "event_type", "new_ticker"}
    # If we rely on execution_date, add it too:
    required_cols.add("execution_date")

    missing = required_cols - set(events_data.columns)
    if missing:
        logger.warning(f"Ticker events data missing required columns: {missing}. "
                       "Skipping ticker event adjustments.")
        return data

    # Convert execution_date to datetime
    events_data["execution_date"] = pd.to_datetime(events_data["execution_date"], errors="coerce")
    # Drop any rows with invalid dates if needed
    events_data = events_data.dropna(subset=["execution_date"])
    events_data = events_data.sort_values(["ticker", "execution_date"])

    # Apply ticker changes
    for tkr in events_data["ticker"].unique():
        tkr_events = events_data[events_data["ticker"] == tkr]
        for _, row in tkr_events.iterrows():
            if row.get("event_type") == "ticker_change":
                old_ticker = tkr
                new_ticker = row["new_ticker"]
                exec_date = row["execution_date"]

                # If exec_date is missing or NaT, skip
                if pd.isna(exec_date):
                    logger.warning(f"No valid execution_date for ticker event {old_ticker} -> {new_ticker}, skipping.")
                    continue

                mask = (data["ticker"] == old_ticker) & (data["date"] >= exec_date)
                if not data[mask].empty:
                    data.loc[mask, "ticker"] = new_ticker
                    logger.debug(f"Renamed {old_ticker} to {new_ticker} for dates >= {exec_date.date()}")

    return data
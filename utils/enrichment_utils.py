"""
utils/enrichment_utils.py

Utility functions to enrich the top_stocks DataFrame with:
  - L_measure: a dimensionless "leadership" metric
  - quarterly_volume: sum of volumes over a chosen rolling window (~63 days)

Usage:
  from utils.enrichment_utils import enrich_with_L_and_volume

  top_stocks_df = enrich_with_L_and_volume(
      top_stocks_df=top_stocks_df,
      market_proxy_df=market_only_df,  # or whichever DataFrame has the market's close
      l_window=20,                     # how many days to average outperformance
      volume_window=63                 # how many days to sum volume (approx a quarter)
  )
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def enrich_with_L_and_volume(
    top_stocks_df: pd.DataFrame,
    market_proxy_df: pd.DataFrame,
    l_window: int = 20,
    volume_window: int = 63
) -> pd.DataFrame:
    """
    Enriches 'top_stocks_df' with two new columns:
      1) L_measure: A continuous measure of "leadership" = average daily alpha vs. market
                    over the last 'l_window' days.
      2) quarterly_volume: Rolling sum of volume over 'volume_window' days (~1 quarter).

    Assumptions:
      - top_stocks_df and market_proxy_df each have a DatetimeIndex (date).
      - Both have a 'close' column (floats).
      - top_stocks_df has a 'ticker' column for grouping different stocks.
      - You have not already computed 'stock_return' or 'market_return' in these DataFrames.
      - The index is sorted by ascending date (we do it again here for safety).

    :param top_stocks_df: DataFrame with columns ['ticker', 'close', 'volume'] at minimum.
    :param market_proxy_df: DataFrame for the market proxy (e.g., SPY) with a 'close' column.
    :param l_window: number of days to average stock outperformance vs. market (Default=20).
    :param volume_window: number of days to sum volume, approximating a quarter (Default=63).
    :return: A new DataFrame with columns 'L_measure' and 'quarterly_volume' added.
             The index remains a DatetimeIndex (date).
    """
    # Make copies to avoid mutating original data
    top_stocks_df = top_stocks_df.copy()
    market_proxy_df = market_proxy_df.copy()

    # Sort by index (date) ascending
    top_stocks_df.sort_index(inplace=True)
    market_proxy_df.sort_index(inplace=True)

    required_cols_top = {"ticker", "close", "volume"}
    if not required_cols_top.issubset(top_stocks_df.columns):
        missing = required_cols_top - set(top_stocks_df.columns)
        logger.error(f"top_stocks_df missing required columns {missing}. Returning original.")
        return top_stocks_df

    if "close" not in market_proxy_df.columns:
        logger.error("market_proxy_df missing 'close' column. Returning top_stocks_df unchanged.")
        return top_stocks_df

    # ----------------------------------------------------------------
    # 1) Compute daily returns for stocks and the market
    # ----------------------------------------------------------------
    # Stock returns (grouped by ticker to avoid crossing ticker boundaries)
    top_stocks_df["stock_return"] = (
        top_stocks_df.groupby("ticker")["close"]
        .pct_change()
        .fillna(0)
    )

    # Market returns
    market_proxy_df["market_return"] = market_proxy_df["close"].pct_change().fillna(0)

    # ----------------------------------------------------------------
    # 2) Merge the market's daily return into top_stocks_df by date
    # ----------------------------------------------------------------
    # Because we have two separate DataFrames with date as the index,
    # we do a reset_index to get date as a column for merging, then set index back.
    top_reset = top_stocks_df.reset_index()      # date becomes a column
    market_reset = market_proxy_df.reset_index() # date becomes a column

    # We only need 'date' and 'market_return' from the market DataFrame
    market_reset = market_reset[["date", "market_return"]]

    merged_df = pd.merge(
        top_reset,
        market_reset,
        on="date",
        how="left"
    )
    # Now put the date back as the index
    merged_df.set_index("date", inplace=True)
    merged_df.sort_index(inplace=True)

    # Fill any missing market_return with 0 (just in case)
    merged_df["market_return"] = merged_df["market_return"].fillna(0)

    # ----------------------------------------------------------------
    # 3) Compute a dimensionless "L_measure"
    #    Example: average alpha (stock_return - market_return) over the last l_window days
    # ----------------------------------------------------------------
    merged_df["daily_alpha"] = merged_df["stock_return"] - merged_df["market_return"]

    # Rolling mean by ticker
    merged_df["L_measure"] = (
        merged_df.groupby("ticker")["daily_alpha"]
        .transform(lambda x: x.rolling(l_window, min_periods=1).mean())
    )
    # Fill any leading NaNs
    merged_df["L_measure"]=merged_df["L_measure"].fillna(0)


    # ----------------------------------------------------------------
    # 4) Compute quarterly_volume = rolling sum of daily volume over volume_window
    # ----------------------------------------------------------------
    merged_df["quarterly_volume"] = (
        merged_df.groupby("ticker")["volume"]
        .transform(lambda x: x.rolling(volume_window, min_periods=1).sum())
    )
    merged_df["quarterly_volume"]=merged_df["quarterly_volume"].fillna(0)

    # ----------------------------------------------------------------
    # 5) Return the enriched DataFrame
    # ----------------------------------------------------------------
    # We leave 'stock_return', 'daily_alpha', etc. so you can debug if needed.
    # Drop them if you want a cleaner final DataFrame.

    logger.info(
        f"Enriched with L_measure (window={l_window}) and quarterly_volume (window={volume_window}). "
        f"Final shape: {merged_df.shape}, index type: {type(merged_df.index)}"
    )

    return merged_df

def calculate_m(market_only_df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the 'M' indicator on a DataFrame that has 'close' and a DatetimeIndex.
    Rolling calculations sort by index instead of by a 'date' column.
    """
    if "close" not in market_only_df.columns:
        logger.error("Market proxy data missing 'close' column required for M computation.")
        return market_only_df

    # Sort by the date index (ascending) so rolling windows go in chronological order
    market_only_df = market_only_df.sort_index()

    # compute moving averages
    market_only_df["50_MA"] = market_only_df["close"].rolling(50, min_periods=1).mean()
    market_only_df["200_MA"] = market_only_df["close"].rolling(200, min_periods=1).mean()

    # compute M
    market_only_df["M"] = market_only_df["50_MA"] > market_only_df["200_MA"]
    
    return market_only_df
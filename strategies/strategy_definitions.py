# strategies/strategy_definitions.py

import logging
import pandas as pd 
from config.settings import MARKET_PROXY, MONEY_MARKET_PROXY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def market_only_strategy(rebalance_date, portfolio_value, data_dict):
    """
    Strategy 1: Market Only
    Always invests fully in the MARKET_PROXY (e.g., SPY) and never changes.
    """
    # 100% MARKET_PROXY
    return {MARKET_PROXY: 1.0}


def shy_spy_strategy(rebalance_date, portfolio_value, data_dict):
    """
    Strategy 2: SHY-SPY
    Uses M from market_proxy_df. If M is True at rebalance_date, invest fully in SPY,
    otherwise fully in SHY.
    """
    market_proxy_df = data_dict.get("market_proxy_df")
    if market_proxy_df is None:
        logger.error("market_proxy_df not found in data_dict")
        return {MONEY_MARKET_PROXY: 1.0}

    # Debug logs
    logger.debug(f"SHY-SPY Strategy called for rebalance_date: {rebalance_date}")
    logger.debug(f"market_proxy_df shape: {market_proxy_df.shape}")

    if "date" not in market_proxy_df.columns:
        logger.error("market_proxy_df missing 'date' column.")
        return {MONEY_MARKET_PROXY: 1.0}

    # Check the date range and a sample of dates
    min_date = market_proxy_df["date"].min()
    max_date = market_proxy_df["date"].max()
    logger.debug(f"market_proxy_df date range: {min_date} to {max_date}")
    logger.debug("Sample of market_proxy_df dates:\n" + str(market_proxy_df["date"].head(5)))

    # Locate M value for this rebalance_date
    # Ensure rebalance_date is compatible with the type in market_proxy_df (Timestamp vs date)
    if pd.api.types.is_datetime64_any_dtype(market_proxy_df["date"]):
        # If our rebalance_date is a date, convert it to a Timestamp
        if isinstance(rebalance_date, (pd.Timestamp, pd.DatetimeIndex)):
            search_date = rebalance_date
        else:
            search_date = pd.Timestamp(rebalance_date)
    else:
        # If market_proxy_df date is not datetime, try converting it
        market_proxy_df["date"] = pd.to_datetime(market_proxy_df["date"], errors="coerce")
        search_date = pd.Timestamp(rebalance_date)

    logger.debug(f"Converted rebalance_date to search_date: {search_date}")

    row = market_proxy_df.loc[market_proxy_df["date"] == search_date]

    if row.empty:
        logger.warning(f"No market data for rebalance_date {rebalance_date}, "
                       f"search_date {search_date}, defaulting to SHY")
        # Log a few nearby dates to see if we're off by a day
        close_matches = market_proxy_df.iloc[(market_proxy_df["date"] - search_date).abs().argsort()[:5]]
        logger.debug("Closest dates found in market_proxy_df to search_date:\n" + str(close_matches["date"]))
        return {MONEY_MARKET_PROXY: 1.0}

    m_value = row["M"].iloc[0]
    logger.debug(f"For rebalance_date {rebalance_date}, M value is {m_value}")

    if m_value:
        # M is True, go full SPY
        return {MARKET_PROXY: 1.0}
    else:
        # M is False, go full SHY
        return {MONEY_MARKET_PROXY: 1.0}

def canslim_strategy(rebalance_date, portfolio_value, data_dict):
    """
    Strategy 3: CANSLI-based
    - If M is False, invest fully in SHY.
    - If M is True, find all stocks meeting CANSLI_all == True on rebalance_date,
      pick top 6 (or fewer if not enough), and invest equally among them.
      If none meet the criteria, invest in SHY.
    """
    market_proxy_df = data_dict.get("market_proxy_df")
    top_stocks_df = data_dict.get("top_stocks_df")

    if market_proxy_df is None or top_stocks_df is None:
        logger.error("market_proxy_df or top_stocks_df not found in data_dict")
        return {MONEY_MARKET_PROXY: 1.0}

    # Check M value
    m_row = market_proxy_df.loc[market_proxy_df["date"] == rebalance_date]
    if m_row.empty:
        logger.warning(f"No market data for {rebalance_date}, defaulting to SHY")
        return {MONEY_MARKET_PROXY: 1.0}

    m_value = m_row["M"].iloc[0]
    if not m_value:
        # M is False -> full SHY
        return {MONEY_MARKET_PROXY: 1.0}

    # M is True, select CANSLI_all stocks
    # We find all stocks meeting CANSLI_all on rebalance_date
    # top_stocks_df should have daily rows per stock with CANSLI_all column
    # We'll pick them by alphabetic order of ticker for simplicity.
    candidates = top_stocks_df[(top_stocks_df["date"] == rebalance_date) & (top_stocks_df["CANSLI_all"] == True)]
    if candidates.empty:
        # No CANSLI_all stocks -> SHY
        return {MONEY_MARKET_PROXY: 1.0}

    # Sort candidates by ticker (or another metric if desired)
    candidates = candidates.sort_values("ticker")
    chosen = candidates["ticker"].head(6).tolist()

    # Equal weight among chosen stocks
    weight = 1.0 / len(chosen)
    allocation = {tkr: weight for tkr in chosen}
    return allocation
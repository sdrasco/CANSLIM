# strategies/strategy_definitions.py

import logging
import pandas as pd
from config.settings import MARKET_PROXY, MONEY_MARKET_PROXY, INITIAL_FUNDS
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def market_only_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Strategy 1: Market Only
    Always invests fully in the MARKET_PROXY (e.g., SPY) and never changes.

    On the first rebalance day, if is_first_rebalance=True, we know no holdings have been set yet.
    In that case, ignore the passed-in portfolio_value (which may be 0) and use INITIAL_FUNDS directly.
    """
    if is_first_rebalance:
        logger.debug(f"First rebalance on {rebalance_date}. Overriding portfolio_value with INITIAL_FUNDS={INITIAL_FUNDS}")
        # The backtester will convert these weights into shares using INITIAL_FUNDS / price
        # Because we always return {MARKET_PROXY:1.0}, the backtester will allocate INITIAL_FUNDS worth of shares.
        return {MARKET_PROXY: 1.0}
    else:
        # On subsequent rebalances (if any), use the portfolio_value passed in. 
        # Since this strategy never changes allocation, it's always full MARKET_PROXY.
        return {MARKET_PROXY: 1.0}


def risk_managed_market_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Strategy 2: Risk Managed Market
    Uses M from market_proxy_df. If M is True at rebalance_date, invest fully in market proxy,
    otherwise fully in money market proxy.

    The is_first_rebalance parameter is available for first-day logic if desired.
    Currently, we do not use it here.
    """
    market_proxy_df = data_dict.get("market_proxy_df")
    if market_proxy_df is None:
        logger.error("market_proxy_df not found in data_dict")
        return {MONEY_MARKET_PROXY: 1.0}

    logger.debug(f"Risk Managed Market Strategy called for {rebalance_date}, is_first_rebalance={is_first_rebalance}")
    logger.debug(f"market_proxy_df shape: {market_proxy_df.shape}")

    if "date" not in market_proxy_df.columns:
        logger.error("market_proxy_df missing 'date' column.")
        return {MONEY_MARKET_PROXY: 1.0}

    # Determine search_date
    if pd.api.types.is_datetime64_any_dtype(market_proxy_df["date"]):
        if not isinstance(rebalance_date, pd.Timestamp):
            search_date = pd.Timestamp(rebalance_date)
        else:
            search_date = rebalance_date
    else:
        market_proxy_df["date"] = pd.to_datetime(market_proxy_df["date"], errors="coerce")
        search_date = pd.Timestamp(rebalance_date)

    logger.debug(f"Converted rebalance_date to search_date: {search_date}")

    row = market_proxy_df.loc[market_proxy_df["date"] == search_date]

    if row.empty:
        logger.warning(f"No market data for {rebalance_date}, defaulting to {MONEY_MARKET_PROXY}")
        close_matches = market_proxy_df.iloc[(market_proxy_df["date"] - search_date).abs().argsort()[:5]]
        logger.debug("Closest dates to search_date:\n" + str(close_matches["date"]))
        return {MONEY_MARKET_PROXY: 1.0}

    m_value = row["M"].iloc[0]
    logger.debug(f"For rebalance_date {rebalance_date}, M={m_value}")

    if m_value:
        return {MARKET_PROXY: 1.0}
    else:
        return {MONEY_MARKET_PROXY: 1.0}


def canslim_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Strategy 3: CANSLI-based
    - If M is False, invest fully in Money Market.
    - If M is True, find all stocks with CANSLI_all == True on rebalance_date,
      pick top 6 (or fewer if not enough), and invest equally among them.
      If none meet the criteria, invest in MONEY_MARKET_PROXY.

    The is_first_rebalance parameter is available if you want first-day logic.
    Currently not used here.
    """
    market_proxy_df = data_dict.get("market_proxy_df")
    top_stocks_df = data_dict.get("top_stocks_df")

    if market_proxy_df is None or top_stocks_df is None:
        logger.error("market_proxy_df or top_stocks_df not found in data_dict")
        return {MONEY_MARKET_PROXY: 1.0}

    m_row = market_proxy_df.loc[market_proxy_df["date"] == rebalance_date]
    if m_row.empty:
        logger.warning(f"No market data for {rebalance_date}, defaulting to {MONEY_MARKET_PROXY}")
        return {MONEY_MARKET_PROXY: 1.0}

    m_value = m_row["M"].iloc[0]
    if not m_value:
        return {MONEY_MARKET_PROXY: 1.0}

    # M is True, select CANSLI_all stocks
    candidates = top_stocks_df[(top_stocks_df["date"] == rebalance_date) & (top_stocks_df["CANSLI_all"] == True)]
    if candidates.empty:
        return {MONEY_MARKET_PROXY: 1.0}

    candidates = candidates.sort_values("ticker")
    chosen = candidates["ticker"].head(6).tolist()

    weight = 1.0 / len(chosen)
    allocation = {tkr: weight for tkr in chosen}
    return allocation
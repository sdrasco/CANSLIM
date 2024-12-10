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
    Market Only Strategy: Always invests fully in MARKET_PROXY.

    On the first rebalance day, if is_first_rebalance=True, we note that portfolio_value might be 0
    before rebalancing, but the backtester sets it to INITIAL_FUNDS, so we always return {MARKET_PROXY: 1.0}.
    """
    logger.debug(f"market_only_strategy called for {rebalance_date} with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}")
    if is_first_rebalance:
        logger.debug(f"First rebalance on {rebalance_date}. Overriding portfolio_value with INITIAL_FUNDS={INITIAL_FUNDS} if needed.")
    allocation = {MARKET_PROXY: 1.0}
    logger.debug(f"market_only_strategy returning allocation: {allocation}")
    return allocation


def risk_managed_market_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Risk Managed Market Strategy:
    Uses M from proxies_df (filtered by MARKET_PROXY ticker).
    If M is True, invest fully in MARKET_PROXY, else in MONEY_MARKET_PROXY.
    """
    logger.debug(f"risk_managed_market_strategy called for {rebalance_date} with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}")
    proxies_df = data_dict.get("proxies_df")
    if proxies_df is None:
        logger.error("proxies_df not found in data_dict. Defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    # Filter for MARKET_PROXY ticker to find M
    market_only = proxies_df[(proxies_df["ticker"] == MARKET_PROXY)]
    if "date" not in market_only.columns:
        logger.error("proxies_df missing 'date' column. Defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    # Convert rebalance_date to Timestamp if needed
    search_date = pd.Timestamp(rebalance_date)

    logger.debug(f"Converted rebalance_date {rebalance_date} to search_date {search_date} for M lookup.")
    row = market_only.loc[market_only["date"] == search_date]

    if row.empty:
        logger.warning(f"No market data (for {MARKET_PROXY}) on {rebalance_date}, using {MONEY_MARKET_PROXY}.")
        # Show closest matches
        if not market_only.empty:
            close_matches = market_only.iloc[(market_only['date'] - search_date).abs().argsort()[:5]]
            logger.debug("Closest dates to search_date:\n" + str(close_matches["date"]))
        allocation = {MONEY_MARKET_PROXY: 1.0}
        logger.debug(f"risk_managed_market_strategy returning allocation: {allocation}")
        return allocation

    m_value = row["M"].iloc[0]
    logger.debug(f"At {rebalance_date}, M={m_value}. If True => {MARKET_PROXY}, else => {MONEY_MARKET_PROXY}")

    if m_value:
        allocation = {MARKET_PROXY: 1.0}
    else:
        allocation = {MONEY_MARKET_PROXY: 1.0}

    logger.debug(f"risk_managed_market_strategy returning allocation: {allocation}")
    return allocation


def canslim_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    CANSLIM-based Strategy:
    - If M is False, full MONEY_MARKET_PROXY.
    - If M is True, select CANSLI_all stocks up to 6. If none, MONEY_MARKET_PROXY.

    We also filter proxies_df by MARKET_PROXY to find M.
    """
    logger.debug(f"canslim_strategy called for {rebalance_date} with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}")

    proxies_df = data_dict.get("proxies_df")
    top_stocks_df = data_dict.get("top_stocks_df")

    if proxies_df is None or top_stocks_df is None:
        logger.error("proxies_df or top_stocks_df missing from data_dict. Defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    market_only = proxies_df[proxies_df["ticker"] == MARKET_PROXY]
    search_date = pd.Timestamp(rebalance_date)
    m_row = market_only.loc[market_only["date"] == search_date]

    if m_row.empty:
        logger.warning(f"No market data (for {MARKET_PROXY}) on {rebalance_date}, using {MONEY_MARKET_PROXY}.")
        allocation = {MONEY_MARKET_PROXY: 1.0}
        logger.debug(f"canslim_strategy returning allocation: {allocation}")
        return allocation

    m_value = m_row["M"].iloc[0]
    logger.debug(f"At {rebalance_date}, M={m_value}")

    if not m_value:
        logger.debug("M is False, going full MONEY_MARKET_PROXY.")
        allocation = {MONEY_MARKET_PROXY: 1.0}
        logger.debug(f"canslim_strategy returning allocation: {allocation}")
        return allocation

    # M is True
    candidates = top_stocks_df[(top_stocks_df["date"] == search_date) & (top_stocks_df["CANSLI_all"] == True)]
    logger.debug(f"Found {len(candidates)} CANSLI candidates at {rebalance_date}.")

    if candidates.empty:
        logger.debug("No CANSLI stocks found, defaulting to MONEY_MARKET_PROXY.")
        allocation = {MONEY_MARKET_PROXY: 1.0}
        logger.debug(f"canslim_strategy returning allocation: {allocation}")
        return allocation

    candidates = candidates.sort_values("ticker")
    chosen = candidates["ticker"].head(6).tolist()
    logger.debug(f"Chosen stocks: {chosen}")

    weight = 1.0 / len(chosen)
    allocation = {tkr: weight for tkr in chosen}
    logger.debug(f"canslim_strategy returning allocation: {allocation}")
    return allocation
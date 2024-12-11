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

    # M is True, proceed with CANSLI filtering
    candidates = top_stocks_df[top_stocks_df["date"] == search_date]
    logger.debug(f"Found {len(candidates)} total candidates at {rebalance_date} before filtering.")

    # Require all CANSLI criteria
    require_c = True
    require_a = True
    require_n = True
    require_s = True
    require_l = True
    require_i = True

    logger.debug(f"Filtering candidates with conditions: "
                 f"C={require_c}, A={require_a}, N={require_n}, S={require_s}, L={require_l}, I={require_i}")

    filtered = candidates
    if require_c:
        filtered = filtered[filtered["C"]]
    if require_a:
        filtered = filtered[filtered["A"]]
    if require_n:
        filtered = filtered[filtered["N"]]
    if require_s:
        filtered = filtered[filtered["S"]]
    if require_l:
        filtered = filtered[filtered["L"]]
    if require_i:
        filtered = filtered[filtered["I"]]

    logger.debug(f"After applying conditions, {len(filtered)} candidates remain.")

    if filtered.empty:
        logger.debug("No candidates remain after filtering, defaulting to MONEY_MARKET_PROXY.")
        allocation = {MONEY_MARKET_PROXY: 1.0}
        logger.debug(f"canslim_strategy returning allocation: {allocation}")
        return allocation

    # All passed conditions are True here, so just use a tie-breaker score:
    # score = (close / 52_week_high) + (volume / 50_day_vol_avg)
    filtered = filtered.copy()
    filtered["score"] = (filtered["close"] / filtered["52_week_high"]) + (filtered["volume"] / filtered["50_day_vol_avg"])

    filtered = filtered.sort_values("score", ascending=False)
    chosen = filtered["ticker"].head(6).tolist()
    logger.debug(f"Chosen stocks after scoring: {chosen}")

    weight = 1.0 / len(chosen)
    allocation = {tkr: weight for tkr in chosen}
    logger.debug(f"canslim_strategy returning allocation: {allocation}")
    return allocation
    return allocation
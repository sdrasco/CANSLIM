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
    """
    logger.debug(f"market_only_strategy called for {rebalance_date} with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}")
    allocation = {MARKET_PROXY: 1.0}
    logger.debug(f"market_only_strategy returning allocation: {allocation}")
    return allocation

def risk_managed_market_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Risk Managed Market Strategy:
    If M is True for MARKET_PROXY, invest fully in MARKET_PROXY, else MONEY_MARKET_PROXY.
    """
    logger.debug(f"risk_managed_market_strategy called for {rebalance_date} with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}")
    proxies_df = data_dict.get("proxies_df")
    if proxies_df is None:
        logger.error("proxies_df not found in data_dict. Defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    market_only = proxies_df[(proxies_df["ticker"] == MARKET_PROXY)]
    search_date = pd.Timestamp(rebalance_date)
    row = market_only.loc[market_only["date"] == search_date]

    if row.empty:
        logger.warning(f"No market data (for {MARKET_PROXY}) on {rebalance_date}, using {MONEY_MARKET_PROXY}.")
        return {MONEY_MARKET_PROXY: 1.0}

    logger.info(f"Row for date {rebalance_date}:\n{row}")
    m_value = row["M"].iloc[0]
    allocation = {MARKET_PROXY: 1.0} if m_value else {MONEY_MARKET_PROXY: 1.0}
    logger.debug(f"risk_managed_market_strategy returning allocation: {allocation}")
    return allocation

def canslim_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    CANSLIM Strategy:
    Invests only when M is True. Filters stocks that pass all CANSLI criteria and are also
    members of the S&P 500 on the given rebalance date.

    Among those, chooses top 6 by a scoring metric and invests equally.
    Otherwise, invests in MONEY_MARKET_PROXY.

    Records the chosen stocks and their weights for later reporting in data_dict["canslim_investments"].
    """
    logger.debug(f"canslim_strategy called for {rebalance_date} with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}")

    proxies_df = data_dict.get("proxies_df")
    top_stocks_df = data_dict.get("top_stocks_df")
    sp500_snapshot_df = data_dict.get("sp500_snapshot_df")

    if proxies_df is None or top_stocks_df is None or sp500_snapshot_df is None:
        logger.error("Missing required data (proxies_df, top_stocks_df, or sp500_snapshot_df) in data_dict. Defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    market_only = proxies_df[proxies_df["ticker"] == MARKET_PROXY]
    search_date = pd.Timestamp(rebalance_date)
    m_row = market_only.loc[market_only["date"] == search_date]

    if m_row.empty:
        logger.warning(f"No market data (for {MARKET_PROXY}) on {rebalance_date}, using {MONEY_MARKET_PROXY}.")
        return {MONEY_MARKET_PROXY: 1.0}

    m_value = m_row["M"].iloc[0]
    logger.debug(f"At {rebalance_date}, M={m_value}")

    if not m_value:
        logger.debug("M is False, going full MONEY_MARKET_PROXY.")
        return {MONEY_MARKET_PROXY: 1.0}

    # M is True, proceed with CANSLI filtering
    candidates = top_stocks_df[top_stocks_df["date"] == search_date]
    logger.debug(f"Found {len(candidates)} total candidates at {rebalance_date} before filtering.")

    # Limit candidates to those in the S&P 500 on this date
    # sp500_snapshot_df has a single row per date with a comma-separated tickers column.
    sp500_row = sp500_snapshot_df.loc[sp500_snapshot_df["date"] == search_date]
    if sp500_row.empty:
        # If we have no snapshot for this day, default to money market.
        logger.warning(f"No S&P 500 snapshot data for {rebalance_date}, defaulting to MONEY_MARKET_PROXY.")
        return {MONEY_MARKET_PROXY: 1.0}

    sp500_tickers_str = sp500_row["tickers"].iloc[0]
    # Parse the comma-separated tickers
    sp500_tickers = set(sp500_tickers_str.split(","))

    # Filter candidates to only those in the S&P 500 on this day
    candidates = candidates[candidates["ticker"].isin(sp500_tickers)]
    logger.debug(f"{len(candidates)} candidates remain after restricting to S&P 500 membership.")

    # Apply CANSLI conditions
    filtered = candidates.copy()
    for col in ["C", "A", "N", "S", "L", "I"]:
        if col not in filtered.columns:
            logger.error(f"Missing {col} column in candidates, defaulting to MONEY_MARKET_PROXY.")
            return {MONEY_MARKET_PROXY: 1.0}
        filtered = filtered[filtered[col]]

    logger.debug(f"After applying CANSLI conditions, {len(filtered)} candidates remain.")

    if filtered.empty:
        logger.debug("No candidates remain after CANSLI filtering, defaulting to MONEY_MARKET_PROXY.")
        return {MONEY_MARKET_PROXY: 1.0}

    # Score candidates
    # Using close/52_week_high and volume/50_day_vol_avg as per previous logic
    filtered["score"] = (filtered["close"] / filtered["52_week_high"]) + (filtered["volume"] / filtered["50_day_vol_avg"])
    filtered = filtered.sort_values("score", ascending=False)
    chosen = filtered["ticker"].head(6).tolist()
    logger.debug(f"Chosen stocks after scoring: {chosen}")

    weight = 1.0 / len(chosen)
    allocation = {tkr: weight for tkr in chosen}
    logger.debug(f"canslim_strategy returning allocation: {allocation}")

    # Record chosen allocations
    chosen_details = [{"ticker": t, "weight": allocation[t]} for t in chosen]
    data_dict.setdefault("canslim_investments", []).append({
        "date": rebalance_date,
        "portfolio_value_before_rebalance": portfolio_value,
        "investments": chosen_details
    })

    return allocation

def canslim_sp500_hybrid_option_a(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Hybrid S&P/CANSLIM (Option A):
    1) Must be in the S&P 500 on rebalance_date.
    2) Must have either C==True OR A==True (moderate EPS growth).
    3) Must have L==True (leader vs. the market).
    4) Ignore M entirely (no market-timing filter).
    If no stocks pass, default to MARKET_PROXY (e.g. SPY).
    Equal weighting among all passing stocks.
    """
    logger.debug(f"canslim_sp500_hybrid_option_a called for {rebalance_date} with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}")

    # Unpack data from data_dict
    sp500_snapshot_df = data_dict.get("sp500_snapshot_df")
    top_stocks_df = data_dict.get("top_stocks_df")
    
    # Basic checks
    if sp500_snapshot_df is None or top_stocks_df is None:
        logger.error("Missing sp500_snapshot_df or top_stocks_df in data_dict. Defaulting to MARKET_PROXY.")
        return {MARKET_PROXY: 1.0}

    # Filter top_stocks_df by rebalance_date
    search_date = pd.Timestamp(rebalance_date)
    candidates = top_stocks_df[top_stocks_df["date"] == search_date].copy()

    # Get S&P 500 membership for this date
    sp500_row = sp500_snapshot_df.loc[sp500_snapshot_df["date"] == search_date]
    if sp500_row.empty:
        logger.warning(f"No S&P snapshot for {rebalance_date}, defaulting to {MARKET_PROXY}.")
        return {MARKET_PROXY: 1.0}

    sp500_tickers_str = sp500_row["tickers"].iloc[0]
    sp500_tickers = set(sp500_tickers_str.split(","))

    # Only keep tickers that are in the S&P 500 on this date
    candidates = candidates[candidates["ticker"].isin(sp500_tickers)]
    logger.debug(f"{len(candidates)} candidates after restricting to S&P 500 membership.")

    # We need the columns C, A, and L
    required_cols = ["C", "A", "L"]
    for col in required_cols:
        if col not in candidates.columns:
            logger.error(f"Missing column '{col}' in top_stocks_df; defaulting to MARKET_PROXY.")
            return {MARKET_PROXY: 1.0}

    # Filter: must have (C == True OR A == True) AND L == True
    filtered = candidates[(candidates["L"] == True) & ((candidates["C"] == True) | (candidates["A"] == True))]
    logger.debug(f"{len(filtered)} candidates remain after (C or A) & L filter.")

    # If nothing qualifies, default to S&P 500 proxy
    if filtered.empty:
        logger.debug("No candidates pass the filter, defaulting to MARKET_PROXY.")
        return {MARKET_PROXY: 1.0}

    # Example weighting approach: equally weight all passing tickers
    chosen_tickers = filtered["ticker"].unique().tolist()
    weight = 1.0 / len(chosen_tickers)
    allocation = {tkr: weight for tkr in chosen_tickers}

    logger.debug(f"canslim_sp500_hybrid_option_a returning allocation: {allocation}")
    return allocation





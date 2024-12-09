# strategies/strategy_definitions.py

import logging
from config.settings import MARKET_PROXY, MONEY_MARKET_PROXY

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

    # Locate M value for this rebalance_date
    # Assuming market_proxy_df has a unique row per date
    row = market_proxy_df.loc[market_proxy_df["date"] == rebalance_date]
    if row.empty:
        logger.warning(f"No market data for rebalance_date {rebalance_date}, defaulting to SHY")
        return {MONEY_MARKET_PROXY: 1.0}

    m_value = row["M"].iloc[0]
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
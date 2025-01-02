import logging
import pandas as pd
from config.settings import MARKET_PROXY, MONEY_MARKET_PROXY
from utils.logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)



def market_only_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Market Only Strategy: Always invests fully in MARKET_PROXY.
    """
    logger.debug(
        f"market_only_strategy called for {rebalance_date} "
        f"with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}"
    )
    allocation = {MARKET_PROXY: 1.0}
    logger.debug(f"market_only_strategy returning allocation: {allocation}")
    return allocation


def risk_managed_market_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Risk Managed Market Strategy:
      1) If 'M' is True for MARKET_PROXY at rebalance_date, invest fully in MARKET_PROXY.
      2) Otherwise invest fully in MONEY_MARKET_PROXY.
    
    If 'M' is missing, compute it using a 50-day vs. 200-day MA cross.
    """
    logger.debug(
        f"risk_managed_market_strategy called for {rebalance_date} "
        f"with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}"
    )

    proxies_df = data_dict.get("proxies_df")
    if proxies_df is None:
        logger.error("proxies_df not found in data_dict. Defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    # Convert the incoming date to a Timestamp
    rebalance_ts = pd.to_datetime(rebalance_date).floor("D")

    # Filter the market-only data up to rebalance_ts
    market_only = proxies_df[proxies_df["ticker"] == MARKET_PROXY].copy()
    market_only.sort_index(inplace=True)
    market_slice = market_only.loc[:rebalance_ts]
    if market_slice.empty:
        logger.warning(f"No data for {MARKET_PROXY} on or before {rebalance_ts}, defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    # If 'M' column not found, compute on the entire market_only set
    if "M" not in market_slice.columns:
        logger.info("Column 'M' not found; computing M as (50-day > 200-day MA).")
        if "close" not in market_slice.columns:
            logger.error(
                f"Market proxy data missing 'close' column required for M computation. "
                f"Defaulting to {MONEY_MARKET_PROXY}."
            )
            return {MONEY_MARKET_PROXY: 1.0}
        market_slice["50_MA"] = market_slice["close"].rolling(50, min_periods=1).mean()
        market_slice["200_MA"] = market_slice["close"].rolling(200, min_periods=1).mean()
        market_slice["M"] = market_slice["50_MA"] > market_slice["200_MA"]

    # "Latest available" row
    last_date = market_slice.index.max()
    row = market_slice.loc[[last_date]]

    if row.empty:
        logger.warning(f"No market data for {MARKET_PROXY} on or before {rebalance_ts}, defaulting to money market.")
        return {MONEY_MARKET_PROXY: 1.0}

    m_value = row["M"].iloc[0]
    logger.debug(f"At {rebalance_ts}, M={m_value}")
    return {MARKET_PROXY: 1.0} if m_value else {MONEY_MARKET_PROXY: 1.0}


def canslim_sp500_hybrid(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Hybrid S&P/CANSLIM:
      - Ignores M entirely (no market-timing).
      - Must be in the S&P 500 on rebalance_date.
      - Must satisfy (C == True OR A == True) AND L == True.
      - Ranks by (close/52_week_high + volume/50_day_vol_avg), picks up to 450,
        then allocates capital proportionally to each stock's score.
      - If no stocks pass, defaults to MARKET_PROXY (SPY).

    Records how many stocks were picked each time in data_dict["hybrid_pick_counts"].
    """
    logger.debug(
        f"canslim_sp500_hybrid called for {rebalance_date} "
        f"with portfolio_value={portfolio_value}, is_first_rebalance={is_first_rebalance}"
    )

    sp500_snapshot_df = data_dict.get("sp500_snapshot_df")
    top_stocks_df = data_dict.get("top_stocks_df")
    if sp500_snapshot_df is None or top_stocks_df is None:
        logger.error(
            "Missing sp500_snapshot_df or top_stocks_df in data_dict. "
            f"Defaulting to {MARKET_PROXY}."
        )
        return {MARKET_PROXY: 1.0}

    rebalance_ts = pd.to_datetime(rebalance_date).floor("D")

    # 1) Get top_stocks for exactly rebalance_ts if possible
    if rebalance_ts not in top_stocks_df.index:
        # If there's no exact row, try a fallback
        try:
            candidates = top_stocks_df.loc[[rebalance_ts]].copy()
        except KeyError:
            logger.debug(f"No top_stocks data found for {rebalance_ts}, defaulting to {MARKET_PROXY}.")
            data_dict.setdefault("hybrid_pick_counts", []).append(0)
            return {MARKET_PROXY: 1.0}
    else:
        candidates = top_stocks_df.loc[[rebalance_ts]].copy()

    logger.debug(f"Found {len(candidates)} total candidates at {rebalance_ts} before S&P filtering.")

    # 2) Check S&P 500 membership (one-row-per-date, 'tickers' col with CSV)
    snapshot_slice = sp500_snapshot_df.loc[:rebalance_ts]
    if snapshot_slice.empty:
        logger.warning(f"No S&P 500 snapshot data on or before {rebalance_ts}, defaulting to {MARKET_PROXY}.")
        return {MARKET_PROXY: 1.0}

    last_snapshot_date = snapshot_slice.index.max()
    sp500_row = snapshot_slice.loc[[last_snapshot_date]]

    if sp500_row.empty or "tickers" not in sp500_row.columns:
        logger.warning(f"No S&P 500 snapshot data for {rebalance_ts}, defaulting to {MARKET_PROXY}.")
        return {MARKET_PROXY: 1.0}

    sp500_tickers_str = sp500_row["tickers"].iloc[0]
    sp500_tickers = set(sp500_tickers_str.split(","))

    candidates = candidates[candidates["ticker"].isin(sp500_tickers)]
    logger.debug(f"{len(candidates)} remain after restricting to S&P 500 membership.")

    required_cols = ["C", "A", "L", "close", "52_week_high", "volume", "50_day_vol_avg"]
    missing_cols = [col for col in required_cols if col not in candidates.columns]
    if missing_cols:
        logger.error(f"Missing columns {missing_cols} in top_stocks_df; defaulting to {MARKET_PROXY}.")
        return {MARKET_PROXY: 1.0}

    # Filter for (C == True or A == True) AND L == True
    filtered = candidates[(candidates["L"]) & (candidates["C"] | candidates["A"])]
    logger.debug(f"{len(filtered)} remain after (C or A) & L filter.")

    if filtered.empty:
        logger.debug("No candidates pass the hybrid filter, defaulting to MARKET_PROXY.")
        data_dict.setdefault("hybrid_pick_counts", []).append(0)
        return {MARKET_PROXY: 1.0}

    # Scoring
    filtered = filtered.copy()
    filtered["score"] = (
        (filtered["close"] / filtered["52_week_high"])
        + (filtered["volume"] / filtered["50_day_vol_avg"])
    )
    filtered.sort_values("score", ascending=False, inplace=True)

    # Select up to 450
    chosen_df = filtered.head(450).copy()
    logger.debug(f"Chosen stocks after scoring (up to 450): {len(chosen_df)}")

    total_score = chosen_df["score"].sum()
    if total_score <= 0:
        logger.debug("All scores are zero or negative. Defaulting to MARKET_PROXY.")
        data_dict.setdefault("hybrid_pick_counts", []).append(0)
        return {MARKET_PROXY: 1.0}

    chosen_df["weight"] = chosen_df["score"] / total_score
    allocation = dict(zip(chosen_df["ticker"], chosen_df["weight"]))

    # Record how many stocks were chosen
    pick_count = len(chosen_df)
    logger.info(f"canslim_sp500_hybrid returning {pick_count} allocations weighted by score.")
    data_dict.setdefault("hybrid_pick_counts", []).append(pick_count)

    # Also record the chosen details
    chosen_details = []
    for _, row in chosen_df.iterrows():
        chosen_details.append({"ticker": row["ticker"], "weight": row["weight"]})

    data_dict.setdefault("hybrid_investments", []).append({
        "date": rebalance_ts,
        "portfolio_value_before_rebalance": portfolio_value,
        "investments": chosen_details
    })

    return allocation


# --------------------------------------------------------------------
# Simple Strategies
# --------------------------------------------------------------------

def flattened_spy_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Equal-weight all stocks that are in the S&P 500 as of rebalance_date,
    by reading the single CSV 'tickers' column (same approach as canslim_sp500_hybrid).
    """
    logger.debug(f"flattened_spy_strategy called for {rebalance_date} with value={portfolio_value}")
    sp500_snapshot_df = data_dict.get("sp500_snapshot_df")
    if sp500_snapshot_df is None:
        logger.error("sp500_snapshot_df missing in data_dict. Returning empty.")
        return {}

    rebalance_ts = pd.to_datetime(rebalance_date).floor("D")

    # 1) Slice all data up to rebalance_ts (just like hybrid)
    snapshot_slice = sp500_snapshot_df.loc[:rebalance_ts]
    if snapshot_slice.empty:
        logger.warning(f"No S&P500 snapshot data up to {rebalance_ts}. Returning empty.")
        return {}

    latest_date = snapshot_slice.index.max()
    sp500_row = snapshot_slice.loc[[latest_date]]
    if sp500_row.empty or "tickers" not in sp500_row.columns:
        logger.warning(f"No S&P500 membership found on or before {rebalance_ts}. Returning empty.")
        return {}

    # 2) The 'tickers' column is a comma-delimited string
    sp500_tickers_str = sp500_row["tickers"].iloc[0]
    sp500_tickers = sp500_tickers_str.split(",")
    sp500_tickers = [t.strip() for t in sp500_tickers if t.strip()]

    if not sp500_tickers:
        logger.warning("No tickers in S&P500 membership. Returning empty.")
        return {}

    weight = 1.0 / len(sp500_tickers)
    allocation = {t: weight for t in sp500_tickers}

    logger.debug(f"flattened_spy_strategy returns {len(allocation)} allocations.")
    return allocation


def leader_spy_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Weight all S&P 500 constituents by their 'L_measure', normalized so sum of weights = 1.
    We read membership from the single-row 'tickers' col, just like the hybrid approach.
    """
    logger.debug(f"leader_spy_strategy called for {rebalance_date} with value={portfolio_value}")
    sp500_snapshot_df = data_dict.get("sp500_snapshot_df")
    top_stocks_df = data_dict.get("top_stocks_df")
    if sp500_snapshot_df is None or top_stocks_df is None:
        logger.error("Missing data for leader_spy_strategy. Returning empty.")
        return {}

    rebalance_ts = pd.to_datetime(rebalance_date).floor("D")

    # 1) Slice S&P 500 snapshot, find 'tickers'
    snapshot_slice = sp500_snapshot_df.loc[:rebalance_ts]
    if snapshot_slice.empty:
        logger.warning(f"No S&P500 snapshot data up to {rebalance_ts}. Returning empty.")
        return {}

    latest_date = snapshot_slice.index.max()
    sp500_row = snapshot_slice.loc[[latest_date]]
    if sp500_row.empty or "tickers" not in sp500_row.columns:
        logger.warning(f"No S&P500 membership on or before {rebalance_ts}. Returning empty.")
        return {}

    sp500_tickers_str = sp500_row["tickers"].iloc[0]
    sp500_tickers = sp500_tickers_str.split(",")
    sp500_tickers = [t.strip() for t in sp500_tickers if t.strip()]

    if not sp500_tickers:
        logger.warning(f"No tickers found in S&P500 membership on or before {rebalance_ts}. Returning empty.")
        return {}

    # 2) Among top_stocks_df, pick these tickers as of rebalance_ts
    valid_data = top_stocks_df.loc[:rebalance_ts].copy()
    valid_data = valid_data[valid_data["ticker"].isin(sp500_tickers)]
    if valid_data.empty or "L_measure" not in valid_data.columns:
        logger.warning("No L_measure data for S&P500 members. Returning empty.")
        return {}

    # 3) For each ticker, get the *latest* row
    valid_data.sort_values("ticker", inplace=True)
    recent_entries = valid_data.groupby("ticker", group_keys=False).tail(1)

    # Normalize L_measure
    recent_entries.loc[:, "L_measure"] = recent_entries["L_measure"].clip(lower=0.0).fillna(0.0)
    total_L = recent_entries["L_measure"].sum()

    if total_L <= 0:
        logger.debug("All L_measures are 0.0 or missing, falling back to equal weight.")
        w = 1.0 / len(sp500_tickers)
        return {t: w for t in sp500_tickers}

    allocation = {
        row["ticker"]: row["L_measure"] / total_L
        for _, row in recent_entries.iterrows()
    }

    logger.debug(f"leader_spy_strategy returns {len(allocation)} allocations.")
    return allocation


def volume_spy_strategy(rebalance_date, portfolio_value, data_dict, is_first_rebalance=False):
    """
    Weight all S&P 500 constituents by last quarter's volume, normalized so sum of weights = 1.
    We read membership from the single-row 'tickers' col, just like the hybrid approach.
    """
    logger.debug(f"volume_spy_strategy called for {rebalance_date} with value={portfolio_value}")
    sp500_snapshot_df = data_dict.get("sp500_snapshot_df")
    top_stocks_df = data_dict.get("top_stocks_df")
    if sp500_snapshot_df is None or top_stocks_df is None:
        logger.error("Missing data for volume_spy_strategy. Returning empty.")
        return {}

    rebalance_ts = pd.to_datetime(rebalance_date).floor("D")

    # 1) Slice up to rebalance_ts, find 'tickers' for that day
    snapshot_slice = sp500_snapshot_df.loc[:rebalance_ts]
    if snapshot_slice.empty:
        logger.warning(f"No S&P500 snapshot data up to {rebalance_ts}. Returning empty.")
        return {}

    latest_date = snapshot_slice.index.max()
    sp500_row = snapshot_slice.loc[[latest_date]]
    if sp500_row.empty or "tickers" not in sp500_row.columns:
        logger.warning(f"No S&P500 membership on or before {rebalance_ts}. Returning empty.")
        return {}

    sp500_tickers_str = sp500_row["tickers"].iloc[0]
    sp500_tickers = sp500_tickers_str.split(",")
    sp500_tickers = [t.strip() for t in sp500_tickers if t.strip()]

    if not sp500_tickers:
        logger.warning(f"No tickers in S&P500 membership on or before {rebalance_ts}. Returning empty.")
        return {}

    # 2) Slice top_stocks_df for these tickers up to rebalance_ts
    valid_data = top_stocks_df.loc[:rebalance_ts].copy()
    valid_data = valid_data[valid_data["ticker"].isin(sp500_tickers)]
    if valid_data.empty or "quarterly_volume" not in valid_data.columns:
        logger.warning("No quarterly_volume data for S&P500 members. Returning empty.")
        return {}

    valid_data.sort_values("ticker", inplace=True)
    recent_entries = valid_data.groupby("ticker", group_keys=False).tail(1)

    recent_entries.loc[:,"quarterly_volume"] = recent_entries["quarterly_volume"].clip(lower=0).fillna(0)
    total_vol = recent_entries["quarterly_volume"].sum()

    if total_vol <= 0:
        logger.debug("All volumes are 0 or missing. Falling back to equal weight.")
        w = 1.0 / len(sp500_tickers)
        return {t: w for t in sp500_tickers}

    allocation = {
        row["ticker"]: row["quarterly_volume"] / total_vol
        for _, row in recent_entries.iterrows()
    }

    logger.debug(f"volume_spy_strategy returns {len(allocation)} allocations.")
    return allocation
"""
canslim_calculator.py

Refactored to use the index (date as DatetimeIndex) for proxies_df and top_stocks_df.
When merging, temporarily reset the index to a 'date' column, then set it back.
"""

import pandas as pd
import logging
import warnings
from config.settings import MARKET_PROXY
from utils.logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

def calculate_m(market_only_df: pd.DataFrame, criteria_config: dict) -> pd.DataFrame:
    """
    Computes the 'M' indicator on a DataFrame that has 'close' and a DatetimeIndex.
    Rolling calculations sort by index instead of by a 'date' column.
    """
    if "close" not in market_only_df.columns:
        logger.error("Market proxy data missing 'close' column required for M computation.")
        return market_only_df

    # Sort by the date index (ascending) so rolling windows go in chronological order
    market_only_df = market_only_df.sort_index()

    market_only_df["50_MA"] = market_only_df["close"].rolling(50, min_periods=1).mean()
    market_only_df["200_MA"] = market_only_df["close"].rolling(200, min_periods=1).mean()

    use_ma_cross = criteria_config["M"].get("use_ma_cross", True)
    if use_ma_cross:
        market_only_df["M"] = market_only_df["50_MA"] > market_only_df["200_MA"]
    else:
        market_only_df["M"] = True

    return market_only_df

def compute_c_a_from_financials(financials_df: pd.DataFrame, criteria_config: dict):
    """
    Computes C and A indicators from financials data.
    Returns a DataFrame with ['ticker', 'end_date', 'C', 'A'] so it can be merged.
    """
    required = {"ticker", "timeframe", "fiscal_year", "fiscal_period", "diluted_eps", "end_date"}
    if not required.issubset(financials_df.columns):
        missing = required - set(financials_df.columns)
        logger.error(f"Financials data missing required columns: {missing}")
        return pd.DataFrame(columns=["ticker", "end_date", "C", "A"])

    c_thresh = criteria_config["C"].get("quarterly_growth_threshold", 0.25)
    a_thresh = criteria_config["A"].get("annual_growth_threshold", 0.20)

    logger.debug("Starting computation of C and A from financials.")

    # Quarterly C
    quarterly = financials_df[financials_df["timeframe"] == "quarterly"].copy()
    quarterly.sort_values(["ticker", "fiscal_period", "fiscal_year"], inplace=True)
    quarterly["prev_year_eps"] = quarterly.groupby(["ticker", "fiscal_period"])["diluted_eps"].shift(1)
    quarterly["C"] = (
        (quarterly["diluted_eps"] - quarterly["prev_year_eps"])
        / quarterly["prev_year_eps"].abs()
        >= c_thresh
    )

    # Annual A
    annual = financials_df[financials_df["timeframe"] == "annual"].copy()
    annual.sort_values(["ticker", "fiscal_year"], inplace=True)
    annual["prev_year_eps"] = annual.groupby("ticker")["diluted_eps"].shift(1)
    annual["A_ratio"] = (annual["diluted_eps"] - annual["prev_year_eps"]) / annual["prev_year_eps"].abs()
    annual["A"] = annual["A_ratio"] >= a_thresh

    q_ca = quarterly[["ticker", "end_date", "C"]].drop_duplicates(["ticker", "end_date"])
    a_ca = annual[["ticker", "end_date", "A"]].drop_duplicates(["ticker", "end_date"])

    ca_df = pd.merge(q_ca, a_ca, on=["ticker", "end_date"], how="outer")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        ca_df["C"] = ca_df["C"].fillna(False).astype(bool)
        ca_df["A"] = ca_df["A"].fillna(False).astype(bool)

    return ca_df

def calculate_nsli(top_stocks_df: pd.DataFrame, market_only: pd.DataFrame, criteria_config: dict) -> pd.DataFrame:
    """
    Computes N, S, L, and I indicators for top_stocks_df, joining market data (market_return).
    Both DataFrames have a DatetimeIndex. 
    """
    required_cols = {"ticker", "close", "open", "volume", "high", "low"}
    if not required_cols.issubset(top_stocks_df.columns):
        missing = required_cols - set(top_stocks_df.columns)
        logger.error(f"Top stocks data missing required columns: {missing}")
        return top_stocks_df

    # Sort both by index
    top_stocks_df = top_stocks_df.sort_index()
    market_only = market_only.sort_index()

    # Market daily returns
    market_only["market_return"] = market_only["close"].pct_change().fillna(0)

    # Each stock's daily returns
    top_stocks_df["stock_return"] = top_stocks_df.groupby("ticker")["close"].pct_change().fillna(0)

    # Merge market_return into top_stocks, matching on index date
    # (reset index so we can do a column-based merge, then re-set index)
    top_stocks_reset = top_stocks_df.reset_index()
    market_only_reset = market_only.reset_index()

    top_stocks_merged = top_stocks_reset.merge(
        market_only_reset[["date", "market_return"]],
        on="date",
        how="left"
    )
    top_stocks_merged = top_stocks_merged.set_index("date")

    # N: 52-week high
    lookback_period_n = criteria_config["N"].get("lookback_period", 252)
    top_stocks_merged["52_week_high"] = (
        top_stocks_merged.groupby("ticker")["close"]
        .transform(lambda x: x.rolling(lookback_period_n, min_periods=1).max())
    )
    top_stocks_merged["N"] = top_stocks_merged["close"] >= top_stocks_merged["52_week_high"]

    # S: volume factor
    s_factor = criteria_config["S"].get("volume_factor", 1.5)
    top_stocks_merged["50_day_vol_avg"] = (
        top_stocks_merged.groupby("ticker")["volume"]
        .transform(lambda x: x.rolling(50, min_periods=1).mean())
    )
    top_stocks_merged["S"] = top_stocks_merged["volume"] >= top_stocks_merged["50_day_vol_avg"] * s_factor

    # L: Leader/Laggard
    l_diff = criteria_config["L"].get("return_diff_threshold", 0.0)
    top_stocks_merged["L"] = (
        top_stocks_merged["stock_return"] - top_stocks_merged["market_return"]
    ) > l_diff

    # I: Institutional Sponsorship (A/D ratio)
    def calc_ad_value(row):
        high = row["high"]
        low = row["low"]
        close = row["close"]
        vol = row["volume"]
        if high == low:
            return 0
        return (((close - low) - (high - close)) / (high - low)) * vol

    top_stocks_merged["ad_value"] = top_stocks_merged.apply(calc_ad_value, axis=1)
    i_lookback = criteria_config["I"].get("lookback_period", 50)
    i_threshold = criteria_config["I"].get("ad_ratio_threshold", 1.25)

    top_stocks_merged["AD_ratio"] = (
        top_stocks_merged.groupby("ticker")["ad_value"]
        .transform(lambda x: x.rolling(i_lookback, min_periods=1).mean())
    )
    top_stocks_merged["I"] = top_stocks_merged["AD_ratio"] >= i_threshold

    return top_stocks_merged

def merge_ca_into_top_stocks(top_stocks_df: pd.DataFrame, ca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges C and A columns into top_stocks_df using an asof merge on end_date <= date.
    top_stocks_df has a DatetimeIndex, so we reset it for merging.
    """
    required = {"ticker", "end_date", "C", "A"}
    if not required.issubset(ca_df.columns):
        missing = required - set(ca_df.columns)
        logger.error(f"CA Data missing required columns: {missing}")
        top_stocks_df["C"] = False
        top_stocks_df["A"] = False
        return top_stocks_df

    # Sort by index
    top_stocks_df = top_stocks_df.sort_index()

    # Reset index for merging
    # top_stocks might contain multiple rows per date (different tickers)
    top_stocks_reset = top_stocks_df.reset_index().sort_values(["ticker", "date"])
    ca_df = ca_df.sort_values(["ticker", "end_date"])

    result_parts = []
    for tkr, group in top_stocks_reset.groupby("ticker", group_keys=False):
        ca_sub = ca_df[ca_df["ticker"] == tkr]
        if ca_sub.empty:
            group["C"] = False
            group["A"] = False
        else:
            # asof merge requires the merge keys to be sorted and typically named consistently
            group = pd.merge_asof(
                group.sort_values("date"),
                ca_sub.sort_values("end_date").drop(columns="ticker"),
                left_on="date",
                right_on="end_date",
                direction="backward"
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                group["C"] = group["C"].fillna(False).astype(bool)
                group["A"] = group["A"].fillna(False).astype(bool)
        result_parts.append(group)

    merged_df = pd.concat(result_parts, ignore_index=True)
    merged_df = merged_df.set_index("date").sort_index()
    return merged_df

def calculate_canslim_indicators(
    proxies_df: pd.DataFrame,
    top_stocks_df: pd.DataFrame,
    financials_df: pd.DataFrame,
    criteria_config=None
):
    if criteria_config is None:
        criteria_config = {
            "C": {"quarterly_growth_threshold": 0.1},
            "A": {"annual_growth_threshold": 0.1},
            "N": {"lookback_period": 252},
            "S": {"volume_factor": 1.25},
            "L": {"return_diff_threshold": 0.0},
            "I": {"lookback_period": 50, "ad_ratio_threshold": 1.25},
            "M": {"use_ma_cross": True}
        }

    # Extract or create 'M' for the market proxy
    market_only = proxies_df[proxies_df["ticker"] == MARKET_PROXY].copy()
    if "M" not in market_only.columns:
        market_only = calculate_m(market_only, criteria_config)
    else:
        logger.info("'M' column found in market proxy. Skipping calculation of M.")

    # Remove old columns from proxies_df
    proxies_df = proxies_df.drop(columns=["50_MA", "200_MA", "M"], errors="ignore")

    # Reset index to merge M, then re-set
    proxies_reset = proxies_df.reset_index()
    market_reset = market_only.reset_index()

    proxies_merged = proxies_reset.merge(
        market_reset[["date", "50_MA", "200_MA", "M"]],
        on="date",
        how="left"
    ).set_index("date")

    proxies_merged["M"] = proxies_merged["M"].fillna(False).astype(bool)
    proxies_df = proxies_merged.sort_index()

    # Do the same for top_stocks
    top_stocks_df = top_stocks_df.drop(columns=["M"], errors="ignore")
    top_stocks_reset = top_stocks_df.reset_index()

    # Merge M into top_stocks, then re-set the index
    top_stocks_merged = top_stocks_reset.merge(
        market_reset[["date", "M"]],
        on="date",
        how="left"
    ).set_index("date")
    top_stocks_merged["M"] = top_stocks_merged["M"].fillna(False).astype(bool)
    top_stocks_df = top_stocks_merged.sort_index()

    # Compute C & A from financials
    logger.info("Computing C and A from financial data...")
    ca_df = compute_c_a_from_financials(financials_df, criteria_config)

    # Calculate N, S, L, I (returns a df with a DatetimeIndex)
    logger.info("Calculating N, S, L, I in top stocks data...")
    top_stocks_df = calculate_nsli(top_stocks_df, market_only, criteria_config)

    # Merge C & A
    logger.info("Merging C and A into top stocks data...")
    top_stocks_df = merge_ca_into_top_stocks(top_stocks_df, ca_df)

    # Create a single CANSLI_all column
    logger.info("Calculating CANSLI_all column...")
    required_cansli_cols = ["C", "A", "N", "S", "L", "I"]
    missing_cansli = [col for col in required_cansli_cols if col not in top_stocks_df.columns]
    if missing_cansli:
        logger.error(f"Missing some CANSLI columns: {missing_cansli}")
        top_stocks_df["CANSLI_all"] = False
    else:
        top_stocks_df["CANSLI_all"] = (
            top_stocks_df["C"]
            & top_stocks_df["A"]
            & top_stocks_df["N"]
            & top_stocks_df["S"]
            & top_stocks_df["L"]
            & top_stocks_df["I"]
        )

    logger.info("CANSLIM indicators computed.")

    # Optional dictionary describing final criteria used
    canslim_criteria_dict = {
        "C": {
            "name": "Current Quarterly Earnings",
            "description": "Quarterly year-over-year EPS growth",
            "parameters": criteria_config["C"]["quarterly_growth_threshold"]
        },
        "A": {
            "name": "Annual Earnings Growth",
            "description": "Year-over-year EPS growth",
            "parameters": criteria_config["A"]["annual_growth_threshold"]
        },
        "N": {
            "name": "New High",
            "description": "52-week high lookback period",
            "parameters": criteria_config["N"]["lookback_period"]
        },
        "S": {
            "name": "Supply/Demand",
            "description": "Volume factor above average vol",
            "parameters": criteria_config["S"]["volume_factor"]
        },
        "L": {
            "name": "Leader/Laggard",
            "description": "(stock_return - market_return) > threshold",
            "parameters": criteria_config["L"]["return_diff_threshold"]
        },
        "I": {
            "name": "Institutional Sponsorship",
            "description": "A/D metric above threshold",
            "parameters": (
                criteria_config["I"]["lookback_period"],
                criteria_config["I"]["ad_ratio_threshold"]
            )
        },
        "M": {
            "name": "Market Direction",
            "description": "50-day MA > 200-day MA",
            "parameters": "MA cross logic"
        }
    }

    return proxies_df, top_stocks_df, financials_df, canslim_criteria_dict
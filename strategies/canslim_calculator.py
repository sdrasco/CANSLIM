# strategies/canslim_calculator.py

import pandas as pd
import logging
import warnings
from config.settings import MARKET_PROXY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def calculate_m(market_only_df: pd.DataFrame, criteria_config: dict) -> pd.DataFrame:
    if "close" not in market_only_df.columns:
        logger.error("Market proxy data missing 'close' column required for M computation.")
        return market_only_df

    market_only_df = market_only_df.sort_values("date")
    market_only_df["50_MA"] = market_only_df["close"].rolling(50, min_periods=1).mean()
    market_only_df["200_MA"] = market_only_df["close"].rolling(200, min_periods=1).mean()

    # M criteria by default checks if 50-MA > 200-MA
    use_ma_cross = criteria_config["M"].get("use_ma_cross", True)
    if use_ma_cross:
        #market_only_df["M"] = (market_only_df["close"] > market_only_df["50_MA"]) & \
        #                      (market_only_df["50_MA"] > market_only_df["200_MA"])
        market_only_df["M"] = market_only_df["50_MA"] > market_only_df["200_MA"]
    else:
        # If a different logic is specified, implement it here
        # For now, just default to True if not using MA cross logic
        market_only_df["M"] = True

    return market_only_df


def compute_c_a_from_financials(financials_df: pd.DataFrame, criteria_config: dict):
    """
    Compute C and A indicators from the financials data:
    - C: Quarterly EPS growth >= quarterly_growth_threshold (default 0.25)
    - A: Annual EPS growth >= annual_growth_threshold (default 0.20)

    Returns a DataFrame with columns: ticker, end_date, C, A.
    """
    required = {"ticker", "timeframe", "fiscal_year", "fiscal_period", "diluted_eps", "end_date"}
    if not required.issubset(financials_df.columns):
        missing = required - set(financials_df.columns)
        logger.error(f"Financials data missing required columns: {missing}")
        return pd.DataFrame(columns=["ticker", "end_date", "C", "A"])

    c_thresh = criteria_config["C"].get("quarterly_growth_threshold", 0.25)
    a_thresh = criteria_config["A"].get("annual_growth_threshold", 0.20)

    logger.debug("Starting computation of C and A from financials.")

    # Process Quarterly (C)
    quarterly = financials_df[financials_df["timeframe"] == "quarterly"].copy()
    quarterly.sort_values(["ticker", "fiscal_period", "fiscal_year"], inplace=True)
    quarterly["prev_year_eps"] = quarterly.groupby(["ticker", "fiscal_period"])["diluted_eps"].shift(1)
    quarterly["C"] = ((quarterly["diluted_eps"] - quarterly["prev_year_eps"]) / quarterly["prev_year_eps"].abs()) >= c_thresh
    c_true_count = quarterly["C"].sum()
    logger.debug(f"C: Found {c_true_count} rows with quarterly EPS growth >= {c_thresh*100}%")

    # Process Annual (A)
    annual = financials_df[financials_df["timeframe"] == "annual"].copy()
    annual.sort_values(["ticker", "fiscal_year"], inplace=True)
    annual["prev_year_eps"] = annual.groupby("ticker")["diluted_eps"].shift(1)
    annual["A_ratio"] = (annual["diluted_eps"] - annual["prev_year_eps"]) / annual["prev_year_eps"].abs()
    annual["A"] = annual["A_ratio"] >= a_thresh
    a_true_count = annual["A"].sum()
    logger.debug(f"A: Found {a_true_count} rows with annual EPS growth >= {a_thresh*100}%")

    q_ca = quarterly[["ticker", "end_date", "C"]].drop_duplicates(["ticker", "end_date"])
    a_ca = annual[["ticker", "end_date", "A"]].drop_duplicates(["ticker", "end_date"])

    ca_df = pd.merge(q_ca, a_ca, on=["ticker", "end_date"], how="outer")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        ca_df["C"] = ca_df["C"].fillna(False).astype(bool)
        ca_df["A"] = ca_df["A"].fillna(False).astype(bool)

    ca_rows = len(ca_df)
    ca_c_true = ca_df["C"].sum()
    ca_a_true = ca_df["A"].sum()
    logger.debug(f"Final CA DF: {ca_rows} rows, C=True in {ca_c_true} rows, A=True in {ca_a_true} rows.")

    return ca_df


def calculate_nsli(top_stocks_df: pd.DataFrame, market_only: pd.DataFrame, criteria_config: dict) -> pd.DataFrame:
    required_cols = {"ticker", "date", "close", "open", "volume"}
    if not required_cols.issubset(top_stocks_df.columns):
        missing = required_cols - set(top_stocks_df.columns)
        logger.error(f"Top stocks data missing required columns: {missing}")
        return top_stocks_df

    top_stocks_df = top_stocks_df.sort_values(["ticker", "date"])
    market_only = market_only.sort_values("date")

    # Compute market daily returns
    market_only["market_return"] = market_only["close"].pct_change().fillna(0)

    # Compute each stock's daily returns
    top_stocks_df["stock_return"] = top_stocks_df.groupby("ticker")["close"].pct_change().fillna(0)

    # Merge market returns into top_stocks_df by date
    top_stocks_df = top_stocks_df.merge(market_only[["date", "market_return"]], on="date", how="left")

    # N: New High over a lookback period
    lookback_period = criteria_config["N"].get("lookback_period", 252)
    top_stocks_df["52_week_high"] = top_stocks_df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(lookback_period, min_periods=1).max()
    )
    top_stocks_df["N"] = top_stocks_df["close"] >= top_stocks_df["52_week_high"]

    # S: volume >= volume_factor * 50-day average
    s_factor = criteria_config["S"].get("volume_factor", 1.5)
    top_stocks_df["50_day_vol_avg"] = top_stocks_df.groupby("ticker")["volume"].transform(
        lambda x: x.rolling(50, min_periods=1).mean()
    )
    top_stocks_df["S"] = top_stocks_df["volume"] >= top_stocks_df["50_day_vol_avg"] * s_factor

    # L: stock_return > market_return (+ optional threshold)
    l_diff = criteria_config["L"].get("return_diff_threshold", 0.0)
    top_stocks_df["L"] = (top_stocks_df["stock_return"] - top_stocks_df["market_return"]) > l_diff

    # I: close > open and volume > volume_factor * 50-day avg
    i_factor = criteria_config["I"].get("volume_factor", 1.5)
    top_stocks_df["I"] = (top_stocks_df["close"] > top_stocks_df["open"]) & (
        top_stocks_df["volume"] > top_stocks_df["50_day_vol_avg"] * i_factor
    )

    return top_stocks_df


def merge_ca_into_top_stocks(top_stocks_df: pd.DataFrame, ca_df: pd.DataFrame) -> pd.DataFrame:
    required = {"ticker", "end_date", "C", "A"}
    if not required.issubset(ca_df.columns):
        missing = required - set(ca_df.columns)
        logger.error(f"CA Data missing required columns: {missing}")
        top_stocks_df["C"] = False
        top_stocks_df["A"] = False
        return top_stocks_df

    top_stocks_df = top_stocks_df.sort_values(["ticker", "date"])
    ca_df = ca_df.sort_values(["ticker", "end_date"])

    result_parts = []
    for tkr, group in top_stocks_df.groupby("ticker", group_keys=False):
        ca_sub = ca_df[ca_df["ticker"] == tkr]
        if ca_sub.empty:
            group["C"] = False
            group["A"] = False
        else:
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

    top_stocks_df = pd.concat(result_parts, ignore_index=True)
    return top_stocks_df


def calculate_canslim_indicators(proxies_df: pd.DataFrame,
                                 top_stocks_df: pd.DataFrame,
                                 financials_df: pd.DataFrame,
                                 criteria_config=None):
    """
    Calculate CANSLIM indicators. criteria_config can be used to parameterize thresholds.

    Returns:
    proxies_df, top_stocks_df, financials_df, canslim_criteria_dict
    """

    # Defaults if not provided
    if criteria_config is None:
        criteria_config = {
            "C": {"quarterly_growth_threshold": 0.1},
            "A": {"annual_growth_threshold": 0.1},
            "N": {"lookback_period": 252},
            "S": {"volume_factor": 1.25},
            "L": {"return_diff_threshold": 0.0},
            "I": {"volume_factor": 1.25},
            "M": {"use_ma_cross": True}
        }

    logger.info("Calculating M in market proxy data...")
    market_only = proxies_df[proxies_df["ticker"] == MARKET_PROXY].copy()
    market_only = calculate_m(market_only, criteria_config)

    proxies_df = proxies_df.drop(columns=["50_MA", "200_MA", "M"], errors="ignore")
    proxies_df = proxies_df.merge(market_only[["date", "50_MA", "200_MA", "M"]],
                                  on="date", how="left")
    proxies_df["M"] = proxies_df["M"].fillna(False).astype(bool)

    logger.info("Computing C and A from financial data...")
    ca_df = compute_c_a_from_financials(financials_df, criteria_config)

    logger.info("Calculating N, S, L, I in top stocks data...")
    top_stocks_df = calculate_nsli(top_stocks_df, market_only, criteria_config)

    logger.info("Merging C and A into top stocks data...")
    top_stocks_df = merge_ca_into_top_stocks(top_stocks_df, ca_df)

    logger.info("Calculating CANSLI_all column...")
    required_cansli_cols = ["C", "A", "N", "S", "L", "I"]
    missing_cansli = [col for col in required_cansli_cols if col not in top_stocks_df.columns]
    if missing_cansli:
        logger.error(f"Missing some CANSLI columns: {missing_cansli}")
        top_stocks_df["CANSLI_all"] = False
    else:
        top_stocks_df["CANSLI_all"] = (top_stocks_df["C"] &
                                       top_stocks_df["A"] &
                                       top_stocks_df["N"] &
                                       top_stocks_df["S"] &
                                       top_stocks_df["L"] &
                                       top_stocks_df["I"])

    logger.info("CANSLIM indicators computed.")

    # Prepare a criteria dictionary for reporting with succinct parameters
    canslim_criteria_dict = {
        "C": {
            "name": "Current Quarterly Earnings",
            "description": "Quarterly year-over-year EPS growth",
            "parameters": criteria_config["C"]["quarterly_growth_threshold"]
        },
        "A": {
            "name": "Annual Earnings Growth",
            "description": "Year-over-year growth of EPS for the entire year",
            "parameters": criteria_config["A"]["annual_growth_threshold"]
        },
        "N": {
            "name": "New High",
            "description": "During lookback period (in days)",
            "parameters": criteria_config["N"]["lookback_period"]
        },
        "S": {
            "name": "Supply and Demand proxy",
            "description": "volume / (50-day avg volume) > threshold",
            "parameters": criteria_config["S"]["volume_factor"]
        },
        "L": {
            "name": "Leader or Laggard proxy",
            "description": "(stock return) >  (market return) + excess",
            "parameters": criteria_config["L"]["return_diff_threshold"]
        },
        "I": {
            "name": "Institutional Sponsorship proxy",
            "description": "(close > open) and S",
            "parameters": criteria_config["I"]["volume_factor"]
        },
        "M": {
            "name": "Market Direction",
            "description": "Criteria for bull market",
            #"parameters": 'close > 50-day > 200-day'
            "parameters": '50-day > 200-day'
        }
    }

    return proxies_df, top_stocks_df, financials_df, canslim_criteria_dict
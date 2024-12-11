# data/canslim_calculator.py

import pandas as pd
import logging
import warnings
from config.settings import MARKET_PROXY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def calculate_m(market_only_df: pd.DataFrame) -> pd.DataFrame:
    if "close" not in market_only_df.columns:
        logger.error("Market proxy data missing 'close' column required for M computation.")
        return market_only_df

    market_only_df = market_only_df.sort_values("date")
    market_only_df["50_MA"] = market_only_df["close"].rolling(50, min_periods=1).mean()
    market_only_df["200_MA"] = market_only_df["close"].rolling(200, min_periods=1).mean()
    market_only_df["M"] = (market_only_df["50_MA"] > market_only_df["200_MA"])
    # market_only_df["M"] = (market_only_df["close"] > market_only_df["50_MA"]) & \
    #                       (market_only_df["50_MA"] > market_only_df["200_MA"])
    return market_only_df

def compute_c_a_from_financials(financials_df: pd.DataFrame):
    """
    Compute C and A indicators from the financials data:
    - C (quarterly EPS growth >= 25% yoy)
    - A (annual EPS growth >= 20% yoy)

    Uses 'end_date' as the reporting period end date column.

    Returns a DataFrame with columns: ticker, end_date, C, A.
    """
    required = {"ticker", "timeframe", "fiscal_year", "fiscal_period", "diluted_eps", "end_date"}
    if not required.issubset(financials_df.columns):
        missing = required - set(financials_df.columns)
        logger.error(f"Financials data missing required columns: {missing}")
        return pd.DataFrame(columns=["ticker", "end_date", "C", "A"])

    logger.debug("Starting computation of C and A from financials.")

    # Process Quarterly (C)
    quarterly = financials_df[financials_df["timeframe"] == "quarterly"].copy()
    if quarterly.empty:
        logger.debug("No quarterly data found.")
    else:
        logger.debug(f"Quarterly data shape: {quarterly.shape}")
    quarterly.sort_values(["ticker", "fiscal_period", "fiscal_year"], inplace=True)

    # Group by ticker and fiscal_period for prev_year_eps
    quarterly["prev_year_eps"] = quarterly.groupby(["ticker", "fiscal_period"])["diluted_eps"].shift(1)
    # Calculate C
    quarterly["C"] = ((quarterly["diluted_eps"] - quarterly["prev_year_eps"]) / quarterly["prev_year_eps"].abs()) >= 0.1 # suggested 0.25

    # Check how many rows have C = True
    c_true_count = quarterly["C"].sum()
    logger.debug(f"C: Found {c_true_count} rows with quarterly EPS growth >= 25%")

    # Process Annual (A)
    annual = financials_df[financials_df["timeframe"] == "annual"].copy()
    if annual.empty:
        logger.debug("No annual data found.")
    else:
        logger.debug(f"Annual data shape: {annual.shape}")
    annual.sort_values(["ticker", "fiscal_year"], inplace=True)

    # Group by ticker for prev_year_eps
    annual["prev_year_eps"] = annual.groupby("ticker")["diluted_eps"].shift(1)

    # Let's debug what prev_year_eps looks like
    no_prev_year = annual["prev_year_eps"].isna().sum()
    logger.debug(f"A: Out of {len(annual)} annual rows, {no_prev_year} have no prev_year_eps (first year of data?).")

    # Compute A as EPS growth >= 20%
    annual["A_ratio"] = (annual["diluted_eps"] - annual["prev_year_eps"]) / annual["prev_year_eps"].abs()
    # If prev_year_eps is zero or NaN, that could cause division by zero or NaN results. Check that:
    invalid_ratios = annual["A_ratio"].isna().sum()
    logger.debug(f"A: {invalid_ratios} rows have NaN ratio (likely due to missing prev_year_eps or zero EPS).")

    annual["A"] = annual["A_ratio"] >= 0.10 # suggested 0.2
    a_true_count = annual["A"].sum()
    logger.debug(f"A: Found {a_true_count} rows with annual EPS growth >= 20%")

    # Sample debug: print a few rows where A is True
    a_true_rows = annual[annual["A"]].head(5)
    logger.debug(f"Sample rows with A=True:\n{a_true_rows[['ticker','fiscal_year','diluted_eps','prev_year_eps','A_ratio']].to_string(index=False)}")

    q_ca = quarterly[["ticker", "end_date", "C"]].drop_duplicates(["ticker", "end_date"])
    a_ca = annual[["ticker", "end_date", "A"]].drop_duplicates(["ticker", "end_date"])

    ca_df = pd.merge(q_ca, a_ca, on=["ticker", "end_date"], how="outer")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        ca_df["C"] = ca_df["C"].fillna(False).astype(bool)
        ca_df["A"] = ca_df["A"].fillna(False).astype(bool)

    # More debugging: How many rows end up in ca_df and how many are True?
    ca_rows = len(ca_df)
    ca_c_true = ca_df["C"].sum()
    ca_a_true = ca_df["A"].sum()
    logger.debug(f"Final CA DF: {ca_rows} rows, with C=True in {ca_c_true} rows and A=True in {ca_a_true} rows.")

    return ca_df

def calculate_nsli(top_stocks_df: pd.DataFrame, market_only: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"ticker", "date", "close", "open", "volume"}
    if not required_cols.issubset(top_stocks_df.columns):
        missing = required_cols - set(top_stocks_df.columns)
        logger.error(f"Top stocks data missing required columns: {missing}")
        return top_stocks_df

    # Sort both DataFrames by date
    top_stocks_df = top_stocks_df.sort_values(["ticker", "date"])
    market_only = market_only.sort_values("date")

    # Compute market daily returns
    market_only["market_return"] = market_only["close"].pct_change().fillna(0)

    # Compute each stock's daily returns
    # group by ticker and compute pct_change in close
    top_stocks_df["stock_return"] = top_stocks_df.groupby("ticker")["close"].pct_change().fillna(0)

    # Merge market returns into top_stocks_df by date
    top_stocks_df = top_stocks_df.merge(market_only[["date", "market_return"]], on="date", how="left")

    # N: close at new 52-week high
    top_stocks_df["52_week_high"] = top_stocks_df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(252, min_periods=1).max()
    )
    top_stocks_df["N"] = top_stocks_df["close"] >= top_stocks_df["52_week_high"]

    # S: volume >= 1.5 * 50-day average
    top_stocks_df["50_day_vol_avg"] = top_stocks_df.groupby("ticker")["volume"].transform(
        lambda x: x.rolling(50, min_periods=1).mean()
    )
    top_stocks_df["S"] = top_stocks_df["volume"] >= top_stocks_df["50_day_vol_avg"] * 1.5

    # L: stock_return > market_return?
    # If stock outperformed market today, L = True, else False
    top_stocks_df["L"] = top_stocks_df["stock_return"] > top_stocks_df["market_return"]

    # I: close > open and volume spike
    top_stocks_df["I"] = (top_stocks_df["close"] > top_stocks_df["open"]) & (
        top_stocks_df["volume"] > top_stocks_df["50_day_vol_avg"] * 1.5
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
                                 financials_df: pd.DataFrame):
    logger.info("Calculating M in market proxy data...")
    market_only = proxies_df[proxies_df["ticker"] == MARKET_PROXY].copy()
    market_only = calculate_m(market_only)

    proxies_df = proxies_df.drop(columns=["50_MA", "200_MA", "M"], errors="ignore")
    proxies_df = proxies_df.merge(market_only[["date","50_MA","200_MA","M"]],
                                  on="date", how="left")
    proxies_df["M"] = proxies_df["M"].fillna(False).astype(bool)

    logger.info("Computing C and A from financial data...")
    ca_df = compute_c_a_from_financials(financials_df)

    logger.info("Calculating N, S, L, I in top stocks data...")
    top_stocks_df = calculate_nsli(top_stocks_df, market_only)

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
    return proxies_df, top_stocks_df, financials_df
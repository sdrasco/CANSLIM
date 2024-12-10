# data/canslim_calculator.py

import pandas as pd
import logging
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def calculate_m(market_proxy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the M indicator for the market proxy data on a daily basis.
    We first compute 50-day and 200-day moving averages on the close price, 
    then determine M based on close > 50_MA > 200_MA.
    """
    if "close" not in market_proxy_df.columns:
        logger.error("Market proxy data missing 'close' column required for M computation.")
        return market_proxy_df

    # Compute rolling means for M calculation
    market_proxy_df = market_proxy_df.sort_values("date")  # Ensure sorted by date
    market_proxy_df["50_MA"] = market_proxy_df["close"].rolling(50, min_periods=1).mean()
    market_proxy_df["200_MA"] = market_proxy_df["close"].rolling(200, min_periods=1).mean()

    market_proxy_df["M"] = (market_proxy_df["close"] > market_proxy_df["50_MA"]) & \
                           (market_proxy_df["50_MA"] > market_proxy_df["200_MA"])
    return market_proxy_df


def compute_c_a_from_financials(financials_df: pd.DataFrame):
    """
    Compute C and A indicators from financials:
    - C (quarterly EPS growth >= 25% yoy)
    - A (annual EPS growth >= 20% yoy)
    
    Returns a DataFrame with columns: ticker, report_date, C, A.
    """
    required = {"ticker", "timeframe", "fiscal_year", "fiscal_period", "diluted_eps", "report_date"}
    if not required.issubset(financials_df.columns):
        logger.error(f"Financials data missing required columns: {required - set(financials_df.columns)}")
        return pd.DataFrame(columns=["ticker", "report_date", "C", "A"])

    # Quarterly C
    quarterly = financials_df[financials_df["timeframe"] == "quarterly"].copy()
    quarterly.sort_values(["ticker", "fiscal_period", "fiscal_year"], inplace=True)
    quarterly["prev_year_eps"] = quarterly.groupby(["ticker", "fiscal_period"])["diluted_eps"].shift(1)
    quarterly["C"] = ((quarterly["diluted_eps"] - quarterly["prev_year_eps"]) / quarterly["prev_year_eps"].abs()) >= 0.25

    # Annual A
    annual = financials_df[financials_df["timeframe"] == "annual"].copy()
    annual.sort_values(["ticker", "fiscal_year"], inplace=True)
    annual["prev_year_eps"] = annual.groupby("ticker")["diluted_eps"].shift(1)
    annual["A"] = ((annual["diluted_eps"] - annual["prev_year_eps"]) / annual["prev_year_eps"].abs()) >= 0.20

    # Merge quarterly and annual results
    q_ca = quarterly[["ticker", "report_date", "C"]].drop_duplicates(["ticker", "report_date"])
    a_ca = annual[["ticker", "report_date", "A"]].drop_duplicates(["ticker", "report_date"])

    ca_df = pd.merge(q_ca, a_ca, on=["ticker", "report_date"], how="outer")
    ca_df["C"] = ca_df["C"].fillna(False)
    ca_df["A"] = ca_df["A"].fillna(False)

    return ca_df


def calculate_nsli(top_stocks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute N, S, L, I indicators for top stocks data on a daily basis.
    - N: close at new 52-week high
    - S: volume today >= 1.5 * 50-day vol avg
    - L: relative_strength > 1.0 (if relative_strength available)
    - I: up day with volume spike
    """
    required_cols = {"ticker", "date", "close", "open", "volume"}
    if not required_cols.issubset(top_stocks_df.columns):
        missing = required_cols - set(top_stocks_df.columns)
        logger.error(f"Top stocks data missing required columns: {missing}")
        return top_stocks_df

    top_stocks_df = top_stocks_df.sort_values(["ticker", "date"])

    # N: 52-week high (252 trading days ~ 1 year)
    top_stocks_df["52_week_high"] = top_stocks_df.groupby("ticker")["close"].transform(lambda x: x.rolling(252, min_periods=1).max())
    top_stocks_df["N"] = top_stocks_df["close"] >= top_stocks_df["52_week_high"]

    # S: 50-day volume average
    top_stocks_df["50_day_vol_avg"] = top_stocks_df.groupby("ticker")["volume"].transform(lambda x: x.rolling(50, min_periods=1).mean())
    top_stocks_df["S"] = top_stocks_df["volume"] >= top_stocks_df["50_day_vol_avg"] * 1.5

    # L: relative_strength > 1.0 if available
    if "relative_strength" in top_stocks_df.columns:
        top_stocks_df["L"] = top_stocks_df["relative_strength"] > 1.0
    else:
        logger.warning("relative_strength not found, setting L = False.")
        top_stocks_df["L"] = False

    # I: up day with volume spike
    top_stocks_df["I"] = (top_stocks_df["close"] > top_stocks_df["open"]) & (top_stocks_df["volume"] > top_stocks_df["50_day_vol_avg"] * 1.5)

    return top_stocks_df


def merge_ca_into_top_stocks(top_stocks_df: pd.DataFrame, ca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge C and A values into top_stocks_df daily data using the last known financial report date.
    """
    required = {"ticker", "report_date", "C", "A"}
    if not required.issubset(ca_df.columns):
        logger.error(f"CA Data missing required columns: {required - set(ca_df.columns)}")
        top_stocks_df["C"] = False
        top_stocks_df["A"] = False
        return top_stocks_df

    top_stocks_df = top_stocks_df.sort_values(["ticker", "date"])
    ca_df = ca_df.sort_values(["ticker", "report_date"])

    result_parts = []
    for tkr, group in top_stocks_df.groupby("ticker", group_keys=False):
        ca_sub = ca_df[ca_df["ticker"] == tkr]
        if ca_sub.empty:
            group["C"] = False
            group["A"] = False
        else:
            # merge_asof by date on report_date
            group = pd.merge_asof(
                group.sort_values("date"),
                ca_sub.sort_values("report_date").drop(columns="ticker"),
                left_on="date", right_on="report_date",
                direction="backward"
            )
            group["C"] = group["C"].fillna(False)
            group["A"] = group["A"].fillna(False)
        result_parts.append(group)

    top_stocks_df = pd.concat(result_parts, ignore_index=True)
    return top_stocks_df


def calculate_canslim_indicators(market_proxy_df: pd.DataFrame,
                                 top_stocks_df: pd.DataFrame,
                                 financials_df: pd.DataFrame):
    """
    High-level function to compute indicators:
    - M in market_proxy_df (computed from close, adding 50_MA, 200_MA)
    - N, S, L, I in top_stocks_df (computed directly)
    - C, A derived from financials but merged as daily columns into top_stocks_df
    - Finally, add a CANSLI_all column that is True if C, A, N, S, L, I are all True.
    
    Returns:
      market_proxy_df (with M),
      top_stocks_df (with C, A, N, S, L, I, CANSLI_all),
      financials_df (unchanged)
    """
    logger.info("Calculating M in market proxy data...")
    market_proxy_df = calculate_m(market_proxy_df)

    logger.info("Computing C and A from financial data...")
    ca_df = compute_c_a_from_financials(financials_df)

    logger.info("Calculating N, S, L, I in top stocks data...")
    top_stocks_df = calculate_nsli(top_stocks_df)

    logger.info("Merging C and A into top stocks data...")
    top_stocks_df = merge_ca_into_top_stocks(top_stocks_df, ca_df)

    logger.info("Calculating CANSLI_all column...")
    # CANSLI_all is True if all C, A, N, S, L, I are True
    required_cansli_cols = ["C", "A", "N", "S", "L", "I"]
    missing_cansli = [col for col in required_cansli_cols if col not in top_stocks_df.columns]
    if missing_cansli:
        logger.error(f"Missing some CANSLI columns: {missing_cansli}")
        # Just set CANSLI_all to False if we are missing something
        top_stocks_df["CANSLI_all"] = False
    else:
        top_stocks_df["CANSLI_all"] = (top_stocks_df["C"] &
                                       top_stocks_df["A"] &
                                       top_stocks_df["N"] &
                                       top_stocks_df["S"] &
                                       top_stocks_df["L"] &
                                       top_stocks_df["I"])

    logger.info("CANSLIM indicators computed.")
    return market_proxy_df, top_stocks_df, financials_df
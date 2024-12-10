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
    """
    Compute the M indicator for the market proxy (MARKET_PROXY) data on a daily basis.
    We first compute 50-day and 200-day moving averages on the close price,
    then determine M based on close > 50_MA > 200_MA.

    market_only_df should contain only the MARKET_PROXY ticker rows.
    """
    if "close" not in market_only_df.columns:
        logger.error("Market proxy data missing 'close' column required for M computation.")
        return market_only_df

    market_only_df = market_only_df.sort_values("date")  # Ensure sorted by date
    market_only_df["50_MA"] = market_only_df["close"].rolling(50, min_periods=1).mean()
    market_only_df["200_MA"] = market_only_df["close"].rolling(200, min_periods=1).mean()

    market_only_df["M"] = (market_only_df["close"] > market_only_df["50_MA"]) & \
                          (market_only_df["50_MA"] > market_only_df["200_MA"])
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

    quarterly = financials_df[financials_df["timeframe"] == "quarterly"].copy()
    quarterly.sort_values(["ticker", "fiscal_period", "fiscal_year"], inplace=True)
    quarterly["prev_year_eps"] = quarterly.groupby(["ticker", "fiscal_period"])["diluted_eps"].shift(1)
    quarterly["C"] = ((quarterly["diluted_eps"] - quarterly["prev_year_eps"]) / quarterly["prev_year_eps"].abs()) >= 0.25

    annual = financials_df[financials_df["timeframe"] == "annual"].copy()
    annual.sort_values(["ticker", "fiscal_year"], inplace=True)
    annual["prev_year_eps"] = annual.groupby("ticker")["diluted_eps"].shift(1)
    annual["A"] = ((annual["diluted_eps"] - annual["prev_year_eps"]) / annual["prev_year_eps"].abs()) >= 0.2 # 0.2 suggested

    q_ca = quarterly[["ticker", "end_date", "C"]].drop_duplicates(["ticker", "end_date"])
    a_ca = annual[["ticker", "end_date", "A"]].drop_duplicates(["ticker", "end_date"])

    ca_df = pd.merge(q_ca, a_ca, on=["ticker", "end_date"], how="outer")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        ca_df["C"] = ca_df["C"].fillna(False).astype(bool)
        ca_df["A"] = ca_df["A"].fillna(False).astype(bool)

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

    top_stocks_df["52_week_high"] = top_stocks_df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(252, min_periods=1).max()
    )
    top_stocks_df["N"] = top_stocks_df["close"] >= top_stocks_df["52_week_high"]

    top_stocks_df["50_day_vol_avg"] = top_stocks_df.groupby("ticker")["volume"].transform(
        lambda x: x.rolling(50, min_periods=1).mean()
    )
    top_stocks_df["S"] = top_stocks_df["volume"] >= top_stocks_df["50_day_vol_avg"] * 1.5

    if "relative_strength" in top_stocks_df.columns:
        top_stocks_df["L"] = top_stocks_df["relative_strength"] > 1.0 # 1.0 suggested
    else:
        logger.warning("relative_strength not found, setting L = False.")
        top_stocks_df["L"] = False

    top_stocks_df["I"] = (top_stocks_df["close"] > top_stocks_df["open"]) & (
        top_stocks_df["volume"] > top_stocks_df["50_day_vol_avg"] * 1.5
    )

    return top_stocks_df

def merge_ca_into_top_stocks(top_stocks_df: pd.DataFrame, ca_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge C and A values into top_stocks_df daily data using the last known financial end_date.
    """
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
    """
    High-level function to compute indicators:
    - M in proxies_df (filtered for MARKET_PROXY)
    - N, S, L, I in top_stocks_df (computed directly)
    - C, A derived from financials but merged into top_stocks_df
    - Finally, add a CANSLI_all column that is True if C, A, N, S, L, I are all True.

    Returns:
      proxies_df (with M on MARKET_PROXY rows),
      top_stocks_df (with C, A, N, S, L, I, CANSLI_all),
      financials_df (unchanged)
    """

    logger.info("Calculating M in market proxy data...")
    # Filter the MARKET_PROXY ticker from proxies_df
    market_only = proxies_df[proxies_df["ticker"] == MARKET_PROXY].copy()
    market_only = calculate_m(market_only)

    # Merge the M and MA columns back into proxies_df
    # Drop old M/MA columns from proxies_df if they exist, then merge
    proxies_df = proxies_df.drop(columns=["50_MA", "200_MA", "M"], errors="ignore")
    proxies_df = proxies_df.merge(market_only[["date","50_MA","200_MA","M"]],
                                  on="date", how="left")

    # For rows that are not MARKET_PROXY, M will be NaN. That's expected.
    # They don't need M, but let's fill them with False for consistency.
    proxies_df["M"] = proxies_df["M"].fillna(False).astype(bool)

    logger.info("Computing C and A from financial data...")
    ca_df = compute_c_a_from_financials(financials_df)

    logger.info("Calculating N, S, L, I in top stocks data...")
    top_stocks_df = calculate_nsli(top_stocks_df)

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
                                       top_stocks_df["N"] &
                                       top_stocks_df["S"] &
                                       top_stocks_df["I"])
        # top_stocks_df["CANSLI_all"] = (top_stocks_df["C"] &
        #                                top_stocks_df["A"] &
        #                                top_stocks_df["N"] &
        #                                top_stocks_df["S"] &
        #                                top_stocks_df["L"] &
        #                                top_stocks_df["I"])

    logger.info("CANSLIM indicators computed.")
    return proxies_df, top_stocks_df, financials_df
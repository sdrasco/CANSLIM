# clean_spy.py

"""
clean_spy.py

Removes invalid tickers (those that do not exist in top_stocks_df on or before a given date)
from an S&P 500 membership snapshot file, and saves a cleaned version.
"""

import logging
import pandas as pd
from pathlib import Path

# Example: If you have these in your codebase:
from data.data_loaders import load_top_stocks  # or whichever module actually loads your polygon-based top_stocks data
from config.settings import DATA_DIR
from utils.logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

def clean_sp500_snapshot(snapshot_df: pd.DataFrame, top_stocks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes any tickers that don't actually appear in the top_stocks data
    on or before the given snapshot date. Returns a new DataFrame where
    the 'tickers' column has been filtered accordingly.

    snapshot_df: must have a DatetimeIndex named 'date'
                 and a 'tickers' column with comma-delimited tickers.
    top_stocks_df: must have a DatetimeIndex (date) plus 'ticker' column
                   for each row, representing actual price data coverage.
    """
    snapshot_df = snapshot_df.copy()

    # For each ticker, figure out the earliest date it appears in top_stocks_df
    # so we can skip tickers that didn't exist prior to that date.
    top_stocks_min_date = (
        top_stocks_df
        .reset_index()  # date becomes a normal column
        .groupby("ticker")["date"]
        .min()
        .to_dict()
    )

    new_rows = []
    for date_, row in snapshot_df.iterrows():
        raw_tickers = row["tickers"].split(",")
        cleaned_list = []
        for tkr in raw_tickers:
            tkr = tkr.strip()
            if not tkr:
                continue
            min_dt = top_stocks_min_date.get(tkr, None)
            # If min_dt is None => we have no record of that ticker in top_stocks
            # If min_dt > date_ => ticker didn't exist on or before date_ in top_stocks
            if min_dt is not None and pd.to_datetime(min_dt) <= date_:
                cleaned_list.append(tkr)

        if cleaned_list:
            row["tickers"] = ",".join(cleaned_list)
        else:
            # Could set to empty string if no valid tickers left for that date
            row["tickers"] = ""

        new_rows.append(row)

    cleaned_df = pd.DataFrame(new_rows, index=snapshot_df.index)
    return cleaned_df

def main():
    logger.info("Loading top_stocks data (Polygon-based) for reference...")
    top_stocks_df = load_top_stocks()  # Adjust if your data loader has different naming
    
    # Load the original snapshot
    snapshot_path = DATA_DIR / "sp_500_historic_snapshot.feather"
    if not snapshot_path.exists():
        logger.error(f"Cannot find snapshot file: {snapshot_path}")
        return
    
    logger.info(f"Reading snapshot from {snapshot_path}")
    snapshot_df = pd.read_feather(snapshot_path)
    
    # Convert to DatetimeIndex
    if "date" not in snapshot_df.columns:
        logger.error("Snapshot file missing 'date' column. Exiting.")
        return
    snapshot_df.dropna(subset=["date"], inplace=True)
    snapshot_df["date"] = pd.to_datetime(snapshot_df["date"]).dt.normalize()
    snapshot_df.set_index("date", drop=True, inplace=True)
    snapshot_df.sort_index(inplace=True)

    # Clean
    logger.info("Cleaning snapshot by removing tickers that have no price data in top_stocks_df on or before that date...")
    cleaned_df = clean_sp500_snapshot(snapshot_df, top_stocks_df)

    # Save results
    clean_feather = DATA_DIR / "sp_500_historic_snapshot_clean.feather"
    clean_csv = DATA_DIR / "sp_500_historic_snapshot_clean.csv"

    logger.info(f"Writing cleaned snapshot to {clean_feather} and {clean_csv}...")
    cleaned_df.reset_index().to_feather(clean_feather)
    cleaned_df.reset_index().to_csv(clean_csv, index=False)
    logger.info("Done. You can now use the cleaned snapshot file in your analysis.")

if __name__ == "__main__":
    main()
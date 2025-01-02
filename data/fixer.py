#!/usr/bin/env python3

import pandas as pd
import sys

def fix_sp500_snapshot(input_path: str, output_path: str):
    """
    Reads an S&P 500 snapshot Feather file, converts 'date' to datetime, sorts,
    and writes back out to a new Feather file.
    """
    print(f"Loading snapshot from {input_path}...")
    df = pd.read_feather(input_path)
    
    print("Converting 'date' column to datetime...")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    print("Dropping rows with NaN dates (if any)...")
    df.dropna(subset=["date"], inplace=True)

    print("Sorting by date and resetting index...")
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f"Writing updated snapshot to {output_path}...")
    df.to_feather(output_path)
    print("Done!")

if __name__ == "__main__":
    

    input_file = "sp_500_historic_snapshot.feather"
    output_file = "sp_500_historic_snapshot.feather.fixed"

    fix_sp500_snapshot(input_file, output_file)
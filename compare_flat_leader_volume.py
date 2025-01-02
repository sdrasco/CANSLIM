"""
compare_flat_leader_volume.py

Compares five strategies:
1) Market Only
2) Risk-Managed Market
3) Flattened SPY (Equal Weight)
4) LeaderSPY (Weights by 'L_measure')
5) VolumeSPY (Weights by 'quarterly_volume')

Stores:
 - final summary CSV (simple_strategies_comparison.csv)
 - daily timeseries CSV of all strategies (all_strategies_timeseries.csv)
 - pickled artifacts (simple_strategies_artifacts.pkl)
"""

import logging
import os
import csv
import pickle
import pandas as pd
from pathlib import Path

from config.settings import (
    DATA_DIR, INITIAL_FUNDS, REBALANCE_FREQUENCY, MARKET_PROXY,
    REPORT_DIR, START_DATE, END_DATE
)
from utils.logging_utils import configure_logging
from data.data_loaders import load_proxies, load_top_stocks, load_financials
from utils.calendar_utils import get_rebalance_dates
from utils.metrics import compute_performance_metrics
from backtesting.backtester import run_backtest

# import the enrichment utilities
from utils.enrichment_utils import enrich_with_L_and_volume, calculate_m

# Import the strategies from your refactored strategy_definitions
from strategies.strategy_definitions import (
    market_only_strategy,
    risk_managed_market_strategy,
    flattened_spy_strategy,
    leader_spy_strategy,
    volume_spy_strategy
)

def main():
    configure_logging()
    logger = logging.getLogger(__name__)

    try:
        # ------------------------------------------------------
        # 1) Load Data
        # ------------------------------------------------------
        proxies_df = load_proxies()      
        top_stocks_df = load_top_stocks()
        financials_df = load_financials()

        logger.info("Determining rebalance dates...")
        market_only_df = proxies_df[proxies_df["ticker"] == MARKET_PROXY]
        rebalance_dates = get_rebalance_dates(
            market_only_df,
            REBALANCE_FREQUENCY,
            start_date=START_DATE,
            end_date=END_DATE
        )
        if not rebalance_dates:
            logger.warning("No rebalance dates found.")
            return

        logger.info(f"Using rebalance dates: {rebalance_dates[:5]}... [truncated]")

        # ------------------------------------------------------
        # 2) Enrich top_stocks_df
        # ------------------------------------------------------
        logger.info("Enriching top_stocks_df with L_measure and quarterly_volume...")
        top_stocks_df = enrich_with_L_and_volume(
            top_stocks_df=top_stocks_df,
            market_proxy_df=market_only_df,
            l_window=20,                     
            volume_window=63
        )

        # ------------------------------------------------------
        # Enrich proxies_df with 'M'
        # ------------------------------------------------------
        logger.info("Enriching proxies with M...")
        proxies_df = calculate_m(proxies_df)

        # ------------------------------------------------------
        # 3) Load & refactor S&P 500 historic snapshot
        # ------------------------------------------------------
        sp500_snapshot_path = DATA_DIR / "sp_500_historic_snapshot.feather"
        if not sp500_snapshot_path.exists():
            logger.error(f"Missing file: {sp500_snapshot_path}")
            return

        sp500_snapshot_df = pd.read_feather(sp500_snapshot_path)
        sp500_snapshot_df.dropna(subset=["date"], inplace=True)
        if not pd.api.types.is_datetime64_any_dtype(sp500_snapshot_df["date"]):
            sp500_snapshot_df["date"] = pd.to_datetime(sp500_snapshot_df["date"])
        sp500_snapshot_df["date"] = sp500_snapshot_df["date"].dt.normalize()
        sp500_snapshot_df = sp500_snapshot_df.set_index("date", drop=True).sort_index()

        logger.info(
            f"Loaded sp500_snapshot with shape={sp500_snapshot_df.shape}, "
            f"index type={type(sp500_snapshot_df.index)} "
            f"({sp500_snapshot_df.index.dtype})"
        )

        # Prepare a data_dict
        data_dict = {
            "proxies_df": proxies_df,
            "top_stocks_df": top_stocks_df,
            "sp500_snapshot_df": sp500_snapshot_df
        }

        # ------------------------------------------------------
        # 4) Run and Compare 5 Strategies
        # ------------------------------------------------------
        strategies_artifacts = {}
        STRATEGY_ORDER = ["market", "risk", "flattened", "leader", "volume"]
        strategy_funcs = {
            "market": market_only_strategy,
            "risk": risk_managed_market_strategy,
            "flattened": flattened_spy_strategy,
            "leader": leader_spy_strategy,
            "volume": volume_spy_strategy
        }

        # Dictionary to hold each strategy's daily portfolio DataFrame
        all_histories = {}

        for strat_name in STRATEGY_ORDER:
            strat_func = strategy_funcs[strat_name]
            logger.info(f"Backtesting {strat_name.title()} strategy...")

            hist = run_backtest(
                strategy_func=strat_func,
                proxies_df=proxies_df,
                top_stocks_df=top_stocks_df,
                rebalance_dates=rebalance_dates,
                initial_funds=INITIAL_FUNDS,
                data_dict=data_dict
            )
            metrics = compute_performance_metrics(hist)

            # store in artifacts
            strategies_artifacts[f"{strat_name}_history"] = hist
            strategies_artifacts[f"{strat_name}_metrics"] = metrics

            # also keep a reference to the DataFrame for daily timeseries
            all_histories[strat_name] = hist

        # ------------------------------------------------------
        # 5) Store Comparison Results
        # ------------------------------------------------------
        summary_csv_path = REPORT_DIR / "simple_strategies_comparison.csv"
        if summary_csv_path.exists():
            os.remove(summary_csv_path)

        with open(summary_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["strategy_name", "final_value", "total_return"])
            for strat_name in STRATEGY_ORDER:
                hist = strategies_artifacts[f"{strat_name}_history"]
                final_val = hist["portfolio_value"].iloc[-1]
                total_ret = (final_val / INITIAL_FUNDS) - 1.0
                writer.writerow([strat_name, f"{final_val:.2f}", f"{total_ret:.4f}"])

        # (A) Also store the daily timeseries of each strategy's portfolio_value
        # in a single DataFrame
        daily_df = pd.DataFrame()
        for strat_name in STRATEGY_ORDER:
            hist = all_histories[strat_name].copy()
            # if 'date' isn't the index already, set it
            if "date" in hist.columns:
                hist.set_index("date", inplace=True)
            daily_df[strat_name] = hist["portfolio_value"]

        # Write out the combined daily timeseries
        timeseries_csv_path = REPORT_DIR / "all_strategies_timeseries.csv"
        if timeseries_csv_path.exists():
            os.remove(timeseries_csv_path)

        daily_df.sort_index(inplace=True)
        daily_df.to_csv(timeseries_csv_path)
        logger.info(f"Wrote daily portfolio timeseries to {timeseries_csv_path}")

        # (B) Pickle everything
        artifacts_path = REPORT_DIR / "simple_strategies_artifacts.pkl"
        with open(artifacts_path, "wb") as f:
            pickle.dump(strategies_artifacts, f)

        logger.info(f"Comparison complete. CSV => {summary_csv_path}")
        logger.info(f"Timeseries => {timeseries_csv_path}")
        logger.info(f"Artifacts => {artifacts_path}")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
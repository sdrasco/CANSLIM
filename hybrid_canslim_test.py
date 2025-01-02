"""
hybrid_canslim_test.py

Refactored to:
  1) Load and enrich data.
  2) Compare Market and Risk strategies (one-off), storing their results in 'strategies_artifacts'.
  3) Perform a grid search over Hybrid (C, A, L), computing:
     - final portfolio value
     - total_return
     - average (non-zero) pick count per run
  4) Write all runs to 'hybrid_gridsearch_results.csv', including 'avg_picks' for each run.
  5) Track the best run, store it in 'best_run.pkl', store Market/Risk in 'strategies_artifacts.pkl'.
  6) Avoid calling any reporting module. A separate script can use these artifacts.
"""

import logging
import pandas as pd
import csv
import pickle
import os
import numpy as np
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns

from config.settings import (
    DATA_DIR, INITIAL_FUNDS, REBALANCE_FREQUENCY, MARKET_PROXY, MONEY_MARKET_PROXY,
    REPORT_DIR, START_DATE, END_DATE
)
from utils.logging_utils import configure_logging
from data.data_loaders import load_proxies, load_top_stocks, load_financials
from utils.calendar_utils import get_rebalance_dates
from utils.metrics import compute_performance_metrics
from backtesting.backtester import run_backtest

from strategies.canslim_calculator import calculate_canslim_indicators
from strategies.strategy_definitions import (
    market_only_strategy,
    risk_managed_market_strategy,
    canslim_sp500_hybrid
)

def main():
    configure_logging()
    logger = logging.getLogger(__name__)

    try:
        # ------------------------------------------------------
        # 1) Load & Enrich Data
        # ------------------------------------------------------
        proxies_df = load_proxies()
        top_stocks_df = load_top_stocks()
        financials_df = load_financials()

        # Baseline config
        criteria_config = {
            "C": {"quarterly_growth_threshold": 0.05},
            "A": {"annual_growth_threshold": 0.05},
            "N": {"lookback_period": 252},
            "S": {"volume_factor": 1.25},
            "L": {"return_diff_threshold": 0.0},
            "I": {"lookback_period": 50, "ad_ratio_threshold": 1.25},
            "M": {"use_ma_cross": True}
        }

        logger.info("Enriching data with CANSLIM indicators...")
        enriched_proxies_df, enriched_top_stocks_df, _, _ = calculate_canslim_indicators(
            proxies_df=proxies_df.copy(),
            top_stocks_df=top_stocks_df.copy(),
            financials_df=financials_df.copy(),
            criteria_config=criteria_config
        )

        logger.info("Determining rebalance dates...")
        market_only_df = enriched_proxies_df[enriched_proxies_df["ticker"] == MARKET_PROXY]
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

        sp500_snapshot_path = DATA_DIR / "sp_500_historic_snapshot.feather"
        if not sp500_snapshot_path.exists():
            logger.error(f"Missing file: {sp500_snapshot_path}")
            return
        sp500_snapshot_df = (
            pd.read_feather(sp500_snapshot_path)
              .dropna(subset=["date"])
              .sort_values("date")
        )

        # ------------------------------------------------------
        # 2) Market-Only & Risk-Managed
        # ------------------------------------------------------
        logger.info("Backtesting Market-Only strategy...")
        market_history = run_backtest(
            strategy_func=market_only_strategy,
            proxies_df=enriched_proxies_df,
            top_stocks_df=enriched_top_stocks_df,
            rebalance_dates=rebalance_dates,
            initial_funds=INITIAL_FUNDS
        )
        market_metrics = compute_performance_metrics(market_history)

        logger.info("Backtesting Risk-Managed Market strategy...")
        risk_history = run_backtest(
            strategy_func=risk_managed_market_strategy,
            proxies_df=enriched_proxies_df,
            top_stocks_df=enriched_top_stocks_df,
            rebalance_dates=rebalance_dates,
            initial_funds=INITIAL_FUNDS
        )
        risk_metrics = compute_performance_metrics(risk_history)

        # Keep them in one dict so we can pickle
        strategies_artifacts = {
            "market_history": market_history,
            "market_metrics": market_metrics,
            "risk_history": risk_history,
            "risk_metrics": risk_metrics
        }

        # ------------------------------------------------------
        # 3) Grid Search Over Hybrid (C, A, L)
        # ------------------------------------------------------
        logger.info("Starting grid search for Hybrid strategy...")

        C_list = [0.05 + 0.05*i for i in range(15)]  # e.g. 0.05..0.75
        A_list = [0.05 + 0.05*i for i in range(10)]  # e.g. 0.05..0.50
        L_list = [0.00, 0.05, 0.10, 0.15]

        param_grid = []
        for c_v in C_list:
            for a_v in A_list:
                for l_v in L_list:
                    param_grid.append({"C": c_v, "A": a_v, "L": l_v})

        total_runs = len(param_grid)
        logger.info(f"Total combos: {total_runs}.")

        grid_csv_path = REPORT_DIR / "hybrid_gridsearch_results.csv"
        if grid_csv_path.exists():
            os.remove(grid_csv_path)

        # We'll add an "avg_picks" column to store average (non-zero) picks
        with open(grid_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["C","A","L","final_value","total_return","avg_picks"])

        best_run = None
        best_final_value = -float("inf")

        run_count = 0
        for params in param_grid:
            run_count += 1
            logger.info(f"=== Grid Search Run {run_count}/{total_runs} === {params}")

            run_criteria = {
                "C": {"quarterly_growth_threshold": params["C"]},
                "A": {"annual_growth_threshold": params["A"]},
                "N": {"lookback_period": 252},
                "S": {"volume_factor": 1.25},
                "L": {"return_diff_threshold": params["L"]},
                "I": {"lookback_period": 50, "ad_ratio_threshold": 1.25},
                "M": {"use_ma_cross": True}
            }

            gp_proxies, gp_tops, _, _ = calculate_canslim_indicators(
                proxies_df=proxies_df.copy(),
                top_stocks_df=top_stocks_df.copy(),
                financials_df=financials_df.copy(),
                criteria_config=run_criteria
            )

            # Data dict to track picks
            run_data_dict = {
                "sp500_snapshot_df": sp500_snapshot_df,
                "top_stocks_df": gp_tops
            }

            run_history = run_backtest(
                strategy_func=canslim_sp500_hybrid,
                proxies_df=gp_proxies,
                top_stocks_df=gp_tops,
                rebalance_dates=rebalance_dates,
                initial_funds=INITIAL_FUNDS,
                data_dict=run_data_dict
            )
            run_metrics = compute_performance_metrics(run_history)

            final_val = run_history["portfolio_value"].iloc[-1]
            total_ret = (final_val / INITIAL_FUNDS) - 1.0

            # Compute average picks for non-zero quarters
            pick_counts = run_data_dict.get("hybrid_pick_counts", [])
            pick_counts_nonzero = [pc for pc in pick_counts if pc > 0]
            if pick_counts_nonzero:
                avg_picks = sum(pick_counts_nonzero) / len(pick_counts_nonzero)
            else:
                avg_picks = 0.0

            logger.info(f"Result => final={final_val:.2f}, ret={total_ret:.2%}, avg_picks={avg_picks:.2f}")

            # Write run to CSV, including avg_picks
            with open(grid_csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    params["C"],
                    params["A"],
                    params["L"],
                    f"{final_val:.2f}",
                    f"{total_ret:.4f}",
                    f"{avg_picks:.2f}"
                ])

            # Track best
            if final_val > best_final_value:
                best_run = {
                    "params": params,
                    "history": run_history,
                    "metrics": run_metrics,
                    "data_dict": run_data_dict,
                    "avg_picks": avg_picks  # store for best run
                }
                best_final_value = final_val

        if best_run is None:
            logger.warning("No successful runs found.")
            return

        logger.info(f"Best run => params={best_run['params']}, final_val={best_final_value:.2f}, avg_picks={best_run['avg_picks']:.2f}")

        # ------------------------------------------------------
        # 4) Save artifacts, do NOT call a reporting function
        # ------------------------------------------------------
        logger.info("Saving artifacts so they can be used by a separate reporting script...")

        # (A) Save best_run
        best_run_path = REPORT_DIR / "best_run.pkl"
        with open(best_run_path, "wb") as f:
            pickle.dump(best_run, f)
        logger.info(f"Saved best_run to {best_run_path}")

        # (B) Save Market/Risk + best_run picks to strategies_artifacts
        strategies_artifacts["best_run_params"] = best_run["params"]
        strategies_artifacts["best_run_value"] = best_final_value
        strategies_artifacts["best_run_avg_picks"] = best_run["avg_picks"]

        artifacts_path = REPORT_DIR / "strategies_artifacts.pkl"
        with open(artifacts_path, "wb") as f:
            pickle.dump(strategies_artifacts, f)
        logger.info(f"Saved strategies artifacts (Market, Risk) to {artifacts_path}")

        logger.info(f"Grid CSV results stored at {grid_csv_path}")
        logger.info("Done. You can run a separate reporting script to generate HTML now.")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
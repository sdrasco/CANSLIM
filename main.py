# main.py

import logging
import os
import json
from pathlib import Path
import pandas as pd
import numpy as np
from itertools import product

from data.aggs_fetcher import AggregatesFetcher
from data.aggs_processor import AggregatesProcessor
from data.financials_fetcher import FinancialsFetcher
from config.settings import DATA_DIR, INITIAL_FUNDS, REBALANCE_FREQUENCY, MARKET_PROXY, MONEY_MARKET_PROXY, REPORT_DIR, START_DATE, END_DATE
from utils.logging_utils import configure_logging

from data.data_loaders import load_proxies, load_top_stocks, load_financials
from strategies.canslim_calculator import calculate_canslim_indicators
from utils.calendar_utils import get_rebalance_dates
from strategies.strategy_definitions import market_only_strategy, risk_managed_market_strategy, canslim_strategy
from backtesting.backtester import run_backtest
from utils.metrics import compute_performance_metrics
from utils.reporting import create_html_report

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def main():
    try:
        # Remove old files to ensure a clean run
        files_to_remove = [
            "financials.feather", 
            "proxies.feather", 
            "top_stocks_tickersymbols.csv", 
            "top_stocks.feather"
        ]
        for filename in files_to_remove:
            file_path = DATA_DIR / filename
            if file_path.exists():
                os.remove(file_path)
        logger.info(f"Removed old files: {files_to_remove}")

        # Step 1: Fetch Aggregates Data
        logger.info("Step 1: Fetching aggregates data...")
        fetcher = AggregatesFetcher()
        fetcher.run()
        logger.info("Step 1 completed: Aggregates data fetched.")

        # Step 2: Process Aggregates Data
        logger.info("Step 2: Processing aggregates data...")
        aggs_processor = AggregatesProcessor(
            base_dir=DATA_DIR / "us_stocks_sip/day_aggs_feather",
            output_path=DATA_DIR / "processed_data.feather"
        )
        aggs_processor.process()
        logger.info("Step 2 completed: Aggregates data processed.")

        # Step 3: Fetch Financials Data
        logger.info("Step 3: Fetching financials data...")
        financials_fetcher = FinancialsFetcher()
        financials_fetcher.run()
        logger.info("Step 3 completed: Financials data fetched.")

        # Load base data
        proxies_df = load_proxies()
        top_stocks_df = load_top_stocks()
        financials_df = load_financials()

        # Step 5: Determine Rebalance Dates
        logger.info("Step 5: Determining rebalance dates...")
        market_only_df = proxies_df[proxies_df["ticker"] == MARKET_PROXY].copy()
        rebalance_dates = get_rebalance_dates(market_only_df, REBALANCE_FREQUENCY, start_date=START_DATE, end_date=END_DATE)

        if not rebalance_dates:
            logger.warning("No rebalance dates found. Exiting.")
            return

        logger.info(f"Rebalancing {REBALANCE_FREQUENCY}, first few dates: {rebalance_dates[:5]}...")

        # Load S&P 500 historical snapshot
        sp500_snapshot_path = DATA_DIR / "sp_500_historic_snapshot.feather"
        if not sp500_snapshot_path.exists():
            logger.error(f"S&P 500 historic snapshot file not found at {sp500_snapshot_path}.")
            return
        sp500_snapshot_df = pd.read_feather(sp500_snapshot_path)
        sp500_snapshot_df["date"] = pd.to_datetime(sp500_snapshot_df["date"], errors="coerce")
        sp500_snapshot_df = sp500_snapshot_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

        # Define parameter spaces for about ~500 runs
        # For example:
        # C quarterly_growth_threshold: 5 values between 0.05 and 0.25
        # A annual_growth_threshold: 5 values between 0.05 and 0.25
        # Volume factor (for S and I): 5 values between 1.1 and 2.0
        # AD ratio threshold: 5 values between 1.0 and 2.0
        c_values = np.linspace(0.05, 0.25, 5)
        a_values = np.linspace(0.05, 0.25, 5)
        vol_values = np.linspace(1.1, 2.0, 2)
        ad_values = np.linspace(1.0, 2.0, 2)

        combos = list(product(c_values, a_values, vol_values, ad_values))
        # combos will have 5*5*5*5 = 625 combos, slightly more than 500 but close enough

        results = []
        total_combinations = len(combos)
        combo_count = 0

        for c_thresh, a_thresh, vol_factor, ad_ratio in combos:
            combo_count += 1
            logger.info(f"Testing combination {combo_count}/{total_combinations}: "
                        f"C={c_thresh:.2f}, A={a_thresh:.2f}, vol_factor={vol_factor:.2f}, ad_ratio={ad_ratio:.2f}")

            criteria_config = {
                "C": {"quarterly_growth_threshold": c_thresh},
                "A": {"annual_growth_threshold": a_thresh},
                "N": {"lookback_period": 252},
                "S": {"volume_factor": vol_factor},
                "L": {"return_diff_threshold": 0.0},
                "I": {"lookback_period": 50, "ad_ratio_threshold": ad_ratio},
                "M": {"use_ma_cross": True}
            }

            # Recompute indicators with these parameters
            p_df, ts_df, fin_df, canslim_criteria_dict = calculate_canslim_indicators(
                proxies_df.copy(),
                top_stocks_df.copy(),
                financials_df.copy(),
                criteria_config=criteria_config
            )

            canslim_data_dict = {
                "proxies_df": p_df,
                "top_stocks_df": ts_df,
                "sp500_snapshot_df": sp500_snapshot_df,
                "use_slots": False  # Adjust if you want slots or not
            }

            canslim_history = run_backtest(
                canslim_strategy, p_df, ts_df, rebalance_dates, initial_funds=INITIAL_FUNDS, data_dict=canslim_data_dict
            )

            canslim_metrics = compute_performance_metrics(canslim_history)
            total_return = canslim_metrics.get("total_return", float("-inf"))

            logger.info(f"Results for this combination: total_return={total_return:.4f}")

            results.append({
                "C": c_thresh,
                "A": a_thresh,
                "vol_factor": vol_factor,
                "ad_ratio": ad_ratio,
                "total_return": total_return,
                "criteria_config": criteria_config,
                "canslim_history": canslim_history.copy(),
                "canslim_criteria_dict": canslim_criteria_dict.copy(),
                "canslim_investments": canslim_data_dict.get("canslim_investments", [])
            })

        # Find best result
        best_result = max(results, key=lambda x: x["total_return"])
        logger.info(f"Best parameters found: C={best_result['C']:.2f}, "
                    f"A={best_result['A']:.2f}, vol_factor={best_result['vol_factor']:.2f}, "
                    f"ad_ratio={best_result['ad_ratio']:.2f} with total_return={best_result['total_return']}")

        # Rerun strategies with best parameters
        final_criteria_config = best_result["criteria_config"]
        p_df, ts_df, fin_df, final_canslim_criteria_dict = calculate_canslim_indicators(
            proxies_df.copy(),
            top_stocks_df.copy(),
            financials_df.copy(),
            criteria_config=final_criteria_config
        )

        final_canslim_data_dict = {
            "proxies_df": p_df,
            "top_stocks_df": ts_df,
            "sp500_snapshot_df": sp500_snapshot_df,
            "use_slots": False
        }

        # Market only
        market_history = run_backtest(
            market_only_strategy, p_df, ts_df, rebalance_dates, initial_funds=INITIAL_FUNDS
        )
        market_metrics = compute_performance_metrics(market_history)

        # Risk managed market
        risk_managed_market_history = run_backtest(
            risk_managed_market_strategy, p_df, ts_df, rebalance_dates, initial_funds=INITIAL_FUNDS
        )
        risk_managed_market_metrics = compute_performance_metrics(risk_managed_market_history)

        # CANSLIM best
        canslim_history = best_result["canslim_history"]
        canslim_metrics = compute_performance_metrics(canslim_history)
        canslim_investments = best_result["canslim_investments"]

        strategies_data = [
            (f"Market Only ({MARKET_PROXY})", market_history, market_metrics),
            (f"Risk Managed Market ({MONEY_MARKET_PROXY}-{MARKET_PROXY})", risk_managed_market_history, risk_managed_market_metrics),
            ("CANSLIM", canslim_history, canslim_metrics)
        ]

        # Create final report
        logger.info("Generating final report for best parameter set...")
        create_html_report(
            strategies_data, 
            canslim_criteria_dict=final_canslim_criteria_dict,
            canslim_investments=canslim_investments,
            output_path=REPORT_DIR / "backtest_report.html"
        )

        # Save all results to CSV
        results_for_csv = []
        for r in results:
            results_for_csv.append({
                "C": r["C"],
                "A": r["A"],
                "vol_factor": r["vol_factor"],
                "ad_ratio": r["ad_ratio"],
                "total_return": r["total_return"]
            })
        pd.DataFrame(results_for_csv).to_csv(DATA_DIR / "canslim_param_search_results.csv", index=False)
        logger.info("All steps completed successfully and results saved.")

    except Exception as e:
        logger.error(f"An error occurred in the pipeline: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    configure_logging()
    main()
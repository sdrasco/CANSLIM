# main.py

import logging
from data.aggs_fetcher import AggregatesFetcher
from data.aggs_processor import AggregatesProcessor
from data.financials_fetcher import FinancialsFetcher
from config.settings import DATA_DIR, NUM_TICKERS, INITIAL_FUNDS, REBALANCE_FREQUENCY, MARKET_PROXY, MONEY_MARKET_PROXY, REPORT_DIR
from utils.logging_utils import configure_logging

# Updated import: load_proxies() instead of load_market_proxy()
from data.data_loaders import load_proxies, load_top_stocks, load_financials
from data.canslim_calculator import calculate_canslim_indicators
from utils.calendar_utils import get_quarter_end_dates, get_rebalance_dates
from strategies.strategy_definitions import market_only_strategy, risk_managed_market_strategy, canslim_strategy
from backtesting.backtester import run_backtest
from utils.metrics import compute_performance_metrics
from utils.reporting import create_html_report

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def main():
    try:
        # Step 1: Fetch Aggregates Data
        logger.info("Step 1: Fetching aggregates data...")
        fetcher = AggregatesFetcher()
        fetcher.run()
        logger.info("Step 1 completed: Aggregates data fetched.")

        # Step 2: Process Aggregates Data
        logger.info("Step 2: Processing aggregates data...")
        aggs_processor = AggregatesProcessor(
            base_dir=DATA_DIR / "us_stocks_sip/day_aggs_feather",
            output_path=DATA_DIR / "processed_data.feather",
            top_n_tickers=NUM_TICKERS,
        )
        aggs_processor.process()
        logger.info("Step 2 completed: Aggregates data processed.")

        # Step 3: Fetch Financials Data
        logger.info("Step 3: Fetching financials data...")
        financials_fetcher = FinancialsFetcher()
        financials_fetcher.run()
        logger.info("Step 3 completed: Financials data fetched.")

        # Step 4: Calculate CANSLIM Indicators
        logger.info("Step 4: Calculating CANSLIM indicators...")

        # Load the combined proxies (both MARKET_PROXY and MONEY_MARKET_PROXY)
        proxies_df = load_proxies()
        top_stocks_df = load_top_stocks()
        financials_df = load_financials()

        # Compute CANSLIM indicators:
        # The calculate_canslim_indicators function may need to filter the MARKET_PROXY ticker internally
        # or we can ensure it handles multiple tickers. Assuming it was adjusted as discussed:
        proxies_df, top_stocks_df, financials_df = calculate_canslim_indicators(proxies_df, top_stocks_df, financials_df)

        # Save updated data if needed
        top_stocks_df.to_feather(DATA_DIR / "top_stocks.feather")
        proxies_df.to_feather(DATA_DIR / "proxies.feather")

        logger.info("CANSLIM indicators calculated.")

        # Step 5: Determine Rebalance Dates
        # We need MARKET_PROXY data specifically for determining rebalance dates.
        market_only_df = proxies_df[proxies_df["ticker"] == MARKET_PROXY].copy()

        logger.info("Step 5: Determining rebalance dates...")
        quarter_ends_df = get_quarter_end_dates(financials_df, "AAPL")
        rebalance_dates = get_rebalance_dates(market_only_df, quarter_ends_df)

        if not rebalance_dates:
            logger.warning("No rebalance dates found. Exiting after CANSLIM calculation.")
            return

        logger.info(f"Rebalancing {REBALANCE_FREQUENCY}, dates: {rebalance_dates[:5]}...")

        # Step 6: Run Backtests for each strategy
        logger.info("Step 6: Running backtests...")

        # Now run_backtest expects proxies_df (with multiple tickers) instead of market_proxy_df
        # The strategies and backtester code should now filter by ticker as needed.
        # top_stocks_df remains the same.
        market_history = run_backtest(market_only_strategy, proxies_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS)
        risk_managed_market_history = run_backtest(risk_managed_market_strategy, proxies_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS)
        canslim_history = run_backtest(canslim_strategy, proxies_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS)

        # Step 7: Compute Metrics
        logger.info("Step 7: Computing metrics...")
        market_metrics = compute_performance_metrics(market_history)
        risk_managed_market_metrics = compute_performance_metrics(risk_managed_market_history)
        canslim_metrics = compute_performance_metrics(canslim_history)

        # Step 8: Generate Report
        logger.info("Step 8: Generating HTML report...")
        strategies_data = [
            (f"Market Only ({MARKET_PROXY})", market_history, market_metrics),
            (f"Risk Managed Market ({MONEY_MARKET_PROXY}-{MARKET_PROXY})", risk_managed_market_history, risk_managed_market_metrics),
            ("CANSLIM", canslim_history, canslim_metrics)
        ]
        create_html_report(strategies_data, output_path=REPORT_DIR / "backtest_report.html")

        logger.info("All steps completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred in the pipeline: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    configure_logging()
    main()
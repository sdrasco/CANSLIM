# main.py

import logging
from data.aggs_fetcher import AggregatesFetcher
from data.aggs_processor import AggregatesProcessor
from data.financials_fetcher import FinancialsFetcher
from config.settings import DATA_DIR, NUM_TICKERS, INITIAL_FUNDS, REBALANCE_FREQUENCY, MARKET_PROXY, MONEY_MARKET_PROXY
from utils.logging_utils import configure_logging

# New imports for the backtesting pipeline
from data.data_loaders import load_market_proxy, load_top_stocks, load_financials
from data.canslim_calculator import calculate_canslim_indicators
from utils.calendar_utils import get_quarter_end_dates, get_rebalance_dates
from strategies.strategy_definitions import market_only_strategy, shy_spy_strategy, canslim_strategy
from backtesting.backtester import run_backtest
from utils.metrics import compute_performance_metrics
from utils.reporting import create_html_report

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

        # Step 4: Calculate CANSLIM Indicators (M in market_proxy, C/A in financials merged into top_stocks, N/S/L/I in top_stocks)
        logger.info("Step 4: Calculating CANSLIM indicators...")
        market_proxy_df = load_market_proxy()
        top_stocks_df = load_top_stocks()
        financials_df = load_financials()

        # Compute CANSLIM indicators
        market_proxy_df, top_stocks_df, financials_df = calculate_canslim_indicators(market_proxy_df, top_stocks_df, financials_df)

        # Save updated data if desired, but it is not necessary
        top_stocks_df.to_feather(DATA_DIR / "top_stocks.feather")
        market_proxy_df.to_feather(DATA_DIR / "market_proxy.feather")

        logger.info("CANSLIM indicators calculated.")

        # Step 5: Determine Rebalance Dates
        # Use AAPL to get quarter_end_dates
        logger.info("Step 5: Determining rebalance dates...")
        quarter_ends_df = get_quarter_end_dates(financials_df, "AAPL")
        rebalance_dates = get_rebalance_dates(market_proxy_df, quarter_ends_df)

        if not rebalance_dates:
            logger.warning("No rebalance dates found. Exiting after CANSLIM calculation.")
            return

        logger.info(f"Rebalancing {REBALANCE_FREQUENCY}, dates: {rebalance_dates[:5]}...")

        # Step 6: Run Backtests for each strategy
        logger.info("Step 6: Running backtests...")

        market_history = run_backtest(market_only_strategy, market_proxy_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS)
        shy_spy_history = run_backtest(shy_spy_strategy, market_proxy_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS)
        canslim_history = run_backtest(canslim_strategy, market_proxy_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS)

        # Step 7: Compute Metrics
        logger.info("Step 7: Computing metrics...")
        market_metrics = compute_performance_metrics(market_history)
        shy_spy_metrics = compute_performance_metrics(shy_spy_history)
        canslim_metrics = compute_performance_metrics(canslim_history)

        # Step 8: Generate Report
        logger.info("Step 8: Generating HTML report...")
        strategies_data = [
            ("Market Only", market_history, market_metrics),
            ("SHY-SPY", shy_spy_history, shy_spy_metrics),
            ("CANSLI", canslim_history, canslim_metrics)
        ]
        create_html_report(strategies_data, output_path=DATA_DIR / "backtest_report.html")

        logger.info("All steps completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred in the pipeline: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    configure_logging()
    main()
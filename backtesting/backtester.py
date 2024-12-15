# backtesting/backtester.py

import logging
import pandas as pd
from datetime import timedelta
from config.settings import INITIAL_FUNDS, MARKET_PROXY, MONEY_MARKET_PROXY, REBALANCE_FREQUENCY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def run_backtest(strategy_func, proxies_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS, data_dict=None):
    """
    Run a backtest for a given strategy.

    Parameters:
        strategy_func (callable): The strategy function that takes 
                                  (rebalance_date, portfolio_value, data_dict, is_first_rebalance)
                                  and returns an allocation dict {ticker: weight}.
        proxies_df (pd.DataFrame): Combined DataFrame with both MARKET_PROXY and MONEY_MARKET_PROXY tickers.
                                   Must have columns: date, ticker, close.
        top_stocks_df (pd.DataFrame): Top stocks data with date, ticker, close, and CANSLI columns.
        rebalance_dates (list of date): The dates on which to rebalance the portfolio.
        initial_funds (float): The initial amount of money to start with.
        data_dict (dict, optional): A dictionary to pass additional data to the strategy. The run_backtest
                                    will ensure `proxies_df` and `top_stocks_df` are in this dict.

    Returns:
        portfolio_history (pd.DataFrame): DataFrame with columns ['date', 'portfolio_value'] representing the daily value.
    """

    # If no data_dict is provided, create an empty one
    if data_dict is None:
        data_dict = {}

    # Ensure proxies_df and top_stocks_df are in data_dict
    data_dict["proxies_df"] = proxies_df
    data_dict["top_stocks_df"] = top_stocks_df

    proxies_df = proxies_df.sort_values(["date", "ticker"])
    top_stocks_df = top_stocks_df.sort_values(["date", "ticker"])

    start_date = rebalance_dates[0]
    end_date = rebalance_dates[-1]

    max_available_date = proxies_df["date"].max()
    if end_date > max_available_date.date():
        logger.warning(f"End date {end_date} beyond available market data ({max_available_date.date()}), truncating.")
        end_date = max_available_date.date()

    trading_days = proxies_df[(proxies_df["date"] >= pd.to_datetime(start_date)) &
                              (proxies_df["date"] <= pd.to_datetime(end_date))]["date"].unique()

    trading_days = pd.to_datetime(sorted(trading_days))

    portfolio_value = initial_funds
    holdings = {}

    def get_price(date, ticker):
        if ticker == MARKET_PROXY or ticker == MONEY_MARKET_PROXY:
            # Filter proxies_df by date and ticker
            row = proxies_df[(proxies_df["date"] == date) & (proxies_df["ticker"] == ticker)]
        else:
            # For regular stocks, filter top_stocks_df
            row = top_stocks_df[(top_stocks_df["date"] == date) & (top_stocks_df["ticker"] == ticker)]

        if row.empty:
            logger.debug(f"No price data for {ticker} on {date}, returning None.")
            return None
        return row["close"].iloc[0]

    def compute_portfolio_value(date, holdings):
        val = 0.0
        for tkr, sh in holdings.items():
            price = get_price(date, tkr)
            if price is not None:
                val += sh * price
            else:
                logger.debug(f"Missing price for {tkr} on {date}, treating as 0 in portfolio value.")
        return val

    portfolio_records = []
    first_rebalance_done = False

    for current_date in trading_days:
        current_date = pd.to_datetime(current_date).normalize()

        if current_date.date() in rebalance_dates:
            # Log rebalancing at INFO level, including the start_date and REBALANCE_FREQUENCY
            logger.debug(
                f"Rebalancing portfolio on {current_date.date()}, frequency: {REBALANCE_FREQUENCY}"
            )

            logger.debug(f"Rebalance date {current_date.date()} encountered.")
            portfolio_value = compute_portfolio_value(current_date, holdings)
            logger.debug(f"Portfolio value before rebalancing: {portfolio_value}")

            # Determine if this is the first rebalance
            is_first_rebalance = not first_rebalance_done

            # If this is the first rebalance, override the portfolio_value with initial_funds
            if is_first_rebalance:
                logger.debug(f"First rebalance day {current_date.date()}: overriding portfolio_value with initial_funds={initial_funds}")
                portfolio_value = initial_funds

            # Call the strategy function with data_dict included
            allocation = strategy_func(current_date.date(), portfolio_value, data_dict, is_first_rebalance=is_first_rebalance)
            logger.debug(f"Strategy allocation on {current_date.date()}: {allocation}")

            new_holdings = {}
            for tkr, weight in allocation.items():
                price = get_price(current_date, tkr)
                if price is None or price <= 0:
                    logger.warning(f"Invalid or missing price for {tkr} on {current_date.date()}, skipping ticker.")
                    continue
                shares = (weight * portfolio_value) / price
                new_holdings[tkr] = shares
                logger.debug(f"Allocated {shares} shares of {tkr} at price {price}")

            holdings = new_holdings
            logger.debug(f"New holdings after rebalancing: {holdings}")

            first_rebalance_done = True

        daily_value = compute_portfolio_value(current_date, holdings)
        logger.debug(f"Portfolio value on {current_date.date()}: {daily_value}")
        portfolio_records.append({"date": current_date, "portfolio_value": daily_value})

    portfolio_history = pd.DataFrame(portfolio_records)
    portfolio_history.sort_values("date", inplace=True)
    portfolio_history.reset_index(drop=True, inplace=True)
    logger.debug("Final portfolio history:")
    logger.debug(portfolio_history.head(10))

    return portfolio_history
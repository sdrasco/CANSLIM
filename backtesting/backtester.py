# backtesting/backtester.py

import logging
import pandas as pd
from datetime import timedelta
from config.settings import INITIAL_FUNDS, MARKET_PROXY, MONEY_MARKET_PROXY

logger = logging.getLogger(__name__)

def run_backtest(strategy_func, market_proxy_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS):
    """
    Run a backtest for a given strategy.

    Parameters:
        strategy_func (callable): The strategy function that takes (rebalance_date, portfolio_value, data_dict)
                                  and returns an allocation dict {ticker: weight}.
        market_proxy_df (pd.DataFrame): Market proxy data with 'date', 'close', 'M', etc.
        top_stocks_df (pd.DataFrame): Top stocks data with 'date', 'ticker', 'close', CANSLI columns.
        rebalance_dates (list of date): The dates on which to rebalance the portfolio.
        initial_funds (float): The initial amount of money to start with.

    Returns:
        portfolio_history (pd.DataFrame): DataFrame with columns ['date', 'portfolio_value'] representing the daily value.
    """

    # Combine market and stocks into a single data_dict for strategies
    data_dict = {
        "market_proxy_df": market_proxy_df,
        "top_stocks_df": top_stocks_df
    }

    # Ensure data are sorted by date
    market_proxy_df = market_proxy_df.sort_values("date")
    top_stocks_df = top_stocks_df.sort_values(["date", "ticker"])

    # Determine the full date range we will simulate over:
    start_date = rebalance_dates[0]
    end_date = rebalance_dates[-1]

    # But we may have to extend end_date a bit to capture some final days after last rebalance
    # Let's find the max available date in market_proxy_df as the natural end
    max_available_date = market_proxy_df["date"].max()
    if end_date > max_available_date:
        logger.warning("End date beyond available market data, truncating.")
        end_date = max_available_date

    # We'll iterate over all trading days between start_date and end_date
    # Identify all unique trading days from market_proxy_df in the range
    trading_days = market_proxy_df[(market_proxy_df["date"] >= pd.to_datetime(start_date)) &
                                   (market_proxy_df["date"] <= pd.to_datetime(end_date))]["date"].unique()

    trading_days = pd.to_datetime(sorted(trading_days))

    # Portfolio state
    portfolio_value = initial_funds
    holdings = {}  # {ticker: shares}

    portfolio_records = []

    def get_price(date, ticker):
        """Get the close price for a ticker on a given date from top_stocks_df or market_proxy_df if it's a proxy."""
        if ticker == MARKET_PROXY or ticker == MONEY_MARKET_PROXY:
            # Proxy prices from market_proxy_df (assuming we have them there)
            row = market_proxy_df[market_proxy_df["date"] == date]
        else:
            row = top_stocks_df[(top_stocks_df["date"] == date) & (top_stocks_df["ticker"] == ticker)]

        if row.empty:
            # If no price found, this is a problem. In practice, handle missing data gracefully.
            logger.warning(f"No price data for {ticker} on {date}, assuming no change or skipping.")
            return None
        return row["close"].iloc[0]

    def compute_portfolio_value(date, holdings):
        val = 0.0
        for tkr, sh in holdings.items():
            price = get_price(date, tkr)
            if price is not None:
                val += sh * price
            else:
                # If price is missing, assume price = 0 or last known price. Here, let's just skip or assume no value.
                logger.warning(f"Missing price for {tkr} on {date}, treating as 0.")
        return val

    for current_date in trading_days:
        current_date = pd.to_datetime(current_date).normalize()

        # Check if this is a rebalance date
        if current_date.date() in rebalance_dates:
            # Rebalance the portfolio
            # First, compute current portfolio value
            portfolio_value = compute_portfolio_value(current_date, holdings)

            # Get new allocation from strategy
            allocation = strategy_func(current_date.date(), portfolio_value, data_dict)

            # Convert allocation (weights) into shares
            # total_value = portfolio_value
            new_holdings = {}
            for tkr, weight in allocation.items():
                # weight * portfolio_value / price = shares
                price = get_price(current_date, tkr)
                if price is None or price <= 0:
                    logger.warning(f"Invalid price for {tkr} on rebalance {current_date}, skipping ticker.")
                    continue
                shares = (weight * portfolio_value) / price
                new_holdings[tkr] = shares

            holdings = new_holdings

        else:
            # Just update portfolio value for a normal trading day
            # no trading, just price changes
            pass

        # Compute daily portfolio value
        daily_value = compute_portfolio_value(current_date, holdings)
        portfolio_records.append({"date": current_date, "portfolio_value": daily_value})

    portfolio_history = pd.DataFrame(portfolio_records)
    portfolio_history.sort_values("date", inplace=True)
    portfolio_history.reset_index(drop=True, inplace=True)

    return portfolio_history
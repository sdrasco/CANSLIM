# backtesting/backtester.py

import logging
import pandas as pd
from datetime import timedelta
from config.settings import INITIAL_FUNDS, MARKET_PROXY, MONEY_MARKET_PROXY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def run_backtest(strategy_func, market_proxy_df, top_stocks_df, rebalance_dates, initial_funds=INITIAL_FUNDS):
    data_dict = {
        "market_proxy_df": market_proxy_df,
        "top_stocks_df": top_stocks_df
    }

    market_proxy_df = market_proxy_df.sort_values("date")
    top_stocks_df = top_stocks_df.sort_values(["date", "ticker"])

    start_date = rebalance_dates[0]
    end_date = rebalance_dates[-1]

    max_available_date = market_proxy_df["date"].max()
    if end_date > max_available_date.date():
        logger.warning(f"End date {end_date} beyond available market data ({max_available_date.date()}), truncating.")
        end_date = max_available_date.date()

    trading_days = market_proxy_df[(market_proxy_df["date"] >= pd.to_datetime(start_date)) &
                                   (market_proxy_df["date"] <= pd.to_datetime(end_date))]["date"].unique()

    trading_days = pd.to_datetime(sorted(trading_days))

    portfolio_value = initial_funds
    holdings = {}

    def get_price(date, ticker):
        if ticker == MARKET_PROXY or ticker == MONEY_MARKET_PROXY:
            row = market_proxy_df[market_proxy_df["date"] == date]
        else:
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
            logger.debug(f"Rebalance date {current_date.date()} encountered.")
            portfolio_value = compute_portfolio_value(current_date, holdings)
            logger.debug(f"Portfolio value before rebalancing: {portfolio_value}")

            # Determine if this is the first rebalance
            is_first_rebalance = not first_rebalance_done

            # If this is the first rebalance, override the portfolio_value with initial_funds
            if is_first_rebalance:
                logger.debug(f"First rebalance day {current_date.date()}: overriding portfolio_value with initial_funds={initial_funds}")
                portfolio_value = initial_funds

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
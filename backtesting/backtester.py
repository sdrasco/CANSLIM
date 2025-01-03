# backtesting/backtester.py
"""
Uses pivoted "close" matrices for faster get_price lookups.
Adds checks to ensure strategies don't exceed 100% allocation,
handle leftover cash, or go negative if shorting isn't intended.
"""

import logging
import pandas as pd
from datetime import timedelta
from config.settings import (
    INITIAL_FUNDS, MARKET_PROXY, MONEY_MARKET_PROXY, REBALANCE_FREQUENCY
)
from utils.logging_utils import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

def run_backtest(
    strategy_func,
    proxies_df,
    top_stocks_df,
    rebalance_dates,
    initial_funds=INITIAL_FUNDS,
    data_dict=None
):
    if data_dict is None:
        data_dict = {}

    proxies_reset = proxies_df.reset_index()
    if "ticker" not in proxies_reset.columns or "close" not in proxies_reset.columns:
        logger.error("proxies_df missing required columns 'ticker' or 'close'.")
        return pd.DataFrame()

    proxies_close_matrix = proxies_reset.pivot(
        index="date", columns="ticker", values="close"
    )

    top_stocks_reset = top_stocks_df.reset_index()
    if "ticker" not in top_stocks_reset.columns or "close" not in top_stocks_reset.columns:
        logger.error("top_stocks_df missing required columns 'ticker' or 'close'.")
        return pd.DataFrame()

    top_stocks_close_matrix = top_stocks_reset.pivot(
        index="date", columns="ticker", values="close"
    )

    proxies_close_matrix.index = pd.to_datetime(proxies_close_matrix.index)
    proxies_close_matrix.sort_index(inplace=True)
    top_stocks_close_matrix.index = pd.to_datetime(top_stocks_close_matrix.index)
    top_stocks_close_matrix.sort_index(inplace=True)

    data_dict["proxies_df"] = proxies_df
    data_dict["top_stocks_df"] = top_stocks_df

    start_date = rebalance_dates[0]
    end_date = rebalance_dates[-1]

    max_available_timestamp = proxies_close_matrix.index.max()
    if pd.Timestamp(end_date) > max_available_timestamp:
        logger.warning(
            f"End date {end_date} is beyond available market data ({max_available_timestamp.date()}), truncating."
        )
        end_date = max_available_timestamp.date()

    mask = (
        (proxies_close_matrix.index >= pd.to_datetime(start_date)) &
        (proxies_close_matrix.index <= pd.to_datetime(end_date))
    )
    trading_days = proxies_close_matrix.index[mask].unique()
    trading_days = sorted(trading_days)

    portfolio_value = initial_funds
    holdings = {}

    def get_price(timestamp, ticker):
        if ticker in [MARKET_PROXY, MONEY_MARKET_PROXY]:
            mat = proxies_close_matrix
        else:
            mat = top_stocks_close_matrix

        if (timestamp not in mat.index) or (ticker not in mat.columns):
            logger.debug(f"No price data for {ticker} at {timestamp}, returning None.")
            return None

        price = mat.loc[timestamp, ticker]
        if pd.isna(price):
            return None
        return price

    def compute_portfolio_value(timestamp, current_holdings):
        val = 0.0
        for tkr, shares in current_holdings.items():
            price = get_price(timestamp, tkr)
            if price is not None:
                val += shares * price
            else:
                logger.debug(f"No price for {tkr} at {timestamp}, treating as 0.")
        return val

    portfolio_records = []
    first_rebalance_done = False

    for current_timestamp in trading_days:
        current_date = current_timestamp.date()

        if current_date in rebalance_dates:
            logger.debug(f"Rebalancing portfolio on {current_date} ({REBALANCE_FREQUENCY}).")
            portfolio_value = compute_portfolio_value(current_timestamp, holdings)
            logger.debug(f"Portfolio value before rebalancing: {portfolio_value}")

            is_first_rebalance = not first_rebalance_done
            if is_first_rebalance:
                logger.debug(f"First rebalance day {current_date}: using initial_funds={initial_funds}")
                portfolio_value = initial_funds

            allocation = strategy_func(current_date, portfolio_value, data_dict, is_first_rebalance)
            logger.debug(f"Strategy allocation on {current_date}: {allocation}")

            sum_of_weights = sum(allocation.values())
            if sum_of_weights > 1.000001:
                logger.error(
                    f"Strategy allocated {sum_of_weights:.4f}, which is >1.0. "
                    "Potential over-investment. Verify logic."
                )

            negative_allocs = [t for t, w in allocation.items() if w < 0]
            if negative_allocs:
                logger.error(
                    f"Negative weights detected for tickers: {negative_allocs}. "
                    "Potential shorting if not intended."
                )

            leftover_cash = (1.0 - sum_of_weights) * portfolio_value
            logger.info(f"Leftover cash (uninvested) = {leftover_cash:.2f}")

            new_holdings = {}
            for tkr, weight in allocation.items():
                price = get_price(current_timestamp, tkr)
                if price is None or price <= 0:
                    logger.warning(f"No valid price for {tkr} on {current_date}, skipping.")
                    continue
                shares = (weight * portfolio_value) / price
                new_holdings[tkr] = shares
                logger.debug(f"Allocated {shares} shares of {tkr} at {price}")

            holdings = new_holdings
            first_rebalance_done = True

        daily_value = compute_portfolio_value(current_timestamp, holdings)
        logger.debug(f"Portfolio value on {current_date}: {daily_value}")
        portfolio_records.append({"date": current_timestamp, "portfolio_value": daily_value})

    portfolio_history = pd.DataFrame(portfolio_records)
    portfolio_history.sort_values("date", inplace=True)
    portfolio_history.reset_index(drop=True, inplace=True)
    logger.debug("Final portfolio history (first few rows):")
    logger.debug(portfolio_history.head(5))

    return portfolio_history
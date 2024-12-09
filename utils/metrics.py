# utils/metrics.py

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def compute_daily_returns(portfolio_history: pd.DataFrame):
    """
    Given a portfolio_history DataFrame with 'date' and 'portfolio_value',
    compute daily returns.
    Returns a Series of daily returns aligned with dates.
    """
    portfolio_history = portfolio_history.sort_values("date").reset_index(drop=True)
    portfolio_history["daily_return"] = portfolio_history["portfolio_value"].pct_change()
    return portfolio_history["daily_return"]


def annualized_return(portfolio_history: pd.DataFrame):
    """
    Compute annualized return:
    ((final_value / initial_value)^(1/years)) - 1
    """
    ph = portfolio_history.sort_values("date").reset_index(drop=True)
    initial_val = ph["portfolio_value"].iloc[0]
    final_val = ph["portfolio_value"].iloc[-1]

    start_date = ph["date"].iloc[0]
    end_date = ph["date"].iloc[-1]

    # Calculate the number of years in the backtest period
    days = (end_date - start_date).days
    if days <= 0:
        logger.warning("No time elapsed for annualized return computation.")
        return 0.0
    years = days / 365.0  # approximate

    return (final_val / initial_val)**(1/years) - 1


def annualized_volatility(portfolio_history: pd.DataFrame):
    """
    Compute annualized volatility as std(daily_returns) * sqrt(252)
    """
    daily_returns = compute_daily_returns(portfolio_history).dropna()
    if daily_returns.empty:
        return 0.0
    return daily_returns.std() * np.sqrt(252)


def max_drawdown(portfolio_history: pd.DataFrame):
    """
    Compute the maximum drawdown:
    The maximum peak-to-trough decline during the period.
    """
    ph = portfolio_history.sort_values("date").reset_index(drop=True)
    cummax = ph["portfolio_value"].cummax()
    drawdowns = (ph["portfolio_value"] - cummax) / cummax
    return drawdowns.min()  # This should be a negative number (the worst drawdown)


def sharpe_ratio(portfolio_history: pd.DataFrame, risk_free_rate=0.0):
    """
    Compute the Sharpe Ratio:
    (Annualized Return - Risk-Free Rate) / Annualized Volatility
    """
    ann_return = annualized_return(portfolio_history)
    ann_vol = annualized_volatility(portfolio_history)
    if ann_vol == 0:
        logger.warning("Annualized volatility is zero, Sharpe ratio undefined.")
        return np.nan
    return (ann_return - risk_free_rate) / ann_vol


def downside_volatility(portfolio_history: pd.DataFrame, threshold=0.0):
    """
    Compute downside volatility:
    std of returns below the threshold (e.g., 0) * sqrt(252).
    """
    daily_returns = compute_daily_returns(portfolio_history).dropna()
    downside = daily_returns[daily_returns < threshold]
    if downside.empty:
        return 0.0
    return downside.std() * np.sqrt(252)


def sortino_ratio(portfolio_history: pd.DataFrame, risk_free_rate=0.0):
    """
    Compute the Sortino Ratio:
    (Annualized Return - Risk-Free Rate) / Downside Volatility
    """
    ann_return = annualized_return(portfolio_history)
    d_vol = downside_volatility(portfolio_history)
    if d_vol == 0:
        logger.warning("Downside volatility is zero, Sortino ratio undefined.")
        return np.nan
    return (ann_return - risk_free_rate) / d_vol


def compute_performance_metrics(portfolio_history: pd.DataFrame, risk_free_rate=0.0):
    """
    Compute a dictionary of performance metrics for quick summary.
    Metrics include:
    - total_return
    - annualized_return
    - annualized_volatility
    - max_drawdown
    - sharpe_ratio
    - sortino_ratio

    Returns a dict of metric_name: value
    """
    ph = portfolio_history.sort_values("date").reset_index(drop=True)
    if ph.empty:
        logger.error("Empty portfolio history, cannot compute metrics.")
        return {}

    initial_val = ph["portfolio_value"].iloc[0]
    final_val = ph["portfolio_value"].iloc[-1]
    total_return = (final_val / initial_val) - 1

    metrics = {
        "total_return": total_return,
        "annualized_return": annualized_return(ph),
        "annualized_volatility": annualized_volatility(ph),
        "max_drawdown": max_drawdown(ph),
        "sharpe_ratio": sharpe_ratio(ph, risk_free_rate),
        "sortino_ratio": sortino_ratio(ph, risk_free_rate)
    }

    return metrics
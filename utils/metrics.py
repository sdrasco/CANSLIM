# utils/metrics.py

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from config.settings import DATA_DIR, MONEY_MARKET_PROXY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def compute_daily_returns(portfolio_history: pd.DataFrame):
    portfolio_history = portfolio_history.sort_values("date").reset_index(drop=True)
    portfolio_history["daily_return"] = portfolio_history["portfolio_value"].pct_change()
    return portfolio_history["daily_return"]

def annualized_return(portfolio_history: pd.DataFrame):
    ph = portfolio_history.sort_values("date").reset_index(drop=True)
    if ph.empty:
        logger.warning("Empty portfolio history for annualized return computation.")
        return 0.0

    initial_val = ph["portfolio_value"].iloc[0]
    final_val = ph["portfolio_value"].iloc[-1]

    start_date = ph["date"].iloc[0]
    end_date = ph["date"].iloc[-1]

    days = (end_date - start_date).days
    if days <= 0:
        logger.warning("No time elapsed for annualized return computation.")
        return 0.0
    years = days / 365.0

    if initial_val <= 0:
        logger.warning("Initial value <= 0, invalid for annualized return.")
        return 0.0

    return (final_val / initial_val)**(1/years) - 1

def annualized_volatility(portfolio_history: pd.DataFrame):
    daily_returns = compute_daily_returns(portfolio_history).dropna()
    if daily_returns.empty:
        return 0.0
    return daily_returns.std() * np.sqrt(252)

def max_drawdown(portfolio_history: pd.DataFrame):
    ph = portfolio_history.sort_values("date").reset_index(drop=True)
    if ph.empty:
        return 0.0
    cummax = ph["portfolio_value"].cummax()
    drawdowns = (ph["portfolio_value"] - cummax) / cummax
    return drawdowns.min()

def compute_risk_free_rate():
    proxies_path = DATA_DIR / "proxies.feather"
    if not proxies_path.exists():
        logger.warning("proxies.feather not found, defaulting risk-free to 0.0.")
        return 0.0

    try:
        proxies_df = pd.read_feather(proxies_path)
    except Exception as e:
        logger.error(f"Error loading proxies.feather: {e}")
        return 0.0

    mm_data = proxies_df[proxies_df["ticker"] == MONEY_MARKET_PROXY].copy()
    if mm_data.empty:
        logger.warning(f"No data found for money market proxy {MONEY_MARKET_PROXY}, defaulting risk-free to 0.0")
        return 0.0

    # Create a portfolio_value column from close prices
    mm_data = mm_data.sort_values("date").reset_index(drop=True)
    if "close" not in mm_data.columns:
        logger.warning("Money market proxy data missing 'close' column, defaulting to 0.0")
        return 0.0

    initial_close = mm_data["close"].iloc[0]
    if initial_close == 0 or pd.isna(initial_close):
        logger.warning("Initial close value is zero or NaN, cannot compute risk-free rate.")
        return 0.0

    mm_data["portfolio_value"] = mm_data["close"] / initial_close

    return annualized_return(mm_data)

def sharpe_ratio(portfolio_history: pd.DataFrame, risk_free_rate: float):
    ann_return = annualized_return(portfolio_history)
    ann_vol = annualized_volatility(portfolio_history)
    if ann_vol == 0:
        logger.warning("Annualized volatility is zero, Sharpe ratio undefined.")
        return np.nan
    return (ann_return - risk_free_rate) / ann_vol

def downside_volatility(portfolio_history: pd.DataFrame, threshold=0.0):
    daily_returns = compute_daily_returns(portfolio_history).dropna()
    downside = daily_returns[daily_returns < threshold]
    if downside.empty:
        return 0.0
    return downside.std() * np.sqrt(252)

def sortino_ratio(portfolio_history: pd.DataFrame, risk_free_rate: float):
    ann_return = annualized_return(portfolio_history)
    d_vol = downside_volatility(portfolio_history)
    if d_vol == 0:
        logger.warning("Downside volatility is zero, Sortino ratio undefined.")
        return np.nan
    return (ann_return - risk_free_rate) / d_vol

def compute_performance_metrics(portfolio_history: pd.DataFrame):
    ph = portfolio_history.sort_values("date").reset_index(drop=True)
    if ph.empty:
        logger.error("Empty portfolio history, cannot compute metrics.")
        return {}

    initial_val = ph["portfolio_value"].iloc[0]
    final_val = ph["portfolio_value"].iloc[-1]
    total_return = (final_val / initial_val) - 1

    # Compute risk-free rate once
    risk_free_rate = compute_risk_free_rate()

    metrics = {
        "total_return": total_return,
        "annualized_return": annualized_return(ph),
        "annualized_volatility": annualized_volatility(ph),
        "max_drawdown": max_drawdown(ph),
        "sharpe_ratio": sharpe_ratio(ph, risk_free_rate),
        "sortino_ratio": sortino_ratio(ph, risk_free_rate)
    }

    return metrics
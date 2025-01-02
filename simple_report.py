"""
utils/report_five_strategies.py

Generates an HTML report for the 5-strategy comparison:
1) Market Only
2) Risk-Managed Market
3) Flattened SPY
4) LeaderSPY
5) VolumeSPY

Relies on:
- simple_strategies_comparison.csv (for final_value & total_return)
- all_strategies_timeseries.csv (for daily portfolio_value per strategy)
- simple_strategies_artifacts.pkl (for any additional details/metrics)
"""

import logging
import os
import base64
import io
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# If you have some config settings
from config.settings import REPORT_DIR, INITIAL_FUNDS

logger = logging.getLogger(__name__)

# -----------------------------------
# 1) MAIN REPORT FUNCTION
# -----------------------------------
def create_five_strategies_report(
    summary_df: pd.DataFrame,
    timeseries_df: pd.DataFrame,
    artifacts: dict,
    output_html: str = "five_strategies_report.html"
):
    """
    Creates an HTML report comparing the five strategies. 
    The final HTML includes:
      - A brief paragraph describing each strategy.
      - A summary table for each strategy (final_value, total_return, annualized_return, max_drawdown, sharpe, etc.).
      - An equity curve chart comparing daily portfolio_value (timeseries_df).
      - (Optional) A drawdown chart or any additional charts.

    :param summary_df: DataFrame with columns [strategy_name, final_value, total_return, ...].
    :param timeseries_df: DataFrame of daily portfolio_value, one column per strategy (e.g. market, risk, flattened, leader, volume).
    :param artifacts: The unpickled dictionary from 'simple_strategies_artifacts.pkl'.
    :param output_html: Filename/path to write the HTML report.
    """
    logger.info(f"Creating 5-strategy HTML report => {output_html}")

    # 1) Compute additional metrics from timeseries (annualized return, max drawdown, Sharpe, etc.)
    metrics_df = _compute_additional_metrics(timeseries_df)

    # 2) Merge those additional metrics into summary_df
    #    We'll assume 'strategy_name' in summary_df matches the columns in timeseries_df.
    summary_df = summary_df.copy()
    summary_df.set_index("strategy_name", inplace=True)
    for strat in metrics_df.index:
        if strat in summary_df.index:
            summary_df.loc[strat, "annualized_return"] = metrics_df.loc[strat, "annualized_return"]
            summary_df.loc[strat, "max_drawdown"] = metrics_df.loc[strat, "max_drawdown"]
            summary_df.loc[strat, "sharpe_ratio"] = metrics_df.loc[strat, "sharpe_ratio"]
    summary_df.reset_index(inplace=True)

    # 3) Build HTML
    html = _build_html_report(summary_df, timeseries_df)

    # 4) Write to file
    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"Report saved to {output_html}")


# -----------------------------------
# 2) BUILD HTML
# -----------------------------------
def _build_html_report(summary_df: pd.DataFrame, timeseries_df: pd.DataFrame) -> str:
    """
    Builds the HTML content, including:
      - Strategy descriptions
      - Summary table (with final_value, total_return, annualized_return, etc.)
      - Equity curve chart
    """
    css = """
    <style>
      body {
        font-family: Arial, sans-serif;
        background-color: #f8f9fa;
        color: #264653;
        margin: 0;
        padding: 0;
      }
      .container {
        max-width: 960px;
        margin: 40px auto;
        padding: 20px;
        background-color: #ffffff;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
      }
      h1, h2, h3 {
        color: #264653;
      }
      h1 {
        text-align: center;
      }
      h3.strategy-title {
        margin-top: 20px;
        color: #2a9d8f;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        margin: 20px 0;
      }
      th, td {
        text-align: left;
        padding: 8px;
        border-bottom: 1px solid #e9ecef;
      }
      th {
        background-color: #2a9d8f;
        color: #ffffff;
      }
      .chart {
        margin: 20px auto;
        display: block;
        max-width: 800px;
        border: 1px solid #ccc;
      }
      .strategy-descriptions {
        margin: 20px 0;
        line-height: 1.5;
        color: #555;
      }
    </style>
    """

    html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Five Strategies Report</title>{css}</head><body>"
    html += "<div class='container'>"
    html += "<h1>5-Strategy Comparison</h1>"

    # Strategy Descriptions
    html += _strategy_descriptions_section()

    # Final Summary Table
    html += "<h2>Performance Summary</h2>"
    # Round some columns for readability
    for col in ["final_value","total_return","annualized_return","max_drawdown","sharpe_ratio"]:
        if col in summary_df.columns:
            summary_df[col] = summary_df[col].apply(lambda x: f"{x:0.4f}" if pd.notnull(x) else "")

    html += summary_df.to_html(index=False, justify="left", border=0)

    # Equity Curve Chart
    eq_chart_b64 = _generate_equity_curve(timeseries_df)
    html += "<h2>Equity Curves Over Time</h2>"
    html += f"<img class='chart' src='data:image/png;base64,{eq_chart_b64}' alt='Equity Curve Chart'/>"

    # Drawdown Chart
    dd_chart_b64 = _generate_drawdown_chart(timeseries_df)
    html += "<h2>Drawdown Chart</h2>"
    html += f"<img class='chart' src='data:image/png;base64,{dd_chart_b64}' alt='Drawdown Chart'/>"

    html += "</div></body></html>"
    return html


def _strategy_descriptions_section() -> str:
    """
    Returns an HTML snippet describing each of the five strategies in a short paragraph.
    """
    # Short, single-paragraph descriptions based on the code's docstrings/logic
    market_desc = (
        "<strong>Market Only Strategy:</strong> Always invests 100% in the market proxy (e.g., SPY). "
        "A passive approach with no timing or selection element."
    )
    risk_desc = (
        "<strong>Risk-Managed Market Strategy:</strong> Invests fully in the market proxy "
        "only if a 50-day over 200-day moving average is bullish; otherwise moves to a money market proxy."
    )
    flatten_desc = (
        "<strong>Flattened SPY:</strong> Equal-weights all current S&P 500 members, "
        "rather than using market-cap weighting."
    )
    leader_desc = (
        "<strong>LeaderSPY:</strong> Weights all S&P 500 constituents by a 'L_measure' reflecting recent outperformance, "
        "giving higher allocations to stronger momentum or 'leadership' stocks."
    )
    volume_desc = (
        "<strong>VolumeSPY:</strong> Weights S&P 500 constituents by their last quarter's volume, "
        "tilting toward higher-liquidity names."
    )

    # If you want to mention the Hybrid canslim, you can also add it here. 
    # But from the script, we only have five strategies, so we'll skip it unless needed.

    text = f"""
    <div class='strategy-descriptions'>
      <p>{market_desc}</p>
      <p>{risk_desc}</p>
      <p>{flatten_desc}</p>
      <p>{leader_desc}</p>
      <p>{volume_desc}</p>
    </div>
    """
    return text


# -----------------------------------
# 3) STATISTICS COMPUTATION
# -----------------------------------
def _compute_additional_metrics(timeseries_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame of daily portfolio_value columns (one column per strategy),
    compute:
      - annualized_return
      - max_drawdown
      - sharpe_ratio (assuming risk-free rate = 0)
    
    Returns a DataFrame indexed by strategy column name with these columns.
    """
    results = []
    for col in timeseries_df.columns:
        pv = timeseries_df[col].dropna()
        if len(pv) < 2:
            results.append({
                "strategy": col,
                "annualized_return": None,
                "max_drawdown": None,
                "sharpe_ratio": None
            })
            continue

        # Daily returns (simple arithmetic returns)
        daily_returns = pv.pct_change().dropna()
        if daily_returns.empty:
            results.append({
                "strategy": col,
                "annualized_return": None,
                "max_drawdown": None,
                "sharpe_ratio": None
            })
            continue

        # Annualized Return (assuming ~252 trading days)
        # (1 + mean_daily_return)^252 - 1
        mean_daily = daily_returns.mean()
        annual_ret = (1 + mean_daily)**252 - 1

        # Max Drawdown
        running_max = pv.cummax()
        drawdowns = (pv - running_max) / running_max
        max_dd = drawdowns.min()  # negative number

        # Sharpe Ratio (assuming risk-free ~ 0)
        # annualized = sqrt(252) * mean(daily_returns)/std(daily_returns)
        daily_std = daily_returns.std()
        if daily_std > 0:
            sharpe = (mean_daily / daily_std) * (252**0.5)
        else:
            sharpe = None

        results.append({
            "strategy": col,
            "annualized_return": annual_ret,
            "max_drawdown": max_dd,
            "sharpe_ratio": sharpe
        })

    metrics_df = pd.DataFrame(results).set_index("strategy")
    return metrics_df


# -----------------------------------
# 4) PLOTTING UTILITIES
# -----------------------------------
def _generate_equity_curve(timeseries_df: pd.DataFrame) -> str:
    """
    Generates a line chart of each column in 'timeseries_df' (daily portfolio_value).
    Returns a base64-encoded PNG string.
    """
    if timeseries_df.empty:
        logger.warning("No timeseries data to plot.")
        return ""

    fig, ax = plt.subplots(figsize=(8,5))
    for col in timeseries_df.columns:
        ax.plot(timeseries_df.index, timeseries_df[col], label=col)

    ax.set_title("Portfolio Value Over Time", fontsize=14)
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    chart_b64 = base64.b64encode(buf.read()).decode("utf-8")
    return chart_b64


def _generate_drawdown_chart(timeseries_df: pd.DataFrame) -> str:
    """
    Creates a drawdown chart for each strategy's portfolio_value.
    Return base64-encoded PNG.
    """
    if timeseries_df.empty:
        return ""

    fig, ax = plt.subplots(figsize=(8,5))
    for col in timeseries_df.columns:
        pv = timeseries_df[col].copy()
        running_max = pv.cummax()
        drawdown = (pv - running_max) / running_max
        ax.plot(drawdown.index, drawdown, label=f"{col} Drawdown")

    ax.set_title("Drawdown Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Drawdown (fraction)")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


# -----------------------------------
# 5) MAIN SCRIPT ENTRY
# -----------------------------------
def main():
    """
    Example usage:
      python report_five_strategies.py

    Steps:
      1) Loads 'simple_strategies_comparison.csv' => summary_df
      2) Loads 'all_strategies_timeseries.csv' => timeseries_df
      3) Loads 'simple_strategies_artifacts.pkl' => artifacts
      4) Calls create_five_strategies_report(...)
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # We'll assume these files are stored in REPORT_DIR
    summary_csv_path = REPORT_DIR / "simple_strategies_comparison.csv"
    timeseries_csv_path = REPORT_DIR / "all_strategies_timeseries.csv"
    artifacts_path = REPORT_DIR / "simple_strategies_artifacts.pkl"
    output_html = REPORT_DIR / "five_strategies_report.html"

    if not summary_csv_path.exists():
        logger.error(f"Missing {summary_csv_path}. Exiting.")
        return
    if not timeseries_csv_path.exists():
        logger.error(f"Missing {timeseries_csv_path}. Exiting.")
        return
    if not artifacts_path.exists():
        logger.warning(f"Missing {artifacts_path}. We'll set artifacts to {{}}.")
        artifacts = {}
    else:
        with open(artifacts_path, "rb") as f:
            artifacts = pickle.load(f)
        logger.info(f"Loaded artifacts from {artifacts_path}.")

    # 1) Load summary CSV
    summary_df = pd.read_csv(summary_csv_path)
    logger.info(f"Loaded summary data: {summary_df.shape} rows.")

    # 2) Load daily timeseries (portfolio_value for each strategy)
    timeseries_df = pd.read_csv(timeseries_csv_path, index_col=0, parse_dates=True)
    logger.info(f"Loaded timeseries data: {timeseries_df.shape} rows/cols.")

    # 3) Build the report
    create_five_strategies_report(
        summary_df=summary_df,
        timeseries_df=timeseries_df,
        artifacts=artifacts,
        output_html=str(output_html)
    )

    logger.info("Done generating 5-strategy report.")


if __name__ == "__main__":
    main()
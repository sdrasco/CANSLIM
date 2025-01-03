"""
utils/report_five_strategies.py

Generates an HTML report for the 5-strategy comparison:
1) Market Only
2) Risk-Managed Market
3) Flattened
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

from config.settings import REPORT_DIR, INITIAL_FUNDS

logger = logging.getLogger(__name__)

def create_five_strategies_report(
    summary_df: pd.DataFrame,
    timeseries_df: pd.DataFrame,
    artifacts: dict,
    output_html: str = "five_strategies_report.html"
):
    """
    Creates an HTML report comparing the five strategies.

    The final HTML includes:
      - A short introduction about quarterly rebalancing and strategy definitions.
      - A performance summary table (renamed columns, plus final_value to cents).
      - Three equity curve charts:
          * All strategies (full history),
          * Same all strategies but only up to 2013,
          * Excluding Volume (full history).
      - A drawdown chart.
    """
    logger.info(f"Creating 5-strategy HTML report => {output_html}")

    metrics_df = _compute_additional_metrics(timeseries_df)
    summary_df = summary_df.copy()
    summary_df.set_index("strategy_name", inplace=True)
    for strat in metrics_df.index:
        if strat in summary_df.index:
            summary_df.loc[strat, "annualized_return"] = metrics_df.loc[strat, "annualized_return"]
            summary_df.loc[strat, "max_drawdown"] = metrics_df.loc[strat, "max_drawdown"]
            summary_df.loc[strat, "sharpe_ratio"] = metrics_df.loc[strat, "sharpe_ratio"]
    summary_df.reset_index(inplace=True)

    html = _build_html_report(summary_df, timeseries_df)

    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"Report saved to {output_html}")


def _build_html_report(summary_df: pd.DataFrame, timeseries_df: pd.DataFrame) -> str:
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

    html = "<!DOCTYPE html>"
    html += f"<html><head><meta charset='UTF-8'><title>Simple Rebalanced SPX Strategies</title>{css}</head><body>"
    html += "<div class='container'>"
    html += "<h1>Simple Strategies</h1>"

    # Strategy Descriptions
    html += _strategy_descriptions_section()

    # Performance Summary
    html += "<h2>Performance Summary</h2>"
    summary_df = _rename_and_round_columns(summary_df)
    html += summary_df.to_html(index=False, justify="left", border=0)

    # 1) Equity Curves (full history, all strategies)
    eq_chart_b64_all = _generate_equity_curve(timeseries_df)
    html += "<h2>Equity Curves (All Strategies)</h2>"
    html += f"<img class='chart' src='data:image/png;base64,{eq_chart_b64_all}' alt='Equity Curve Chart'/>"

    # 2) Equity Curves (all, pre-2013)
    eq_chart_pre2013_b64 = _generate_equity_curve_pre2013(timeseries_df)
    html += "<h2>Equity Curves (All, pre-2013)</h2>"
    html += f"<img class='chart' src='data:image/png;base64,{eq_chart_pre2013_b64}' alt='Equity Curve Chart (pre-2013)'/>"

    # 3) Equity Curves (excluding Volume)
    eq_chart_b64_novol = _generate_equity_curve_excluding(timeseries_df, exclude="volume")
    html += "<h2>Equity Curves (Excluding Volume)</h2>"
    html += f"<img class='chart' src='data:image/png;base64,{eq_chart_b64_novol}' alt='Equity Curve Chart (No Volume)'/>"

    # Drawdown Chart
    dd_chart_b64 = _generate_drawdown_chart(timeseries_df)
    html += "<h2>Drawdown Chart</h2>"
    html += f"<img class='chart' src='data:image/png;base64,{dd_chart_b64}' alt='Drawdown Chart'/>"

    html += "</div></body></html>"
    return html


def _strategy_descriptions_section() -> str:
    text = """
        <div class='strategy-descriptions'>
        <p>Initial investment of $100,000. Test for 16 years (2008-12-26 to 2024-12-06) rebalanced quarterly. On each rebalance date, assign allocation weights as follows:</p>
          <p>Benchmarks:</p>

          <p><strong>(1) Market Only:</strong> weight(SPY) = 1</p>
          <p><strong>(2) Risk-Managed Market:</strong> weight(SPY) = 1 if (50-day MA of SPY) &gt; (200-day MA of SPY); otherwise weight(BIL) = 1.</p>

          <p>Simple Rebalancings: For all stocks in the S&amp;P 500 over the previous quarter:</p>
          <p><strong>(3) Flattened:</strong> weight(i) = 1/N, where N is the number of stocks in the S&amp;P 500.</p>
          <p><strong>(4) Leader:</strong> weight(i) = L<sub>i</sub> / Σ(L<sub>j</sub>), where L<sub>i</sub> is stock i's average daily return over the previous 63 days minus the market's.</p>
          <p><strong>(5) Volume:</strong> weight(i) = V<sub>i</sub> / Σ(V<sub>j</sub>), where V<sub>i</sub> is stock i's total volume over the previous 63 days.</p>
        </div>
    """
    return text


def _rename_and_round_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "strategy_name": "Strategy",
        "final_value": "Final Value",
        "total_return": "Total Return",
        "annualized_return": "Annualized Return",
        "max_drawdown": "Max Drawdown",
        "sharpe_ratio": "Sharpe Ratio"
    }
    df = df.rename(columns=rename_map)

    if "Final Value" in df.columns:
        df["Final Value"] = df["Final Value"].apply(
            lambda x: f"{float(x):,.2f}" if x != "" else ""
        )

    for col in ["Total Return", "Annualized Return", "Max Drawdown", "Sharpe Ratio"]:
        if col in df.columns:
            def round_or_blank(v):
                if v == "" or pd.isnull(v):
                    return ""
                try:
                    return f"{float(v):.4f}"
                except ValueError:
                    return v
            df[col] = df[col].apply(round_or_blank)

    return df


def _compute_additional_metrics(timeseries_df: pd.DataFrame) -> pd.DataFrame:
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

        daily_returns = pv.pct_change().dropna()
        if daily_returns.empty:
            results.append({
                "strategy": col,
                "annualized_return": None,
                "max_drawdown": None,
                "sharpe_ratio": None
            })
            continue

        mean_daily = daily_returns.mean()
        annual_ret = (1 + mean_daily)**252 - 1

        running_max = pv.cummax()
        drawdowns = (pv - running_max) / running_max
        max_dd = drawdowns.min()

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

    return pd.DataFrame(results).set_index("strategy")


def _generate_equity_curve(timeseries_df: pd.DataFrame) -> str:
    """
    Generates a line chart for all columns in timeseries_df (full date range), with no plot title.
    """
    if timeseries_df.empty:
        logger.warning("No timeseries data to plot.")
        return ""

    fig, ax = plt.subplots(figsize=(8,5))
    for col in timeseries_df.columns:
        ax.plot(timeseries_df.index, timeseries_df[col], label=col)

    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _generate_equity_curve_pre2013(timeseries_df: pd.DataFrame) -> str:
    """
    Same as _generate_equity_curve, but only plots data up to 2013-01-01.
    """
    if timeseries_df.empty:
        logger.warning("No timeseries data to plot (pre-2013).")
        return ""

    # Filter out dates >= 2013
    cutoff_date = pd.Timestamp("2013-01-01")
    df_pre2013 = timeseries_df.loc[timeseries_df.index < cutoff_date]
    if df_pre2013.empty:
        logger.warning("No data found before 2013-01-01.")
        return ""

    fig, ax = plt.subplots(figsize=(8,5))
    for col in df_pre2013.columns:
        ax.plot(df_pre2013.index, df_pre2013[col], label=col)

    # No title to match your request
    ax.set_xlabel("Date (Pre-2013)")
    ax.set_ylabel("Portfolio Value")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _generate_equity_curve_excluding(timeseries_df: pd.DataFrame, exclude: str = "volume") -> str:
    """
    Generates a line chart for all columns in timeseries_df except the exclude strategy,
    with no plot title.
    """
    if timeseries_df.empty:
        logger.warning("No timeseries data to plot.")
        return ""

    columns_to_plot = [c for c in timeseries_df.columns if exclude not in c.lower()]
    if not columns_to_plot:
        logger.warning(f"No columns left after excluding {exclude}.")
        return ""

    fig, ax = plt.subplots(figsize=(8,5))
    for col in columns_to_plot:
        ax.plot(timeseries_df.index, timeseries_df[col], label=col)

    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _generate_drawdown_chart(timeseries_df: pd.DataFrame) -> str:
    """
    Creates a drawdown chart for each strategy's portfolio_value. No plot title or 'drawdown' in legend.
    """
    if timeseries_df.empty:
        return ""

    fig, ax = plt.subplots(figsize=(8,5))
    for col in timeseries_df.columns:
        pv = timeseries_df[col].copy()
        running_max = pv.cummax()
        drawdown = (pv - running_max) / running_max
        ax.plot(drawdown.index, drawdown, label=col)

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


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    summary_csv_path = REPORT_DIR / "simple_strategies_comparison.csv"
    timeseries_csv_path = REPORT_DIR / "all_strategies_timeseries.csv"
    artifacts_path = REPORT_DIR / "simple_strategies_artifacts.pkl"
    output_html = REPORT_DIR / "simple_strategies_report.html"

    if not summary_csv_path.exists():
        logger.error(f"Missing {summary_csv_path}. Exiting.")
        return
    if not timeseries_csv_path.exists():
        logger.error(f"Missing {timeseries_csv_path}. Exiting.")
        return
    if not artifacts_path.exists():
        logger.warning(f"Missing {artifacts_path}; setting artifacts to {{}}.")
        artifacts = {}
    else:
        with open(artifacts_path, "rb") as f:
            artifacts = pickle.load(f)
        logger.info(f"Loaded artifacts from {artifacts_path}.")

    summary_df = pd.read_csv(summary_csv_path)
    logger.info(f"Loaded summary data: {summary_df.shape} rows.")

    timeseries_df = pd.read_csv(timeseries_csv_path, index_col=0, parse_dates=True)
    logger.info(f"Loaded timeseries data: {timeseries_df.shape} rows/cols.")

    create_five_strategies_report(
        summary_df=summary_df,
        timeseries_df=timeseries_df,
        artifacts=artifacts,
        output_html=str(output_html)
    )

    logger.info("Done generating 5-strategy report.")


if __name__ == "__main__":
    main()
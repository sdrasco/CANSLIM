"""
utils/grid_reporting.py

When run as a script:
  1) Loads previously saved artifacts (best_run.pkl, strategies_artifacts.pkl, and hybrid_gridsearch_results.csv).
  2) Rebuilds strategies_data for the equity curves (Market, Risk, best run).
  3) Calls create_gridsearch_report to generate a final HTML report that includes:
     - Distribution of total_return
     - Distribution of avg_picks across all runs
     - Best run’s pickcount distribution
     - Best run’s avg_picks
"""

import logging
import base64
import io
import os
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path
from sklearn.tree import DecisionTreeRegressor

from config.settings import INITIAL_FUNDS, START_DATE, END_DATE, REBALANCE_FREQUENCY

logger = logging.getLogger(__name__)


def create_gridsearch_report(
    strategies_data,
    best_run,
    df_results,
    output_path="gridsearch_report.html"
):
    """
    Creates an HTML report with:
      - Intro text describing the three strategies
      - Equity curves & metrics for the best model
      - Best hybrid param summary (including best_run["avg_picks"])
      - Decision tree parameter importance
      - 2D heatmap of total_return vs top two important parameters
      - Histogram of total_return distribution for all runs
      - Histogram of avg_picks across all runs (from df_results["avg_picks"])
      - Histogram of pick counts from the best run's data_dict
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
      max-width: 900px;
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
    p.summary, .param-text {
      font-size: 14px;
      color: #555;
      margin: 10px 0 30px 0;
      line-height: 1.6;
    }
    .metrics-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 15px;
    }
    .metrics-table th {
      text-align: left;
      background-color: #2a9d8f;
      color: #ffffff;
      padding: 8px;
    }
    .metrics-table td {
      padding: 8px;
      border-bottom: 1px solid #e9ecef;
    }
    .chart-image,
    .heatmap-img,
    .histogram-img,
    .pickcount-img {
      display: block;
      margin: 20px 0;
      border-radius: 10px;
      width: 100%;
      max-width: 700px;
    }
    .decision-tree-importances {
      margin-top: 30px;
      border: 1px solid #ccc;
      padding: 10px;
      background-color: #fafafa;
      border-radius: 5px;
      font-family: monospace;
    }
    </style>
    """

    mathjax_script = """
    <script>
    MathJax = {
      tex: {
        displayMath: [['$$','$$']]
      }
    };
    </script>
    <script src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>
    """

    summary_text = f"""
    This grid search started with an initial investment of {INITIAL_FUNDS:,.0f}
    and covered the period from {START_DATE} to {END_DATE}, rebalancing at a 
    {REBALANCE_FREQUENCY.title()} frequency.<br><br>
    <strong>Three Strategies:</strong><br>
    1) Market-Only: invests 100% in the market proxy at all times.<br>
    2) Risk-Managed: invests in the market proxy only if a 50/200 MA cross is bullish, 
       otherwise invests in a money market proxy.<br>
    3) Hybrid CANSLIM: invests all stocks from the S&amp;P 500 with (C or A) &amp; L, weighted by score:
        $$
         \\text{{score}}
       = \\frac{{\\text{{close}}}}{{\\text{{(52 week high)}}}} 
       + \\frac{{\\text{{volume}}}}{{\\text{{(50 day vol avg)}}}}~,
       $$
       or defaults to market proxy if no picks.
    <br><br>
    """

    html_content = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
    html_content += "<title>Hybrid Grid Search Report</title>"
    html_content += css
    html_content += mathjax_script
    html_content += "</head><body><div class='container'>"

    html_content += "<h1>Grid Search Results</h1>"
    html_content += f"<p class='summary'>{summary_text}</p>"

    # 1) Equity Curves & Metrics
    html_content += "<h2>Best Model: Equity Curves & Metrics</h2>"
    eq_img_base64 = _generate_equity_chart_base64(strategies_data)
    html_content += f"<img src='data:image/png;base64,{eq_img_base64}' alt='Equity Curve' class='chart-image' />"

    eq_table_html = _generate_metrics_table(strategies_data)
    html_content += eq_table_html

    # 2) Best Hybrid Param Summary
    best_params = best_run["params"]
    param_summary = "<br>".join([f"<strong>{k}:</strong> {v}" for k, v in best_params.items()])

    # Also show best_run["avg_picks"] if present
    avg_picks_best = best_run.get("avg_picks", 0.0)
    param_html = f"""
    <h3>Best Hybrid Parameter Set</h3>
    <div class="param-text">
      {param_summary}<br>
      <strong>Avg Picks (non-zero rebalances)</strong>: {avg_picks_best:.2f}
    </div>
    """
    html_content += param_html

    # 3) Decision Tree Param Significance
    html_content += "<h2>Decision Tree Parameter Significance</h2>"
    dt_importances_html = _compute_decision_tree_importance(df_results)
    html_content += f"<div class='decision-tree-importances'>{dt_importances_html}</div>"

    # 4) Heatmap
    html_content += "<h2>Heatmap of Total Returns</h2>"
    html_content += "<p class='summary'>2D heatmap showing total_return vs top two parameters determined by the decision tree.</p>"
    heatmap_b64 = _generate_2d_heatmap_base64(df_results)
    if heatmap_b64:
        html_content += f"<img src='data:image/png;base64,{heatmap_b64}' class='heatmap-img' alt='Return Heatmap'/>"
    else:
        html_content += "<p>Could not render the 2D heatmap (missing data or columns?).</p>"

    # 5) Histogram of total_return distribution
    html_content += "<h2>Distribution of Total Return</h2>"
    hist_b64 = _generate_return_histogram_base64(df_results)
    if hist_b64:
        html_content += f"<img src='data:image/png;base64,{hist_b64}' class='histogram-img' alt='Return Histogram'/>"
    else:
        html_content += "<p>Could not generate histogram (no total_return in df?).</p>"

    # 6) Histogram of average picks across all runs (df_results["avg_picks"])
    if "avg_picks" in df_results.columns:
        html_content += "<h2>Distribution of Average Picks (non-zero) Across All Runs</h2>"
        picks_hist_b64 = _generate_avg_picks_histogram_base64(df_results)
        if picks_hist_b64:
            html_content += f"<img src='data:image/png;base64,{picks_hist_b64}' class='histogram-img' alt='Avg picks histogram'/>"
        else:
            html_content += "<p>No valid 'avg_picks' data to plot.</p>"
    else:
        html_content += "<p>No 'avg_picks' column found in your CSV, skipping distribution of picks.</p>"

    # 7) Histogram of best run's pick counts
    pick_counts = best_run["data_dict"].get("hybrid_pick_counts", [])
    if pick_counts:
        html_content += "<h2>Pick Count Distribution in Best Run</h2>"
        pickcount_b64 = _generate_pickcount_histogram_base64(pick_counts)
        html_content += f"<img src='data:image/png;base64,{pickcount_b64}' class='pickcount-img' alt='Pick Count Histogram'/>"
    else:
        html_content += "<p>No pick counts recorded in best run. Possibly 0 picks or missing data?</p>"

    html_content += "</div></body></html>"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(f"Grid search report saved to {output_path}")


# --------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------

def _generate_equity_chart_base64(strategies_data):
    fig, ax = plt.subplots(figsize=(8,4))
    colors = ["#2a9d8f", "#e76f51", "#264653", "#f4a261", "#e9c46a"]
    
    for i, (strategy_name, portfolio_history, metrics) in enumerate(strategies_data):
        color = colors[i % len(colors)]
        ax.plot(
            portfolio_history["date"], 
            portfolio_history["portfolio_value"], 
            label=strategy_name, 
            linewidth=2, 
            color=color
        )

    ax.set_title("Equity Curves - Best Model vs. Others", fontsize=14, color="#264653")
    ax.set_xlabel("Date", color="#264653")
    ax.set_ylabel("Portfolio Value", color="#264653")
    ax.grid(True, color="#e9ecef", linestyle="--", linewidth=0.5)
    ax.legend()

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _generate_metrics_table(strategies_data):
    all_keys = set()
    for _, _, metrics in strategies_data:
        all_keys.update(metrics.keys())
    all_keys = sorted(all_keys)

    header_row = "<tr><th>Metric</th>" + "".join([f"<th>{name}</th>" for name, _, _ in strategies_data]) + "</tr>"

    def fmt(val):
        if isinstance(val, (int, float)):
            return f"{val:.4f}"
        return str(val)

    rows = ""
    for key in all_keys:
        row = f"<tr><td>{key.replace('_',' ').title()}</td>"
        for _, _, metrics in strategies_data:
            val = metrics.get(key, "")
            row += f"<td>{fmt(val)}</td>"
        row += "</tr>"
        rows += row

    table_html = f"""
    <table class="metrics-table">
      <thead>
        {header_row}
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
    """
    return table_html


def _compute_decision_tree_importance(df_results):
    needed_cols = {"C", "A", "L", "total_return"}
    if not needed_cols.issubset(df_results.columns):
        logger.warning("Missing columns for DT analysis. Skipping.")
        return "(No Decision Tree due to missing columns.)"

    working_df = df_results.dropna(subset=["C","A","L","total_return"]).copy()
    if working_df.empty:
        return "(No data for DT analysis.)"

    X = working_df[["C","A","L"]]
    y = working_df["total_return"]
    dt = DecisionTreeRegressor(random_state=42)
    dt.fit(X,y)

    importances = dt.feature_importances_
    names = ["C","A","L"]
    sorted_imp = sorted(zip(names, importances), key=lambda x: x[1], reverse=True)

    txt = "<pre>\nDecision Tree Feature Importances:\n"
    for nm, imp in sorted_imp:
        txt += f"  {nm} => {imp:.4f}\n"
    txt += "</pre>\n"
    return txt


def _generate_2d_heatmap_base64(df_results):
    needed_cols = {"C","A","L","total_return"}
    if not needed_cols.issubset(df_results.columns):
        logger.warning("Missing columns for heatmap. Skipping.")
        return None

    working_df = df_results.dropna(subset=["C","A","L","total_return"]).copy()
    if working_df.empty:
        return None

    X = working_df[["C","A","L"]]
    y = working_df["total_return"]
    dt = DecisionTreeRegressor(random_state=42)
    dt.fit(X,y)

    importances = dt.feature_importances_
    names = ["C","A","L"]
    sorted_f = sorted(zip(names, importances), key=lambda x:x[1], reverse=True)
    if len(sorted_f) < 2:
        return None

    param1, _ = sorted_f[0]
    param2, _ = sorted_f[1]

    pivot_df = working_df.groupby([param1, param2])["total_return"].mean().unstack(param2)
    if pivot_df.empty:
        return None

    fig, ax = plt.subplots(figsize=(8,6))
    sns.heatmap(pivot_df, ax=ax, cmap="viridis", annot=False, fmt=".2f")
    ax.set_title(f"Heatmap of total_return over {param1} vs {param2}")
    ax.set_xlabel(param2)
    ax.set_ylabel(param1)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _generate_return_histogram_base64(df_results):
    if "total_return" not in df_results.columns:
        logger.warning("No 'total_return' column; skipping distribution.")
        return ""
    working_df = df_results.dropna(subset=["total_return"])
    if working_df.empty:
        return ""

    fig, ax = plt.subplots(figsize=(6,4))
    ax.hist(working_df["total_return"], bins=30, color="#2a9d8f", alpha=0.9, edgecolor="white")
    ax.set_title("Distribution of Total Return", fontsize=12, color="#264653")
    ax.set_xlabel("Total Return", color="#264653")
    ax.set_ylabel("Frequency", color="#264653")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _generate_avg_picks_histogram_base64(df_results):
    """
    Create a histogram of the 'avg_picks' column across all runs.
    """
    if "avg_picks" not in df_results.columns:
        return ""
    working_df = df_results.dropna(subset=["avg_picks"]).copy()
    if working_df.empty:
        return ""

    # Convert to numeric just in case
    working_df["avg_picks"] = pd.to_numeric(working_df["avg_picks"], errors="coerce")
    working_df = working_df.dropna(subset=["avg_picks"])
    if working_df.empty:
        return ""

    fig, ax = plt.subplots(figsize=(6,4))
    ax.hist(working_df["avg_picks"], bins=30, color="#2a9d8f", alpha=0.9, edgecolor="white")
    ax.set_title("Distribution of Avg Picks (non-zero) Per Run", fontsize=12, color="#264653")
    ax.set_xlabel("Avg Picks (non-zero rebalances)", color="#264653")
    ax.set_ylabel("Frequency", color="#264653")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _generate_pickcount_histogram_base64(pick_counts):
    if not pick_counts:
        return ""
    fig, ax = plt.subplots(figsize=(6,4))
    ax.hist(pick_counts, bins=20, color="#e76f51", alpha=0.9, edgecolor="white")
    ax.set_title("Distribution of # Stocks Picked per Rebalance (Best Run)", fontsize=12, color="#264653")
    ax.set_xlabel("# Stocks Picked", color="#264653")
    ax.set_ylabel("Frequency", color="#264653")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def main():
    """
    Example usage: 
      python grid_reporting.py

    This will:
      - Load best_run from best_run.pkl
      - Load entire grid CSV (with avg_picks)
      - (Optionally) load strategies_artifacts if needed
      - Produce a final HTML report
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Where we expect artifacts to be
    artifacts_dir = Path("html/")
    best_run_path = artifacts_dir / "best_run.pkl"
    grid_csv_path = artifacts_dir / "hybrid_gridsearch_results.csv"
    artifacts_path = artifacts_dir / "strategies_artifacts.pkl"

    # Output HTML
    output_html = artifacts_dir / "gridsearch_report.html"

    # 1) Load best_run
    if not best_run_path.exists():
        logger.error(f"Cannot find {best_run_path}. Exiting.")
        return
    with open(best_run_path, "rb") as f:
        best_run = pickle.load(f)
    logger.info(f"Loaded best_run from {best_run_path}.")

    # 2) Load the grid results CSV
    if not grid_csv_path.exists():
        logger.error(f"Cannot find {grid_csv_path}. Exiting.")
        return
    df_results = pd.read_csv(grid_csv_path)
    # Convert to numeric
    for col in ["final_value","total_return","avg_picks"]:
        if col in df_results.columns:
            df_results[col] = pd.to_numeric(df_results[col], errors="coerce")

    logger.info(f"Loaded grid search results from {grid_csv_path}, shape={df_results.shape}.")

    # 3) Load strategies artifacts (Market, Risk) if needed
    if not artifacts_path.exists():
        logger.warning(f"Cannot find {artifacts_path}. We'll skip Market/Risk data.")
        strategies_data = []
    else:
        with open(artifacts_path, "rb") as f:
            strategies_artifacts = pickle.load(f)
        logger.info(f"Loaded strategies artifacts from {artifacts_path}.")

        market_history = strategies_artifacts["market_history"]
        market_metrics = strategies_artifacts["market_metrics"]
        risk_history   = strategies_artifacts["risk_history"]
        risk_metrics   = strategies_artifacts["risk_metrics"]

        # best run details
        best_history = best_run["history"]
        best_metrics = best_run["metrics"]

        strategies_data = [
            ("Market Only", market_history, market_metrics),
            ("Risk-Managed Market", risk_history, risk_metrics),
            ("BEST Hybrid", best_history, best_metrics),
        ]

    logger.info(f"Generating final HTML report at {output_html}...")

    create_gridsearch_report(
        strategies_data=strategies_data,
        best_run=best_run,
        df_results=df_results,
        output_path=str(output_html)
    )

    logger.info("Report generation complete!")


if __name__ == "__main__":
    main()
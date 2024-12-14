# utils/reporting.py

import logging
import base64
import io
import matplotlib.pyplot as plt
import pandas as pd
from utils.logging_utils import configure_logging
from config.settings import INITIAL_FUNDS, START_DATE, END_DATE, REBALANCE_FREQUENCY

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def generate_combined_equity_curve_chart(strategies_data):
    """
    Generate a single equity curve chart that overlays the equity curves of all strategies.
    Returns a base64-encoded PNG image.
    """
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

    ax.set_title("Equity Curves - All Strategies", fontsize=14, color="#264653")
    ax.set_xlabel("Date", color="#264653")
    ax.set_ylabel("Portfolio Value", color="#264653")
    ax.grid(True, color="#e9ecef", linestyle="--", linewidth=0.5)
    ax.legend()

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode("utf-8")
    return img_base64

def generate_combined_metrics_table(strategies_data):
    """
    Generate a single HTML table that compares metrics for all strategies side-by-side.
    Assumes each metrics_dict contains the same keys. If not, union all keys.
    """
    # Collect all keys
    all_keys = set()
    for _, _, metrics in strategies_data:
        all_keys.update(metrics.keys())
    all_keys = sorted(all_keys)

    # Create a header row with strategy names
    header_row = "<tr><th>Metric</th>" + "".join([f"<th>{name}</th>" for name, _, _ in strategies_data]) + "</tr>"

    # Create rows for each metric
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

def generate_canslim_criteria_section(canslim_criteria_dict):
    """
    Generate an HTML section summarizing the CANSLIM criteria and their parameters.
    Now that parameters is a single numeric or string value, we no longer treat it as a dict.
    """
    if not canslim_criteria_dict:
        return ""

    rows = ""
    for letter, info in canslim_criteria_dict.items():
        name = info.get("name", letter)
        description = info.get("description", "")
        param = info.get("parameters", "N/A")
        
        # Convert numeric params to a nicely formatted string
        if isinstance(param, float):
            param_str = f"{param:.2f}"
        else:
            # If param is not float (e.g., 'N/A' or a string), just convert to string
            param_str = str(param)

        rows += f"""
        <tr>
          <td>{letter}</td>
          <td>{name}</td>
          <td>{description}</td>
          <td>{param_str}</td>
        </tr>
        """

    html = f"""
    <h2>CANSLIM Criteria</h2>
    <table class="canslim-table">
      <thead>
        <tr><th>Letter</th><th>Name</th><th>Description</th><th>Parameter/Threshold</th></tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
    """
    return html

def generate_canslim_investments_table(canslim_investments):
    """
    Generate an HTML table detailing the CANSLIM non-money-market investment periods.
    Each entry in canslim_investments might look like:
    {
       "date": some_date,
       "investments": [{"ticker": t, "weight": w}, ...]
    }

    We've removed portfolio_value_before_rebalance from the display.
    """
    if not canslim_investments:
        return ""

    rows = ""
    for entry in canslim_investments:
        date = entry.get("date", "")
        inv = entry.get("investments", [])
        # Join tickers and weights as a string
        inv_str = ", ".join([f"{d['ticker']} ({d['weight']:.2f})" for d in inv])
        rows += f"""
        <tr>
          <td>{date}</td>
          <td>{inv_str}</td>
        </tr>
        """

    html = f"""
    <h2>CANSLIM Investment Periods</h2>
    <table class="canslim-investments-table">
      <thead>
        <tr><th>Date</th><th>Investments</th></tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
    """
    return html

def create_html_report(strategies_data, 
                       canslim_criteria_dict=None, 
                       canslim_investments=None, 
                       output_path="report.html"):
    """
    Create an HTML report that includes:
    - A summary at the top about initial investment, start/end dates, and rebalancing frequency
    - One chart with all strategies' equity curves overlayed
    - One table with all strategies' metrics side-by-side
    - A summary of the CANSLIM criteria and parameters
    - A table of CANSLIM non-money-market investment periods
    """
    if canslim_criteria_dict is None:
        canslim_criteria_dict = {}

    if canslim_investments is None:
        canslim_investments = []

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
      max-width: 800px;
      margin: 40px auto;
      padding: 20px;
      background-color: #ffffff;
      border-radius: 8px;
      box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    h1 {
      text-align: center;
      color: #264653;
    }
    h2 {
      color: #264653;
      border-bottom: 2px solid #2a9d8f;
      padding-bottom: 5px;
      margin-top: 40px;
    }
    p.summary {
      font-size: 14px;
      color: #555;
      margin: 10px 0 30px 0;
    }
    .metrics-table, .canslim-table, .canslim-investments-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 15px;
    }
    .metrics-table th, .canslim-table th, .canslim-investments-table th {
      text-align: left;
      background-color: #2a9d8f;
      color: #ffffff;
      padding: 8px;
    }
    .metrics-table td, .canslim-table td, .canslim-investments-table td {
      padding: 8px;
      border-bottom: 1px solid #e9ecef;
    }
    .chart-image {
      display: block;
      margin: 20px 0;
      border-radius: 10px;
      width: 100%;
      max-width: 100%;
    }
    </style>
    """

    img_base64 = generate_combined_equity_curve_chart(strategies_data)
    table_html = generate_combined_metrics_table(strategies_data)
    canslim_criteria_html = generate_canslim_criteria_section(canslim_criteria_dict) if canslim_criteria_dict else ""
    canslim_investments_html = generate_canslim_investments_table(canslim_investments) if canslim_investments else ""

    # Add a summary paragraph at the top
    summary_text = f"""
    This report presents the results of a backtest starting with an initial investment of {INITIAL_FUNDS:,}. 
    The analysis covers the period from {START_DATE} to {END_DATE}, and the portfolio was rebalanced 
    at a {REBALANCE_FREQUENCY.title()} frequency.
    """

    html_content = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
    html_content += "<title>Backtest Report</title>"
    html_content += css
    html_content += "</head><body><div class='container'>"
    html_content += "<h1>Backtest Results</h1>"
    html_content += f"<p class='summary'>{summary_text}</p>"

    # Equity curves
    html_content += "<h2>Equity Curves</h2>"
    html_content += f"<img src='data:image/png;base64,{img_base64}' alt='Equity Curve' class='chart-image' />"

    # Metrics
    html_content += "<h2>Metrics Comparison</h2>"
    html_content += table_html

    # CANSLIM Criteria
    if canslim_criteria_html:
        html_content += canslim_criteria_html

    # CANSLIM Investments
    if canslim_investments_html:
        html_content += canslim_investments_html

    html_content += "</div></body></html>"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(f"Report saved to {output_path}")
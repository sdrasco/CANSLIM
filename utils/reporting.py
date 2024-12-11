import logging
import base64
import io
import matplotlib.pyplot as plt
import pandas as pd
from utils.logging_utils import configure_logging  # Updated import

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

def generate_equity_curve_chart(portfolio_history: pd.DataFrame, strategy_name: str):
    """
    Generate an equity curve chart for the given portfolio history and return it as a base64-encoded PNG.
    """
    fig, ax = plt.subplots(figsize=(8,4))
    ax.plot(portfolio_history["date"], portfolio_history["portfolio_value"], color="#2a9d8f", linewidth=2)
    ax.set_title(f"Equity Curve - {strategy_name}", fontsize=14, color="#264653")
    ax.set_xlabel("Date", color="#264653")
    ax.set_ylabel("Portfolio Value", color="#264653")
    ax.grid(True, color="#e9ecef", linestyle="--", linewidth=0.5)

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=100)
    plt.close(fig)
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode("utf-8")
    return img_base64


def generate_metrics_table(metrics_dict: dict):
    """
    Generate an HTML table for the given metrics dictionary.
    """
    def fmt(val):
        if isinstance(val, (int, float)):
            return f"{val:.4f}"
        return str(val)

    rows = ""
    for k, v in metrics_dict.items():
        rows += f"<tr><td>{k.replace('_',' ').title()}</td><td>{fmt(v)}</td></tr>"

    table_html = f"""
    <table class="metrics-table">
      <thead>
        <tr><th>Metric</th><th>Value</th></tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
    """
    return table_html


def create_html_report(strategies_data, descriptions=None, output_path="report.html"):
    """
    Create an HTML report.
    strategies_data: list of tuples (strategy_name, portfolio_history_df, metrics_dict)
    descriptions: dict mapping strategy names to an object with a "description" field.
                  For example:
                  {
                    "Market Only (SPY)": {
                      "description": "This strategy invests entirely in SPY..."
                    },
                    "Risk Managed Market (BIL-SPY)": {
                      "description": "This strategy invests in either BIL or SPY..."
                    }
                  }

    If a description object is found, we extract the "description" field. If not, we show no description.
    """

    if descriptions is None:
        descriptions = {}

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
    }
    .strategy-section {
      margin-bottom: 40px;
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
    .chart-image {
      display: block;
      margin: 20px 0;
      border-radius: 10px;
      width: 100%;
      max-width: 100%;
    }
    .description {
      margin-top: 10px;
      color: #555;
    }
    </style>
    """

    html_content = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
    html_content += "<title>Backtest Report</title>"
    html_content += css
    html_content += "</head><body><div class='container'>"
    html_content += "<h1>Backtest Results</h1>"

    for strategy_name, portfolio_history, metrics_dict in strategies_data:
        html_content += "<div class='strategy-section'>"
        html_content += f"<h2>{strategy_name}</h2>"

        # Try to get the description object and extract the "description" field
        desc_obj = descriptions.get(strategy_name, {})
        if isinstance(desc_obj, dict):
            # Extract description field if present
            description_text = desc_obj.get("description", "")
        else:
            # If not a dict, assume a direct string or empty
            description_text = str(desc_obj) if desc_obj else ""

        if description_text:
            html_content += f"<div class='description'>{description_text}</div>"

        # metrics table
        html_content += generate_metrics_table(metrics_dict)

        # chart
        img_base64 = generate_equity_curve_chart(portfolio_history, strategy_name)
        html_content += f"<img src='data:image/png;base64,{img_base64}' alt='Equity Curve' class='chart-image' />"
        html_content += "</div>"

    html_content += "</div></body></html>"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(f"Report saved to {output_path}")
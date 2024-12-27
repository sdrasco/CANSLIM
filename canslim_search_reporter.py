#!/usr/bin/env python3

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Offscreen backend
import matplotlib.pyplot as plt
import seaborn as sns
import base64
import io
from sklearn.tree import DecisionTreeRegressor
import re

def generate_param_search_html(df: pd.DataFrame) -> str:
    """
    Generates:
      1) Histogram of total_return
      2) Summary stats (min, max, mean, median)
      3) Decision Tree measure of parameter importance
      4) An 'Appendix' full table of parameters
      (No correlation table).
    """
    # (A) HISTOGRAM
    fig, ax = plt.subplots(figsize=(5, 3))
    sns.histplot(df['total_return'], bins=15, ax=ax, kde=False, color="#007acc")
    ax.set_xlabel("total return")
    ax.set_ylabel("Count")

    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format='png')
    plt.close(fig)
    base64_hist = base64.b64encode(buf.getvalue()).decode('utf-8')
    histogram_html = f"""
    <h3>Total Return Distribution</h3>
    <img src='data:image/png;base64,{base64_hist}' 
         alt='Histogram total_return' 
         style='max-width:500px; border-radius:5px;' />
    """

    # (B) SUMMARY STATS
    stats = (
        df.describe()
          .loc[['min', 'max', 'mean', '50%']]
          .rename(index={'50%': 'median'})
    )
    stats_html = (
        stats.style
        .set_properties(**{"text-align": "right"})
        .set_table_styles([
            {"selector": "th",
             "props": [("text-align", "center"),
                       ("background-color", "#2a9d8f"),
                       ("color", "#ffffff")]},
            {"selector": "td",
             "props": [("padding", "6px 12px")]}
        ])
        .to_html()
    )

    # (C) DECISION TREE PARAM IMPORTANCE
    X = df[['C', 'A', 'vol_factor', 'ad_ratio']].fillna(0)
    y = df['total_return'].fillna(0)
    reg = DecisionTreeRegressor(random_state=42)
    reg.fit(X, y)
    importances = reg.feature_importances_

    param_import_df = pd.DataFrame({
        'parameter': X.columns,
        'importance': importances
    }).sort_values('importance', ascending=False)

    import_html = (
        param_import_df.style
        .set_properties(**{"text-align": "right"})
        .set_table_styles([
            {"selector": "th",
             "props": [("text-align", "center"),
                       ("background-color", "#2a9d8f"),
                       ("color", "#ffffff")]},
            {"selector": "td",
             "props": [("padding", "6px 12px")]}
        ])
        .format("{:.4f}", subset=["importance"])
        .hide(axis="index")
        .to_html()
    )

    # (D) APPENDIX: FULL TABLE
    df_subset = df[['C', 'A', 'vol_factor', 'ad_ratio', 'total_return']].copy()
    big_table_html = (
        df_subset.style
        .set_properties(**{"text-align": "right"})
        .set_table_styles([
            {"selector": "th",
             "props": [("text-align", "center"),
                       ("background-color", "#2a9d8f"),
                       ("color", "#ffffff")]},
            {"selector": "td",
             "props": [("padding", "6px 12px")]}
        ])
        .format("{:.4f}", subset=["C", "A", "vol_factor", "ad_ratio", "total_return"])
        .hide(axis="index")
        .to_html()
    )

    html_fragment = f"""
    <h2>CANSLIM Parameter Search Summary</h2>

    {histogram_html}

    <h3>Basic Statistics</h3>
    {stats_html}

    <h3>Decision Tree Parameter Importances</h3>
<p class='summary'>
  To understand how parameters influence performance, we used a simple machine learning model called a Decision Tree Regressor. It works like a flowchart, asking "yes or no" questions to predict total returns and assigning importance scores to each parameter.
  This is handy, but overkill given our backtest results. Correlation gives similar results.
</p>
    {import_html}

    <h3>Appendix: Full Parameter Search Results</h3>
    <p class='summary'>
      All 625 parameter configurations with returns. Here for completeness.
    </p>
    {big_table_html}
    """
    return html_fragment


def main():
    # 1) Load your parameter CSV
    df_params = pd.read_csv("data/canslim_param_search_results.csv")

    # 2) Load the best_case_report.html
    with open("best_case_report.html", "r", encoding="utf-8") as f:
        original_html = f.read()

    # 3) Replace the <h1>Backtest Results</h1> with new heading
    old_heading = "<h1>Backtest Results</h1>"
    new_heading = "<h1>Pure CANSLIM Backtest Results</h1>"
    modified_html = original_html.replace(old_heading, new_heading)

    # 4) Replace the old paragraph block with your new text & table
    old_paragraph_pattern = re.compile(
        r"<p class='summary'>.*?</p>", re.DOTALL
    )

    new_paragraph = """
<p class='summary'>
  Results of testing one implementation of CANSLIM versus two benchmark strategies:<br><br>
  
  - <b>Market Only Strategy</b>: Fully invests in SPY without any adjustments.<br><br>
  
  - <b>Risk-Managed Market Strategy</b>: Dynamically allocates between SPY and BIL based on the M in CANSLIM below. <br><br>
  
  O’Neil is careful not to explicitly define CANSLIM. Our version works by insisting each of these be true (allowing at most six stocks per quarter)
</p>

<table class="canslim-intro-table" style="width:100%; border-collapse: collapse; margin: 15px 0;">
  <thead style="background-color: #2a9d8f; color: white;">
    <tr>
      <th style="padding: 8px; text-align: left;">Letter</th>
      <th style="padding: 8px; text-align: left;">Name</th>
      <th style="padding: 8px; text-align: left;">Description / Implementation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="padding: 8px;">C</td>
      <td style="padding: 8px;">Current Quarterly Earnings</td>
      <td style="padding: 8px;">Quarterly year-over-year EPS growth &gt; threshold</td>
    </tr>
    <tr>
      <td style="padding: 8px;">A</td>
      <td style="padding: 8px;">Annual Earnings Growth</td>
      <td style="padding: 8px;">Year-over-year EPS growth &gt; threshold</td>
    </tr>
    <tr>
      <td style="padding: 8px;">N</td>
      <td style="padding: 8px;">New High</td>
      <td style="padding: 8px;">during lookback period</td>
    </tr>
    <tr>
      <td style="padding: 8px;">S</td>
      <td style="padding: 8px;">Supply &amp; Demand</td>
      <td style="padding: 8px;">Volume/(average daily volume) &gt; threshold</td>
    </tr>
    <tr>
      <td style="padding: 8px;">L</td>
      <td style="padding: 8px;">Leader / Laggard</td>
      <td style="padding: 8px;">(stock_return - market_return) &gt; threshold </td>
    </tr>
    <tr>
      <td style="padding: 8px;">I</td>
      <td style="padding: 8px;">Institutional Sponsorship</td>
      <td style="padding: 8px;">A/D metric above threshold</td>
    </tr>
    <tr>
      <td style="padding: 8px;">M</td>
      <td style="padding: 8px;">Market Direction</td>
      <td style="padding: 8px;">(50-day MA) - (200-day MA) &gt; threshold</td>
    </tr>
  </tbody>
</table>

<p class='summary'>
  The backtest started with an initial investment of $100,000, covering just under 16 years 
  (2008-12-26 to 2024-12-06) rebalanced quarterly. We also tested weekly rebalancing, 
  but it always performed worse. Things go similarly for O'Neil's FFTY ETF, a weekly, rules-based, 
  computer-generated stock index compiled and published by Investor's Business Daily, shown against SPY just below 
  <a href="https://www.google.com/finance/quote/FFTY:NYSEARCA?hl=en&comparison=NYSEARCA%3ASPY&window=5Y" 
     target="_blank" style="color: #2a9d8f; text-decoration: none;">
    from Google Finance.
  </a>
</p>

<img src="images/compare.png" alt="Comparison of Strategies" 
     style="display: block; margin: 20px auto; max-width: 100%; border-radius: 8px;" />
     """

    modified_html = re.sub(old_paragraph_pattern, new_paragraph.strip(), modified_html)

    # 5) Generate the snippet to insert
    param_search_fragment = generate_param_search_html(df_params)

    # 6) Insert the snippet before </div></body></html>
    insertion_marker = "</div></body></html>"
    final_html = modified_html.replace(
        insertion_marker,
        param_search_fragment + "\n" + insertion_marker
    )

    # 7) Write out the new file
    with open("canslim_report_with_params.html", "w", encoding="utf-8") as out:
        out.write(final_html)

    print("Done!")

if __name__ == "__main__":
    main()
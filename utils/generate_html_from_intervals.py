import csv
import json
from datetime import datetime

input_file = "../data/sp_500_intervals.csv"
output_file = "../html/sp500.html"

def parse_date(d):
    return datetime.strptime(d, "%Y-%m-%d").date() if d else None

# Read data
with open(input_file, 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

if not rows:
    print("No data found in sp_500_intervals.csv.")
    exit(1)

all_dates = []
tickers_info = []

for row in rows:
    ticker = row['ticker'].strip()
    num_entries_str = row.get('num_entries', '1').strip()
    try:
        num_entries = int(num_entries_str)
    except ValueError:
        num_entries = 1

    intervals = []
    for i in [1, 2, 3]:
        entry_col = f"entry_{i}"
        exit_col = f"exit_{i}"
        entry_val = row[entry_col].strip()
        exit_val = row[exit_col].strip()
        if entry_val:
            intervals.append((entry_val, exit_val if exit_val else None))

    for (start, end) in intervals:
        all_dates.append(parse_date(start))
        if end:
            all_dates.append(parse_date(end))

    tickers_info.append((ticker, num_entries, intervals))

valid_dates = [d for d in all_dates if d is not None]
if not valid_dates:
    print("No valid intervals in the file.")
    exit(1)

earliest_date = min(valid_dates)
latest_date = max(valid_dates)
total_range_days = (latest_date - earliest_date).days + 1

results = []
for (ticker, num_entries, intervals) in tickers_info:
    days_in_index = 0
    for (start, end) in intervals:
        start_date = parse_date(start)
        if end:
            end_date = parse_date(end)
        else:
            end_date = latest_date

        if start_date < earliest_date:
            start_date = earliest_date
        if end_date > latest_date:
            end_date = latest_date

        interval_days = (end_date - start_date).days + 1
        days_in_index += interval_days

    percentage = days_in_index / total_range_days
    structured_intervals = []
    for (s, e) in intervals:
        structured_intervals.append({"entry": s, "exit": e if e else ""})

    results.append({
        "ticker": ticker,
        "percentage": percentage,
        "num_entries": num_entries,
        "intervals": structured_intervals
    })

results.sort(key=lambda x: x["ticker"])

data_json = json.dumps(results)
earliest_str = earliest_date.strftime("%Y-%m-%d")
latest_str = latest_date.strftime("%Y-%m-%d")

html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>S&P 500 Tickers Over Time</title>

<!-- Open Graph meta tags -->
<meta property="og:title" content="S&P 500 Tickers Over Time" />
<meta property="og:description" content="Explore historical S&P 500 constituents and how they've changed over time." />
<meta property="og:image" content="https://backboard.uk/images/sp500tool.png" />
<meta property="og:url" content="https://backboard.uk/html/sp500.html" />
<meta property="og:type" content="website" />

<style>
    body {{
        margin: 20px;
        font-family: "Helvetica Neue", Arial, sans-serif;
        background: #f2f2f2;
        color: #333;
        position: relative;
        text-align: center;
    }}

    .container {{
        display: inline-block;
        background: #ebeff2;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        padding: 20px;
        max-width: 600px;
        width: 100%;
        margin-bottom: 20px;
        text-align: left;
    }}

    h1 {{
        margin-bottom: 10px;
        font-size: 24px;
        font-weight: normal;
        text-align: center;
    }}

    .legend p {{
        margin: 5px 0;
        font-size: 14px;
        line-height: 1.4;
    }}

    .legend a {{
        color: #336699;
        text-decoration: none;
    }}

    .author-note {{
        font-size: 12px;
        color: #666;
        margin-top: 10px;
        text-align: center;
    }}
    .author-note a {{
        color: #336699;
        text-decoration: none;
    }}

    .controls {{
        margin-top: 20px;
        text-align: center;
    }}

    .controls button {{
        font-size: 14px;
        padding: 6px 12px;
        border: none;
        background: #b6e2d3;
        border-radius: 4px;
        box-shadow: 0 0 3px rgba(0,0,0,0.15);
        cursor: pointer;
        transition: background 0.3s, box-shadow 0.3s;
        margin: 0 5px;
    }}

    .controls button:hover {{
        background: #a7d7c5;
        box-shadow: 0 0 6px rgba(0,0,0,0.2);
    }}

    .grid-container {{
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(60px, 1fr));
        grid-auto-rows: 60px;
        gap: 5px;
        margin-top: 10px;
    }}

    @media (max-width: 600px) {{
        .grid-container {{
            grid-template-columns: repeat(auto-fill, minmax(40px, 1fr));
            gap: 3px;
        }}
    }}

    .ticker-box {{
        position: relative;
        border: 1px solid #ccc;
        text-align: center;
        cursor: pointer;
        border-radius: 5px;
        overflow: hidden; 
    }}

    .ticker-box span {{
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        font-size: 12px;
        font-weight: bold;
        color: #000;
        opacity: 0;
        transition: opacity 0.2s;
        pointer-events: none; 
    }}

    .ticker-box:hover span {{
        opacity: 1;
    }}

    #tooltip {{
        display: none;
        position: absolute;
        background: #fff;
        border: 1px solid #ccc;
        padding: 5px;
        font-size: 12px;
        max-width: 200px;
        pointer-events: none;
        box-shadow: 0 0 5px rgba(0,0,0,0.3);
        border-radius: 3px;
        color: #333;
        z-index: 9999;
    }}

    #tooltip h2 {{
        margin: 0 0 5px 0;
        font-size: 14px;
        font-weight: bold;
    }}

    #tooltip p {{
        margin: 2px 0;
        font-size: 13px;
    }}

    #tooltip ul {{
        padding-left: 15px;
        margin: 5px 0 0 0;
        list-style-type: square;
    }}

    #tooltip li {{
        font-size: 12px;
    }}
</style>
</head>
<body>

<div class="container">
<h1>S&P 500 Tickers Over Time</h1>
<div class="legend">
    <p>Data spans from {earliest_str} to {latest_str}.</p>
    <p>Historical data <a href="https://github.com/hanshof/sp500_constituents" target="_blank">(6MB) version from this shady anonymous repository</a>. 
    Assume nothing.</p>
    <p>My rehashed <a href="https://backboard.uk/data/sp_500_intervals.csv" target="_blank">(35kB) version available here</a>.</p>
</div>

<div class="author-note">
    Made by <a href="mailto:steve.drasco@gmail.com">Steve Drasco</a>
</div>

<div class="controls">
    <button id="sort-alpha">Sort Alphabetically</button>
    <button id="sort-entries">Sort by Number of Entries</button>
    <button id="sort-percent">Sort by % of Time</button>
</div>
</div>

<div class="grid-container" id="grid"></div>

<div id="tooltip"></div>

<script>
var data = {data_json};

function renderGrid() {{
    var container = document.getElementById('grid');
    container.innerHTML = '';
    data.forEach(function(d) {{
        var boxOpacity = 0.2 + 0.8 * d.percentage;
        var div = document.createElement('div');
        div.className = 'ticker-box';
        div.style.backgroundColor = 'rgba(167,215,197,' + boxOpacity + ')';

        div.dataset.ticker = d.ticker;
        div.dataset.percentage = d.percentage;
        div.dataset.numEntries = d.num_entries;
        div.dataset.intervals = JSON.stringify(d.intervals);

        var span = document.createElement('span');
        span.textContent = d.ticker;
        div.appendChild(span);

        container.appendChild(div);
    }});
}}

document.getElementById('sort-alpha').addEventListener('click', function() {{
    data.sort(function(a,b) {{ return a.ticker.localeCompare(b.ticker); }});
    renderGrid();
}});

document.getElementById('sort-entries').addEventListener('click', function() {{
    data.sort(function(a,b) {{ return b.num_entries - a.num_entries; }});
    renderGrid();
}});

document.getElementById('sort-percent').addEventListener('click', function() {{
    data.sort(function(a,b) {{ return b.percentage - a.percentage; }});
    renderGrid();
}});

var tooltip = document.getElementById('tooltip');

document.addEventListener('mouseover', function(e) {{
    if (e.target.classList.contains('ticker-box')) {{
        var t = e.target.dataset.ticker;
        var p = parseFloat(e.target.dataset.percentage);
        var intervals = JSON.parse(e.target.dataset.intervals);
        var percStr = (p * 100).toFixed(2) + '%';

        var html = '<h2>' + t + '</h2>';
        html += '<p>Time in index: ' + percStr + '</p>';
        if (intervals.length > 0) {{
            html += '<p>Intervals:</p><ul>';
            intervals.forEach(function(iv) {{
                var entry = iv.entry;
                var exit = iv.exit || '(still in)';
                html += '<li>' + entry + ' to ' + exit + '</li>';
            }});
            html += '</ul>';
        }}

        tooltip.innerHTML = html;
        tooltip.style.display = 'block';
    }}
}});

document.addEventListener('mouseout', function(e) {{
    if (e.target.classList.contains('ticker-box')) {{
        tooltip.style.display = 'none';
    }}
}});

document.addEventListener('mousemove', function(e) {{
    if (tooltip.style.display === 'block') {{
        var xOffset = 15;
        var yOffset = 15;
        tooltip.style.left = (e.pageX + xOffset) + 'px';
        tooltip.style.top = (e.pageY + yOffset) + 'px';
    }}
}});

renderGrid();
</script>

</body>
</html>
"""

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_template)

print("HTML visualization updated.")
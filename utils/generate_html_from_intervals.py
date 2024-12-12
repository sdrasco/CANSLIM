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

    # Extract intervals (up to 3)
    intervals = []
    for i in [1, 2, 3]:
        entry_col = f"entry_{i}"
        exit_col = f"exit_{i}"
        entry_val = row[entry_col].strip()
        exit_val = row[exit_col].strip()
        if entry_val:
            intervals.append((entry_val, exit_val if exit_val else None))

    # Collect dates to find global range
    for (start, end) in intervals:
        all_dates.append(parse_date(start))
        if end:
            all_dates.append(parse_date(end))

    # Store the raw info to compute later
    tickers_info.append((ticker, num_entries, intervals))

# Compute global earliest and latest dates
valid_dates = [d for d in all_dates if d is not None]
if not valid_dates:
    print("No valid intervals in the file.")
    exit(1)

earliest_date = min(valid_dates)
latest_date = max(valid_dates)
total_range_days = (latest_date - earliest_date).days + 1

# Compute percentage of time in index for each ticker
results = []
for (ticker, num_entries, intervals) in tickers_info:
    days_in_index = 0
    for (start, end) in intervals:
        start_date = parse_date(start)
        if end:
            end_date = parse_date(end)
        else:
            end_date = latest_date

        # Clip to global range
        if start_date < earliest_date:
            start_date = earliest_date
        if end_date > latest_date:
            end_date = latest_date

        interval_days = (end_date - start_date).days + 1
        days_in_index += interval_days

    percentage = days_in_index / total_range_days
    # Store intervals in a structured form for tooltip
    structured_intervals = []
    for (s, e) in intervals:
        structured_intervals.append({"entry": s, "exit": e if e else ""})

    results.append({
        "ticker": ticker,
        "percentage": percentage,
        "num_entries": num_entries,
        "intervals": structured_intervals
    })

# Sort by ticker initially
results.sort(key=lambda x: x["ticker"])

data_json = json.dumps(results)

# Format earliest and latest date for display
earliest_str = earliest_date.strftime("%Y-%m-%d")
latest_str = latest_date.strftime("%Y-%m-%d")

html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>S&P 500 Ticker Tenure Visualization</title>
<style>
    body {{
        margin: 20px;
        font-family: sans-serif;
        position: relative;
    }}

    .controls {{
        margin-bottom: 20px;
    }}
    .controls button {{
        margin-right: 10px;
        padding: 5px 10px;
        cursor: pointer;
    }}

    .grid-container {{
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(60px, 1fr));
        grid-auto-rows: 60px;
        gap: 5px;
    }}

    .ticker-box {{
        position: relative;
        background-color: #98FB98; /* pale green */
        border: 1px solid #ccc;
        text-align: center;
        cursor: pointer;
    }}

    .ticker-box span {{
        /* Hidden by default, we now use tooltip instead */
        display: none;
    }}

    h1 {{
        margin-bottom: 10px;
    }}

    .legend {{
        margin-bottom: 20px;
    }}
    .legend p {{
        margin: 5px 0;
        font-size: 14px;
    }}

    #tooltip {{
        display: none;
        position: absolute;
        background: #fff;
        border: 1px solid #ccc;
        padding: 5px;
        font-size: 12px;
        max-width: 200px;
        pointer-events: none; /* so it doesn't interfere with hover */
        box-shadow: 0 0 5px rgba(0,0,0,0.3);
    }}

    #tooltip h2 {{
        margin: 0 0 5px 0;
        font-size: 14px;
        font-weight: bold;
    }}
    #tooltip p {{
        margin: 2px 0;
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

<h1>S&P 500 Ticker Tenure Visualization</h1>
<div class="legend">
    <p>Data spans from {earliest_str} to {latest_str}.</p>
    <p>Source: A historic data set found at this dubious source: <a href="https://github.com/hanshof/sp500_constituents" target="_blank">https://github.com/hanshof/sp500_constituents</a></p>
    <p>Each box represents a ticker. Hover over a box to see details:</p>
    <ul>
        <li>Ticker symbol</li>
        <li>% of time in the index (over the entire {earliest_str} to {latest_str} period)</li>
        <li>Entry & exit dates for its intervals</li>
    </ul>
</div>

<div class="controls">
    <button id="sort-alpha">Sort Alphabetically</button>
    <button id="sort-entries">Sort by Number of Entries</button>
    <button id="sort-percent">Sort by % of Time</button>
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
        div.style.opacity = boxOpacity;

        // Store data attributes for tooltip
        div.dataset.ticker = d.ticker;
        div.dataset.percentage = d.percentage;
        div.dataset.numEntries = d.num_entries;
        div.dataset.intervals = JSON.stringify(d.intervals);

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
        // Format percentage as, say, XX.XX%
        var percStr = (p * 100).toFixed(2) + '%';

        var html = '<h2>' + t + '</h2>';
        html += '<p>Time in index: ' + percStr + '</p>';
        if (intervals.length > 0) {{
            html += '<p>Intervals:</p><ul>';
            intervals.forEach(function(iv) {{
                var entry = iv.entry;
                var exit = iv.exit || '(still in)';
                html += '<li>' + entry + ' to ' + (exit || '') + '</li>';
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
        // Position tooltip near the mouse cursor
        var xOffset = 15;
        var yOffset = 15;
        tooltip.style.left = (e.pageX + xOffset) + 'px';
        tooltip.style.top = (e.pageY + yOffset) + 'px';
    }}
}});

// Initial render
renderGrid();
</script>

</body>
</html>
"""

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_template)

print("HTML visualization with sorting and tooltip generated:", output_file)
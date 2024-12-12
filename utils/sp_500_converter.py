import csv
from datetime import datetime
from collections import defaultdict

input_file = "../data/sp_500_historical_components.csv"
output_file = "../data/sp_500_intervals.csv"

dates_tickers = []
with open(input_file, 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for line in reader:
        date_str = line['date']
        tickers_str = line['tickers']
        tickers = [t.strip() for t in tickers_str.split(',') if t.strip()]
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        dates_tickers.append((date, tickers))

# Sort by date to ensure chronological order
dates_tickers.sort(key=lambda x: x[0])

# Dictionary to track intervals for each ticker: ticker -> list of [start_date, end_date]
ticker_intervals = defaultdict(list)

previous_tickers = set()
previous_date = None

for (current_date, current_tickers) in dates_tickers:
    current_tickers_set = set(current_tickers)

    newly_added = current_tickers_set - previous_tickers
    removed = previous_tickers - current_tickers_set if previous_tickers else set()

    # Start intervals for newly added tickers
    for t in newly_added:
        ticker_intervals[t].append([current_date, None])

    # Close intervals for removed tickers
    for t in removed:
        if ticker_intervals[t] and ticker_intervals[t][-1][1] is None:
            ticker_intervals[t][-1][1] = previous_date

    previous_tickers = current_tickers_set
    previous_date = current_date

# Build the final records
final_records = []
for t, intervals in ticker_intervals.items():
    # Sort intervals by start date
    intervals.sort(key=lambda x: x[0])
    interval_count = len(intervals)

    # Initialize all fields as empty
    entry_1, exit_1 = "", ""
    entry_2, exit_2 = "", ""
    entry_3, exit_3 = "", ""

    if interval_count == 1:
        (start_1, end_1) = intervals[0]
        entry_1 = start_1.strftime("%Y-%m-%d")
        exit_1 = end_1.strftime("%Y-%m-%d") if end_1 else ""
        num_entries = 1
        num_exits = 1 if end_1 else 0

    elif interval_count == 2:
        (start_1, end_1) = intervals[0]
        (start_2, end_2) = intervals[1]
        entry_1 = start_1.strftime("%Y-%m-%d")
        exit_1 = end_1.strftime("%Y-%m-%d") if end_1 else ""
        entry_2 = start_2.strftime("%Y-%m-%d")
        exit_2 = end_2.strftime("%Y-%m-%d") if end_2 else ""
        num_entries = 2
        num_exits = sum(1 for s, e in intervals if e is not None)

    elif interval_count == 3:
        (start_1, end_1) = intervals[0]
        (start_2, end_2) = intervals[1]
        (start_3, end_3) = intervals[2]

        entry_1 = start_1.strftime("%Y-%m-%d")
        exit_1 = end_1.strftime("%Y-%m-%d") if end_1 else ""
        entry_2 = start_2.strftime("%Y-%m-%d")
        exit_2 = end_2.strftime("%Y-%m-%d") if end_2 else ""
        entry_3 = start_3.strftime("%Y-%m-%d")
        exit_3 = end_3.strftime("%Y-%m-%d") if end_3 else ""

        num_entries = 3
        num_exits = sum(1 for s, e in intervals if e is not None)

    else:
        # More than three intervals - unexpected, skip this ticker
        continue

    final_records.append([
        t, entry_1, exit_1, entry_2, exit_2, entry_3, exit_3, num_entries, num_exits
    ])

# Write out the final CSV
with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["ticker", "entry_1", "exit_1", "entry_2", "exit_2", "entry_3", "exit_3", "num_entries", "num_exits"])
    writer.writerows(final_records)

print("Conversion complete. Results saved in:", output_file)
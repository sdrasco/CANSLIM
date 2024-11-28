import os
import pandas as pd
import matplotlib.pyplot as plt
from config.settings import DATA_DIR

# Base directory for data files
base_data_dir = os.path.join(DATA_DIR, "us_stocks_sip/day_aggs_v1")

def process_file(file_path):
    """
    Process a single file. Extracts the date from the file path,
    calculates the average volume, and returns it.
    """
    print(f"Processing: {file_path}")
    try:
        # Extract date from the file name
        file_name = os.path.basename(file_path)
        date = file_name.replace(".csv.gz", "")

        # Load the file into a DataFrame
        df = pd.read_csv(file_path, compression='gzip')

        # Calculate average volume
        avg_volume = df['volume'].mean()
        return date, avg_volume
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None, None

def collect_data(base_dir):
    """
    Traverse the directory structure and collect average volumes with dates.
    """
    data = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".csv.gz"):
                file_path = os.path.join(root, file)
                date, avg_volume = process_file(file_path)
                if date and avg_volume is not None:
                    data.append((date, avg_volume))
    return data

def plot_data(data):
    """
    Plot the average volume over time.
    """
    # Sort data by date
    data.sort(key=lambda x: x[0])  # Sort by date string
    dates, volumes = zip(*data)

    # Convert dates to pandas datetime for better plotting
    dates = pd.to_datetime(dates)

    # Plot
    plt.figure(figsize=(12, 6))
    plt.plot(dates, volumes, marker='o', linestyle='-')
    plt.title("Average Volume Over Time")
    plt.xlabel("Date")
    plt.ylabel("Average Volume")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Collect data
    collected_data = collect_data(base_data_dir)

    # Plot data
    if collected_data:
        plot_data(collected_data)
    else:
        print("No data to plot.")
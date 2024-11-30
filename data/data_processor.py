import os
import pandas as pd
import matplotlib.pyplot as plt
import logging
from config.settings import DATA_DIR
from config.configure_logging import configure_logging

# Configure logging
configure_logging()

# Create a logger for this module
logger = logging.getLogger(__name__)

# Base directory for Feather data files
base_data_dir = os.path.join(DATA_DIR, "us_stocks_sip/day_aggs_feather")

def process_file(file_path):
    """
    Process a single Feather file. Extracts the date from the file path,
    calculates the average volume, and returns it.
    """
    try:
        # Extract date from the file name
        file_name = os.path.basename(file_path)
        date = file_name.replace(".feather", "")

        # Load the file into a DataFrame
        logger.debug(f"Loading Feather file: {file_path}")
        df = pd.read_feather(file_path)

        # Ensure 'volume' column exists
        if 'volume' not in df.columns:
            logger.warning(f"Missing 'volume' column in {file_path}. Skipping.")
            return None, None

        # Calculate average volume
        avg_volume = df['volume'].mean()
        logger.debug(f"File {file_name}: Avg Volume = {avg_volume}")
        return date, avg_volume
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return None, None

def collect_data(base_dir):
    """
    Traverse the directory structure and collect average volumes with dates.
    """
    data = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".feather"):
                file_path = os.path.join(root, file)
                date, avg_volume = process_file(file_path)
                if date and avg_volume is not None:
                    #logger.info(f"Processed {file_path}: Date={date}, Avg Volume={avg_volume}")
                    data.append((date, avg_volume))
    return data

def plot_data(data):
    """
    Plot the average volume over time.
    """
    if not data:
        logger.warning("No data available for plotting.")
        return

    # Sort data by date
    data.sort(key=lambda x: x[0])  # Sort by date string
    dates, volumes = zip(*data)

    # Convert dates to pandas datetime and normalize
    dates = pd.to_datetime(dates).normalize()

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

    if collected_data:
        logger.info(f"Successfully collected data for {len(collected_data)} days.")
        plot_data(collected_data)
    else:
        logger.warning("No valid data found in the Feather files. Please check the input directory.")
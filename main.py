import os
import logging
from data.data_fetcher import fetch_data
from data.data_processor import collect_data, plot_data
from config.settings import DATA_DIR, START_DATE, END_DATE

# Configure basic logging
logging.basicConfig(
    level=logging.WARNING,
    format="%(message)s"
)

# Create a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main():
    # Define Feather data directory
    base_data_dir = os.path.join(DATA_DIR, "us_stocks_sip/day_aggs_feather")
    
    # Step 1: Fetch data
    logger.info(f"Fetching data for the range: {START_DATE} to {END_DATE}.")
    fetch_data()

    # Step 2: Process and analyze data
    logger.info("Processing collected data.")
    collected_data = collect_data(base_data_dir)

    # Step 3: Generate plot
    if collected_data:
        logger.info(f"Successfully collected data for {len(collected_data)} days.")
        plot_data(collected_data)
    else:
        logger.warning("No data available for processing or visualization.")

if __name__ == "__main__":
    main()
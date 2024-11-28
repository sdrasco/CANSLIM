import logging
from data.data_fetcher import fetch_data
from data.data_processor import collect_data, plot_data
from config.settings import DATA_DIR, START_DATE, END_DATE

# Configure basic logging.  show warning or higher for external modules.
logging.basicConfig(
    level=logging.WARNING,  
    format='%(message)s'
)

# Create a logger for this module
logger = logging.getLogger(__name__)

# Show info level logger events for this module
logger.setLevel(logging.INFO)

def main():
    # Step 1: Fetch data
    logger.info(f"Fetching data for the range: {START_DATE} to {END_DATE}.")
    fetch_data()

    # Step 2: Process and analyze data
    logger.info("Processing collected data.")
    base_data_dir = f"{DATA_DIR}/us_stocks_sip/day_aggs_v1"
    collected_data = collect_data(base_data_dir)

    # Step 3: Generate plot
    if collected_data:
        plot_data(collected_data)
    else:
        logger.error("No data available for processing or visualization.")

if __name__ == "__main__":
    main()
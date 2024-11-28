from data.data_fetcher import fetch_data
from data.data_processor import collect_data, plot_data
from config.settings import DATA_DIR

def main():
    # Step 1: Fetch data
    print("Step 1: Fetching data...")
    fetch_data()

    # Step 2: Process and analyze data
    print("Step 2: Processing data...")
    base_data_dir = f"{DATA_DIR}/us_stocks_sip/day_aggs_v1"
    collected_data = collect_data(base_data_dir)

    # Step 3: Generate plot
    if collected_data:
        print("Step 3: Generating plot...")
        plot_data(collected_data)
    else:
        print("No data to process or plot.")

if __name__ == "__main__":
    main()
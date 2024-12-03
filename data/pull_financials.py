import asyncio
import aiohttp
import pandas as pd
import logging
from pathlib import Path
from config.settings import DATA_DIR, POLYGON_API_KEY
from data.data_processor import load_and_combine_data  # Assuming tickers are derived from processed data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# File paths
FINANCIALS_FILE = Path(DATA_DIR) / "financials.feather"

# Polygon Financials API endpoint
POLYGON_FINANCIALS_ENDPOINT = "https://api.polygon.io/vX/reference/financials"

# Parameters
BATCH_LIMIT = 100  # Max results per API request
CONCURRENT_REQUESTS = 100  # Limit on concurrent API calls

async def fetch_financials(session, ticker, timeframe="quarterly", start_date="2009-01-01", end_date="2024-01-01"):
    """
    Asynchronously fetch financials for a given ticker from the Polygon Financials API.

    Parameters:
    - session: aiohttp.ClientSession, the active session for making HTTP requests
    - ticker: str, the stock ticker
    - timeframe: str, the timeframe ("quarterly" or "annual")
    - start_date: str, the start date in YYYY-MM-DD format
    - end_date: str, the end date in YYYY-MM-DD format

    Returns:
    - A list of financials for the given ticker
    """
    params = {
        "ticker": ticker,
        "period_of_report_date.gte": start_date,
        "period_of_report_date.lt": end_date,
        "timeframe": timeframe,
        "order": "asc",
        "limit": BATCH_LIMIT,
        "apiKey": POLYGON_API_KEY
    }

    url = POLYGON_FINANCIALS_ENDPOINT
    try:
        async with session.get(url, params=params) as response:
            if response.status != 200:
                logger.error(f"Failed to fetch {timeframe} financials for {ticker}: {await response.text()}")
                return []
            data = await response.json()
            return data.get("results", [])
    except Exception as e:
        logger.error(f"Error fetching {timeframe} financials for {ticker}: {e}")
        return []

async def process_ticker(session, ticker):
    """
    Fetch and combine financials for a single ticker.

    Parameters:
    - session: aiohttp.ClientSession
    - ticker: str, the stock ticker

    Returns:
    - A list of financials for the given ticker
    """
    quarterly_data = await fetch_financials(session, ticker, timeframe="quarterly")
    annual_data = await fetch_financials(session, ticker, timeframe="annual")
    return quarterly_data + annual_data

async def fetch_all_financials(tickers):
    """
    Fetch financials for all tickers asynchronously.

    Parameters:
    - tickers: list of stock tickers

    Returns:
    - A list of all financial data
    """
    all_financials = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, ticker in enumerate(tickers, 1):  # Add a counter to track progress
            tasks.append(process_ticker_with_limit(semaphore, session, ticker))

            # Log progress for every 100 tickers
            if idx % 100 == 0:
                logger.info(f"Queued {idx}/{len(tickers)} tickers for processing.")

        # Gather all results asynchronously
        for idx, result in enumerate(await asyncio.gather(*tasks), 1):
            if result:
                all_financials.extend(result)

            # Log progress for every 500 completed tickers
            if idx % 500 == 0:
                logger.info(f"Processed {idx}/{len(tickers)} tickers.")

    return all_financials

async def process_ticker_with_limit(semaphore, session, ticker):
    """
    Process a ticker with a concurrency limit.

    Parameters:
    - semaphore: asyncio.Semaphore to limit concurrent requests
    - session: aiohttp.ClientSession
    - ticker: str, the stock ticker

    Returns:
    - A list of financials for the given ticker
    """
    async with semaphore:
        return await process_ticker(session, ticker)

def load_tickers_from_data():
    """
    Load tickers from the processed data Feather file.

    Returns:
    - A set of unique tickers
    """
    processed_data_path = Path(DATA_DIR) / "processed_data.feather"
    
    if not processed_data_path.exists():
        logger.error(f"Processed data file not found at {processed_data_path}")
        return []

    try:
        processed_data = pd.read_feather(processed_data_path)
        tickers = processed_data["ticker"].unique()
        logger.info(f"Loaded {len(tickers)} unique tickers from processed data.")
        return tickers
    except Exception as e:
        logger.error(f"Error loading processed data: {e}")
        return []

def save_financials_data(financials):
    """
    Save processed financials data to Feather and CSV files.

    Parameters:
    - financials: list of financial data
    """
    logger.info("Processing financials into a DataFrame...")
    financials_df = pd.DataFrame(financials)
    
    # Save to Feather
    financials_df.to_feather(FINANCIALS_FILE)
    logger.info(f"Financials data saved to {FINANCIALS_FILE}")
    
    # # Save to CSV for inspection
    # csv_file = FINANCIALS_FILE.with_suffix(".csv")  # Change file extension to .csv
    # financials_df.to_csv(csv_file, index=False)
    # logger.info(f"Financials data also saved to {csv_file}")

def main():
    tickers = load_tickers_from_data()
    
    if tickers is None or len(tickers) == 0:
        logger.error("No tickers available for fetching financials.")
        return

    # Log the total number of tickers
    logger.info(f"Starting to fetch financials for {len(tickers)} tickers.")

    # Run the asynchronous event loop
    financials = asyncio.run(fetch_all_financials(tickers))
    
    if financials:
        logger.info(f"Fetched financials for {len(financials)} entries.")
        save_financials_data(financials)
    else:
        logger.warning("No financials data fetched.")
    
    logger.info("Financials fetching process completed.")

if __name__ == "__main__":
    main()
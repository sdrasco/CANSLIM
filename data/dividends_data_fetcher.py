# data/dividends_data_fetcher.py
import logging
import httpx
import pandas as pd
import asyncio
import time
import random
from urllib.parse import urlparse, parse_qs
from config.settings import POLYGON_API_KEY
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

MAX_RETRIES = 7
INITIAL_BACKOFF = 2.0  # seconds
MAX_CONCURRENT_REQUESTS = 20  # Limit concurrency to avoid rate limits too frequently
BATCH_SIZE = 100  # Process tickers in batches of 100 to reduce the total concurrency spike

async def fetch_dividends_for_ticker(session: httpx.AsyncClient, semaphore: asyncio.Semaphore, ticker: str, limit=100) -> pd.DataFrame:
    url = "https://api.polygon.io/v3/reference/dividends"
    params = {
        "ticker": ticker,
        "limit": limit,
        "apiKey": POLYGON_API_KEY
    }
    all_results = []
    page_count = 0

    async with semaphore:
        # By acquiring the semaphore here, we limit concurrency for each request
        while True:
            backoff = INITIAL_BACKOFF
            for attempt in range(MAX_RETRIES):
                # Optional small random delay to spread out requests
                await asyncio.sleep(random.uniform(0.1, 0.5))

                try:
                    resp = await session.get(url, params=params)
                    if resp.status_code == 429:
                        # Rate limit hit, backoff and retry
                        logger.warning(f"Rate limit hit for {ticker}, attempt {attempt+1}, sleeping {backoff}s.")
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    resp.raise_for_status()
                    json_data = resp.json()
                    results = json_data.get("results", [])
                    if results:
                        all_results.extend(results)

                    next_url = json_data.get("next_url")
                    if not next_url:
                        # No more pages
                        break

                    page_count += 1
                    logger.debug(f"Fetched page {page_count} for ticker {ticker}, got {len(results)} results on this page.")

                    parsed = urlparse(next_url)
                    next_params = parse_qs(parsed.query)
                    flat_params = {k: v[0] for k, v in next_params.items()}

                    if "apiKey" not in flat_params:
                        flat_params["apiKey"] = POLYGON_API_KEY
                    if "ticker" not in flat_params:
                        flat_params["ticker"] = ticker
                    if "limit" not in flat_params:
                        flat_params["limit"] = str(limit)

                    params = flat_params
                    break  # Successfully processed this page, break out of retry loop

                except httpx.HTTPStatusError as e:
                    logger.error(f"HTTP error fetching dividends for {ticker}: {e.response.status_code} {e.response.text}")
                    if e.response.status_code in [500, 502, 503, 504]:
                        # Server error, backoff and retry
                        logger.warning(f"Server error {e.response.status_code} for {ticker}, retrying in {backoff}s.")
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    else:
                        # Non-recoverable error, give up on this ticker
                        return pd.DataFrame()
                except Exception as e:
                    logger.error(f"Exception fetching dividends for {ticker}: {e}")
                    # Possibly network error, backoff and retry
                    time.sleep(backoff)
                    backoff *= 2
                    continue
            else:
                # If we finished the for-loop without a break (no success), all retries failed
                logger.error(f"All retries failed for {ticker} when fetching dividends.")
                return pd.DataFrame()

            if not next_url:
                # No more pages after successful fetch
                break

    if not all_results:
        return pd.DataFrame()

    return pd.DataFrame(all_results)

def fetch_dividends_data(tickers: list) -> pd.DataFrame:
    logger.info("Fetching dividends data for corporate actions.")

    async def gather_dividends(batch):
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Create a semaphore per batch
        async with httpx.AsyncClient(timeout=30.0) as session:
            tasks = [fetch_dividends_for_ticker(session, semaphore, t) for t in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        dfs = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Exception in fetching dividends: {r}")
                continue
            if isinstance(r, pd.DataFrame) and not r.empty:
                dfs.append(r)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    all_dfs = []
    # Process tickers in batches
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        batch_df = asyncio.run(gather_dividends(batch))
        if not batch_df.empty:
            all_dfs.append(batch_df)

    if all_dfs:
        dividends_data = pd.concat(all_dfs, ignore_index=True)
    else:
        dividends_data = pd.DataFrame()

    logger.info(f"Fetched {len(dividends_data)} dividend records in total.")
    return dividends_data
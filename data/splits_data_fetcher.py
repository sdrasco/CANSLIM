# data/splits_data_fetcher.py
import asyncio
import logging
import httpx
import pandas as pd
import time
from config.settings import POLYGON_API_KEY
from urllib.parse import urlparse, parse_qs
from utils.logging_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds

async def fetch_splits_for_ticker(session: httpx.AsyncClient, ticker: str, limit=50) -> pd.DataFrame:
    """
    Fetch splits data for a single ticker with pagination and retry logic.
    """
    url = "https://api.polygon.io/v3/reference/splits"
    params = {
        "ticker": ticker,
        "limit": limit,
        "apiKey": POLYGON_API_KEY
    }

    all_results = []
    page_count = 0

    while True:
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            try:
                resp = await session.get(url, params=params)
                if resp.status_code == 429:
                    # Rate limit exceeded, backoff and retry
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
                logger.debug(f"Fetched page {page_count} for ticker {ticker}, {len(results)} results.")

                # Parse next_url for next request
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
                break  # Break out of the retry loop if successful

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error fetching splits for {ticker}: {e.response.status_code} {e.response.text}")
                if e.response.status_code in [500, 502, 503, 504]:
                    # Temporary server error, backoff and retry
                    logger.warning(f"Server error {e.response.status_code} for {ticker}, retrying in {backoff}s.")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                else:
                    # Non-recoverable error, give up on this ticker
                    return pd.DataFrame()
            except Exception as e:
                logger.error(f"Exception fetching splits for {ticker}: {e}")
                # Possibly a network error, backoff and retry
                time.sleep(backoff)
                backoff *= 2
                continue
        else:
            # If we exit the for-loop without break, it means all retries failed
            logger.error(f"All retries failed for {ticker} when fetching splits.")
            return pd.DataFrame()

        if not next_url:
            # No more pages after successful fetch
            break

    if not all_results:
        return pd.DataFrame()

    return pd.DataFrame(all_results)

def fetch_splits_data(tickers: list) -> pd.DataFrame:
    """
    Fetch splits data for all given tickers.
    Includes error handling and will try to fetch what it can.
    """
    logger.info("Fetching splits data for corporate actions.")
    async def gather_splits():
        async with httpx.AsyncClient(timeout=30.0) as session:
            tasks = [fetch_splits_for_ticker(session, t) for t in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        dfs = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Exception during splits fetching: {r}")
                continue
            if isinstance(r, pd.DataFrame) and not r.empty:
                dfs.append(r)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    splits_data = asyncio.run(gather_splits())
    logger.info(f"Fetched {len(splits_data)} splits records in total.")
    return splits_data
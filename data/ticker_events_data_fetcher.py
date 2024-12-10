# data/ticker_events_data_fetcher.py
import asyncio
import logging
import httpx
import pandas as pd
import time
from urllib.parse import urlparse, parse_qs
from config.settings import POLYGON_API_KEY

from utils.logging_utils import configure_logging
# Configure logging
configure_logging()
logger = logging.getLogger(__name__)


MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds

async def fetch_events_for_ticker(session: httpx.AsyncClient, ticker: str, limit=50) -> pd.DataFrame:
    """
    Fetch ticker events for a single ticker asynchronously, with pagination and retry logic.
    """
    url = f"https://api.polygon.io/vX/reference/tickers/{ticker}/events"
    params = {
        "apiKey": POLYGON_API_KEY,
        "limit": limit
    }

    all_events = []
    page_count = 0

    while True:
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
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

                events = json_data.get("results", {}).get("events", [])
                if events:
                    all_events.extend(events)

                next_url = json_data.get("next_url")
                if not next_url:
                    # No more pages
                    break

                page_count += 1
                logger.debug(f"Fetched page {page_count} for {ticker}, got {len(events)} events on this page.")

                parsed = urlparse(next_url)
                next_params = parse_qs(parsed.query)
                flat_params = {k: v[0] for k, v in next_params.items()}

                if "apiKey" not in flat_params:
                    flat_params["apiKey"] = POLYGON_API_KEY
                if "limit" not in flat_params:
                    flat_params["limit"] = str(limit)

                params = flat_params
                break  # Successful page fetch, break from retry loop

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error fetching ticker events for {ticker}: {e.response.status_code} {e.response.text}")
                # Check if server error and retry if so
                if e.response.status_code in [500, 502, 503, 504]:
                    logger.warning(f"Server error {e.response.status_code} for {ticker}, retrying in {backoff}s.")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                else:
                    # Non-recoverable error
                    return pd.DataFrame()
            except Exception as e:
                logger.error(f"Exception fetching ticker events for {ticker}: {e}")
                # Network error, backoff and retry
                time.sleep(backoff)
                backoff *= 2
                continue
        else:
            # If we didn't break from the for-loop, all retries failed
            logger.error(f"All retries failed for {ticker} when fetching ticker events.")
            return pd.DataFrame()

        if not next_url:
            # No more pages
            break

    if not all_events:
        return pd.DataFrame()

    return pd.DataFrame(all_events)


def fetch_ticker_events(tickers: list) -> pd.DataFrame:
    """
    Fetch ticker events for all given tickers, with retries and backoff.
    """
    logger.info("Fetching ticker events for corporate actions.")

    async def gather_events():
        async with httpx.AsyncClient(timeout=30.0) as session:
            tasks = [fetch_events_for_ticker(session, t) for t in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        dfs = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Exception during ticker events fetching: {r}")
                continue
            if isinstance(r, pd.DataFrame) and not r.empty:
                dfs.append(r)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    events_data = asyncio.run(gather_events())
    logger.info(f"Fetched {len(events_data)} ticker event records in total.")
    return events_data
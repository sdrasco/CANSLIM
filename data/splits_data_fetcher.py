# data/splits_data_fetcher.py
import asyncio
import logging
import httpx
import pandas as pd
from config.settings import POLYGON_API_KEY

logger = logging.getLogger(__name__)

async def fetch_splits_for_ticker(session: httpx.AsyncClient, ticker: str, limit=1000) -> pd.DataFrame:
    url = "https://api.polygon.io/v3/reference/splits"
    params = {
        "ticker": ticker,
        "limit": limit,
        "apiKey": POLYGON_API_KEY
    }
    all_results = []
    while True:
        resp = await session.get(url, params=params)
        resp.raise_for_status()
        json_data = resp.json()
        results = json_data.get("results", [])
        all_results.extend(results)

        next_url = json_data.get("next_url")
        if not next_url:
            break

        # Parse next_url into params for next request if needed
        # Example: next_url might have query params to continue.
        # This is a stub: implement actual pagination logic
        params = {}  # parse next_url appropriately

    if not all_results:
        return pd.DataFrame()

    return pd.DataFrame(all_results)

def fetch_splits_data(tickers: list) -> pd.DataFrame:
    """
    Fetch splits data for all given tickers.
    """
    logger.info("Fetching splits data for corporate actions.")
    async def gather_splits():
        async with httpx.AsyncClient() as session:
            tasks = [fetch_splits_for_ticker(session, t) for t in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        # Combine all results into a single DataFrame
        dfs = [r for r in results if isinstance(r, pd.DataFrame) and not r.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    splits_data = asyncio.run(gather_splits())
    logger.info(f"Fetched {len(splits_data)} splits records in total.")
    return splits_data
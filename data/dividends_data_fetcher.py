# data/dividends_data_fetcher.py
import asyncio
import logging
import httpx
import pandas as pd
from config.settings import POLYGON_API_KEY

logger = logging.getLogger(__name__)

async def fetch_dividends_for_ticker(session: httpx.AsyncClient, ticker: str, limit=1000) -> pd.DataFrame:
    url = "https://api.polygon.io/v3/reference/dividends"
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

        # Parse and follow next_url for pagination if needed
        params = {} # parse next_url accordingly

    if not all_results:
        return pd.DataFrame()

    return pd.DataFrame(all_results)

def fetch_dividends_data(tickers: list) -> pd.DataFrame:
    logger.info("Fetching dividends data for corporate actions.")
    async def gather_dividends():
        async with httpx.AsyncClient() as session:
            tasks = [fetch_dividends_for_ticker(session, t) for t in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        dfs = [r for r in results if isinstance(r, pd.DataFrame) and not r.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    dividends_data = asyncio.run(gather_dividends())
    logger.info(f"Fetched {len(dividends_data)} dividend records in total.")
    return dividends_data
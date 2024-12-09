# data/ticker_events_data_fetcher.py
import asyncio
import logging
import httpx
import pandas as pd
from config.settings import POLYGON_API_KEY

logger = logging.getLogger(__name__)

async def fetch_events_for_ticker(session: httpx.AsyncClient, ticker: str, limit=50) -> pd.DataFrame:
    # Ticker events endpoint might differ from splits/dividends
    # Adjust the URL and params as documented.
    url = f"https://api.polygon.io/vX/reference/tickers/{ticker}/events"
    params = {
        "apiKey": POLYGON_API_KEY,
        # Add pagination params if supported
    }
    # For now, assume one-page results until we see a next_url or similar mechanism:
    resp = await session.get(url, params=params)
    resp.raise_for_status()
    json_data = resp.json()
    events = json_data.get("results", {}).get("events", [])
    if not events:
        return pd.DataFrame()
    return pd.DataFrame(events)

def fetch_ticker_events(tickers: list) -> pd.DataFrame:
    logger.info("Fetching ticker events for corporate actions.")
    async def gather_events():
        async with httpx.AsyncClient() as session:
            tasks = [fetch_events_for_ticker(session, t) for t in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        dfs = [r for r in results if isinstance(r, pd.DataFrame) and not r.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    events_data = asyncio.run(gather_events())
    logger.info(f"Fetched {len(events_data)} ticker event records in total.")
    return events_data
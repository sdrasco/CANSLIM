# config.settings

import logging
import os
from datetime import date, timedelta
from pathlib import Path
from utils.logging_utils import configure_logging  # Updated import

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Base directory for storing data
DATA_DIR = Path(__file__).parent.parent / "data"

# Base directory for storing reports
REPORT_DIR = Path(__file__).parent.parent / "html"

# Date range for downloading data
START_DATE = date(2003, 9, 1)  # earliest date in flat files
END_DATE = date(2024, 12, 6)
# END_DATE = date.today() - timedelta(days=1)  # or yesterday if you want more

# Number of tickers that we'll be allowed to buy
NUM_TICKERS = 20

# Credentials for Polygon.io
POLYGON_S3_KEY = os.getenv("POLYGONIO_ACCESS_KEY")  # Access Key
POLYGON_S3_SECRET = os.getenv("POLYGONIO_SECRET_KEY")  # Secret Key
POLYGON_API_KEY = os.getenv("POLYGONIO_API_KEY")  # API Key

# Validate credentials
if not POLYGON_S3_KEY or not POLYGON_S3_SECRET:
    logger.warning("Polygon S3 credentials are not fully set.")

# S3-specific settings
POLYGON_S3_ENDPOINT = "https://files.polygon.io"
POLYGON_BUCKET = "flatfiles"

# Proxies for market and money market performance
MARKET_PROXY = "SPY"  # ETF to use as the market proxy
MONEY_MARKET_PROXY = "SHY"  # ETF to use as the money market proxy

# Backtesting parameters
INITIAL_FUNDS = 100000  # starting capital
REBALANCE_FREQUENCY = "quarterly"  # can be "monthly", "quarterly", "annual", or "daily"
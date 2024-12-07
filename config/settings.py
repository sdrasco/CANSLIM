# config.settings

from config.configure_logging import configure_logging
import logging
import os
from datetime import date, timedelta
from pathlib import Path

# Configure logging
configure_logging()

# Create a logger for this module
logger = logging.getLogger(__name__)

# Base directory for storing data
DATA_DIR = Path(__file__).parent.parent / "data"

# Date range for downloading data
END_DATE = date.today() - timedelta(days=1)  # Set to yesterday
START_DATE = date(2003, 9, 1)  # Full range for production/backtesting

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

# Ticker for market index proxy
# Uncomment the desired index-tracking ETF and ensure only one is uncommented
INDEX_PROXY_TICKER = "SPY"  # Default
# INDEX_PROXY_TICKER = "IVV"  # iShares Core S&P 500 ETF

# Nasdaq-100 ETFs
# INDEX_PROXY_TICKER = "QQQ"  # Invesco QQQ Trust

# Dow Jones Industrial Average ETFs
# INDEX_PROXY_TICKER = "DIA"  # SPDR Dow Jones Industrial Average ETF Trust

# Total Market ETFs
# INDEX_PROXY_TICKER = "VTI"  # Vanguard Total Stock Market ETF

# Sector ETFs (example: technology-heavy sectors)
# INDEX_PROXY_TICKER = "XLK"  # Technology Select Sector SPDR Fund
# INDEX_PROXY_TICKER = "XLY"  # Consumer Discretionary Select Sector SPDR Fund

# International Market ETFs
# INDEX_PROXY_TICKER = "EFA"  # iShares MSCI EAFE ETF (Developed Markets ex-U.S.)
# INDEX_PROXY_TICKER = "VEA"  # Vanguard FTSE Developed Markets ETF
# INDEX_PROXY_TICKER = "EWJ"  # iShares MSCI Japan ETF

# Emerging Market ETFs
# INDEX_PROXY_TICKER = "EEM"  # iShares MSCI Emerging Markets ETF
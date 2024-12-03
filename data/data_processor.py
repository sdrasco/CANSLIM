# data_processor.py

import os
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from config.settings import DATA_DIR
from config.configure_logging import configure_logging
from config.settings import INDEX_PROXY_TICKER

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Base directory for Feather data files
base_data_dir = Path(DATA_DIR) / "us_stocks_sip/day_aggs_feather"

def load_file(file_path):
    """
    Load a single Feather file and add a 'date' column extracted from the filename.
    """
    try:
        logger.debug(f"Loading Feather file: {file_path}")
        df = pd.read_feather(file_path)
        
        # Extract date from filename
        date_str = file_path.stem  # e.g., '2024-10-01'
        file_date = pd.to_datetime(date_str)
        df['date'] = file_date

        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return pd.DataFrame() 

def load_and_combine_data(base_dir):
    """
    Traverse the directory structure, load Feather files, and combine into a single DataFrame.
    """
    data_frames = []
    for file_path in base_dir.rglob("*.feather"):
        df = load_file(file_path)
        if not df.empty:
            data_frames.append(df)
    if data_frames:
        combined_data = pd.concat(data_frames, ignore_index=True)
        logger.info(f"Combined data shape: {combined_data.shape}")
        return combined_data
    else:
        logger.warning("No data frames to combine.")
        return pd.DataFrame()

def validate_data(data):
    """
    Validate that essential columns exist in the data.
    """
    required_columns = {'date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'window_start', 'transactions'}
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    return True

def clean_data(data):
    """
    Clean the combined data.
    """
    if not validate_data(data):
        logger.error("Data validation failed. Exiting data cleaning step.")
        return pd.DataFrame()

    # Remove duplicates
    data = data.drop_duplicates()
    logger.debug("Removed duplicates.")

    # Handle missing values
    missing_before = data.isnull().sum()
    data = data.ffill().bfill()
    missing_after = data.isnull().sum()
    logger.debug(f"Missing values before cleaning: {missing_before}")
    logger.debug(f"Missing values after cleaning: {missing_after}")

    return data

def feature_engineering(data):
    logger.debug("Starting feature engineering.")

    # Ensure data is sorted by 'ticker' and 'date'
    data = data.sort_values(by=['ticker', 'date'])

    # Moving Averages
    data['50_MA'] = data.groupby('ticker')['close'].transform(lambda x: x.rolling(window=50, min_periods=1).mean())
    data['200_MA'] = data.groupby('ticker')['close'].transform(lambda x: x.rolling(window=200, min_periods=1).mean())

    # Fill missing values in moving averages with nearest available data
    data['50_MA'] = data.groupby('ticker')['50_MA'].transform(lambda x: x.ffill().bfill())
    data['200_MA'] = data.groupby('ticker')['200_MA'].transform(lambda x: x.ffill().bfill())

    # Volume Average
    data['50_Vol_Avg'] = data.groupby('ticker')['volume'].transform(lambda x: x.rolling(window=50, min_periods=1).mean())
    data['50_Vol_Avg'] = data.groupby('ticker')['50_Vol_Avg'].transform(lambda x: x.ffill().bfill())

    # Price Change Percentages
    data['Price_Change'] = data.groupby('ticker')['close'].pct_change()

    # Handle NaN for the first row of each ticker
    first_date_mask = data.groupby('ticker')['date'].transform('min') == data['date']
    data.loc[first_date_mask, 'Price_Change'] = 0

    # New 52-week High Indicator
    data['52_Week_High'] = data.groupby('ticker')['close'].transform(lambda x: x.rolling(window=252, min_periods=1).max())
    data['Is_New_High'] = data['close'] >= data['52_Week_High'].shift(1)

    # Volume Spike Indicator
    data['Volume_Spike'] = data['volume'] >= data['50_Vol_Avg'] * 1.5

    # Relative Strength using the market index proxy
    index_data = data[data['ticker'] == INDEX_PROXY_TICKER]
    if index_data.empty:
        logger.error(f"Index proxy data for '{INDEX_PROXY_TICKER}' is missing. Relative Strength cannot be calculated.")
        return data

    # Ensure index data contains necessary columns
    index_data = index_data[['date', 'close']].rename(columns={'close': 'INDEX_Close'})

    # Merge index data into the full dataset
    data = data.merge(index_data, on='date', how='left')

    # Calculate stock and index returns
    data['Stock_Returns'] = data.groupby('ticker')['close'].pct_change().fillna(0)
    data['INDEX_Returns'] = data['INDEX_Close'].pct_change().fillna(0)

    # Calculate cumulative returns
    data['Cumulative_Stock_Returns'] = np.exp(np.log1p(data['Stock_Returns']).groupby(data['ticker']).cumsum())
    data['Cumulative_INDEX_Returns'] = np.exp(np.log1p(data['INDEX_Returns']).cumsum())

    # Calculate Relative Strength
    data['Relative_Strength'] = data['Cumulative_Stock_Returns'] / data['Cumulative_INDEX_Returns']

    # Replace infinite values with NaN safely
    data.loc[:, 'Relative_Strength'] = data['Relative_Strength'].replace([np.inf, -np.inf], np.nan)

    # Fill NaN values safely
    data.loc[:, 'Relative_Strength'] = data['Relative_Strength'].ffill()

    # Market Direction Indicators
    data['INDEX_50_MA'] = data['INDEX_Close'].rolling(window=50, min_periods=1).mean()
    data['INDEX_200_MA'] = data['INDEX_Close'].rolling(window=200, min_periods=1).mean()

    logger.debug("Feature engineering completed.")
    return data

def structure_data(data):
    """
    Set index and sort data for efficient access.
    """
    # Convert 'date' to datetime if not already
    data['date'] = pd.to_datetime(data['date'])

    # Set multi-index with 'date' and 'ticker'
    data.set_index(['date', 'ticker'], inplace=True)
    data.sort_index(inplace=True)
    logger.debug("Data structuring completed.")
    return data

def save_processed_data(data, output_path):
    """
    Save the processed data to a Feather file.
    """
    # Reset index to save 'date' and 'ticker' as columns
    data_to_save = data.reset_index()
    data_to_save.to_feather(output_path)
    logger.info(f"Processed data saved to {output_path}.")

def process_data(base_dir, output_path=None):
    # Load and combine data
    combined_data = load_and_combine_data(base_dir)
    
    if not combined_data.empty:
        logger.info("Data loaded and combined successfully.")
        
        # Clean data
        cleaned_data = clean_data(combined_data)
        if not cleaned_data.empty:
            logger.info("Data cleaned successfully.")
    
            # Adjust for corporate actions (if applicable)
            adjusted_data = adjust_for_corporate_actions(cleaned_data)
            logger.info("Data adjusted for corporate actions.")

            # Feature engineering
            featured_data = feature_engineering(adjusted_data)
            logger.info("Feature engineering completed.")

            # Data structuring
            structured_data = structure_data(featured_data)
            logger.info("Data structuring completed.")

            # Save processed data
            if output_path is not None:
                save_processed_data(structured_data, output_path)
                logger.info("Processed data saved successfully.")
            
            # Return the processed data
            return structured_data
        else:
            logger.error("Data cleaning resulted in an empty DataFrame.")
            return None
    else:
        logger.error("Data loading resulted in an empty DataFrame.")
        return None

def adjust_for_corporate_actions(data):
    """
    Adjust data for corporate actions like stock splits and dividends.
    """
    # Placeholder for adjustment logic
    #  Need to fetch corporate actions data and adjust 'open', 'high', 'low', 'close', 'volume' accordingly.
    logger.debug("Adjusting data for corporate actions is not implemented yet.")
    return data
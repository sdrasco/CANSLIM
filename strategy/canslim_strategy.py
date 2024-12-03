# canslim_strategy.py

import logging
from config.configure_logging import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

class CANSLIMStrategy:
	def __init__(self, stock_data, market_data, thresholds=None):
		"""
		Initializes the CANSLIM strategy.

		Parameters:
		- stock_data: DataFrame or dict containing stock-specific data
		- market_data: DataFrame containing market-wide data
		- thresholds: dict specifying thresholds for CANSLIM criteria
		"""
		self.stock_data = stock_data
		self.market_data = market_data
		self.thresholds = thresholds or {
			'current_earnings_growth': 0.25,
			'annual_earnings_growth': 0.20,
		}

	# [C]
def check_current_earnings(self, stock):
    """
    Checks if the stock's most recent quarterly EPS increased significantly compared 
    to the same quarter last year.
    """
    try:
        # Filter financials for the given stock
        financials = self.financial_data[self.financial_data["Ticker"] == stock]

        # Ensure we have quarterly data
        financials_quarterly = financials[financials["Timeframe"] == "quarterly"]

        # Get the most recent quarter and the same quarter last year
        recent_quarter = financials_quarterly.iloc[-1]
        previous_year_quarter = financials_quarterly[
            (financials_quarterly["Fiscal Year"] == recent_quarter["Fiscal Year"] - 1) &
            (financials_quarterly["Fiscal Period"] == recent_quarter["Fiscal Period"])
        ]

        if previous_year_quarter.empty:
            logger.warning(f"Missing previous year quarter for stock: {stock}")
            return False

        # Calculate earnings growth
        recent_eps = recent_quarter["Diluted EPS"]
        previous_eps = previous_year_quarter.iloc[0]["Diluted EPS"]

        growth_rate = (recent_eps - previous_eps) / abs(previous_eps)
        return growth_rate >= self.thresholds['current_earnings_growth']
    except Exception as e:
        logger.error(f"Error in current earnings check for stock {stock}: {e}")
        return False

# [A]
def check_annual_earnings(self, stock):
    """
    Evaluates annual earnings growth by comparing the most recent fiscal year EPS 
    to the EPS of the previous year.
    """
    try:
        # Filter financials for the given stock
        financials = self.financial_data[self.financial_data["Ticker"] == stock]

        # Ensure we have annual data
        financials_annual = financials[financials["Timeframe"] == "annual"]

        # Get the most recent fiscal year and the previous fiscal year
        recent_year = financials_annual.iloc[-1]
        previous_year = financials_annual[
            financials_annual["Fiscal Year"] == recent_year["Fiscal Year"] - 1
        ]

        if previous_year.empty:
            logger.warning(f"Missing previous fiscal year for stock: {stock}")
            return False

        # Calculate annual earnings growth
        recent_eps = recent_year["Diluted EPS"]
        previous_eps = previous_year.iloc[0]["Diluted EPS"]

        growth_rate = (recent_eps - previous_eps) / abs(previous_eps)
        return growth_rate >= self.thresholds['annual_earnings_growth']
    except Exception as e:
        logger.error(f"Error in annual earnings check for stock {stock}: {e}")
        return False

	# [N]
	def check_new_products(self, stock):
		"""
		Checks if the stock has reached a new 52-week high.
		"""
		try:
		    stock_data = self.stock_data[stock]
		    is_new_high = stock_data['Is_New_High'].iloc[-1]  # Boolean indicating new 52-week high
		    
		    if is_new_high:
		        return True
		    else:
		        return False
		except (IndexError, KeyError):
		    logger.error(f"Missing price high data for stock: {stock}")
		    return False

	# [S]
	def check_supply_demand(self, stock):
		"""
		Evaluates supply and demand based on volume indicators.
		"""
		try:
		    stock_data = self.stock_data[stock]
		    recent_volume = stock_data['volume'].iloc[-1]
		    recent_50_vol_avg = stock_data['50_Vol_Avg'].iloc[-1]

		    return recent_volume >= recent_50_vol_avg * 1.5
		except (IndexError, KeyError):
		    logger.error(f"Missing volume data for stock: {stock}")
		    return False

	# [L]
	def check_leader_laggard(self, stock):
		"""
		Determines if the stock is a leader based on its relative strength.
		"""
		try:
		    stock_data = self.stock_data[stock]
		    recent_relative_strength = stock_data['Relative_Strength'].iloc[-1]

		    return recent_relative_strength > 1.0  # Outperforming the market
		except (IndexError, KeyError):
		    logger.error(f"Missing relative strength data for stock: {stock}")
		    return False

	# [I]
	def check_institutional_sponsorship(self, stock):
		"""
		Infers institutional sponsorship from volume patterns.
		"""
		try:
		    stock_data = self.stock_data[stock]
		    
		    # Look for unusually high volume on up days
		    recent_close = stock_data['close'].iloc[-1]
		    recent_open = stock_data['open'].iloc[-1]
		    recent_volume = stock_data['volume'].iloc[-1]
		    recent_50_vol_avg = stock_data['50_Vol_Avg'].iloc[-1]

		    # Condition: Up day with volume > 1.5x 50-day average volume
		    if recent_close > recent_open and recent_volume > recent_50_vol_avg * 1.5:
		        return True  # Indicates potential institutional sponsorship
		    else:
		        return False
		except (IndexError, KeyError):
		    logger.error(f"Missing data for stock: {stock}")
		    return False

	# [M]
	def check_market_direction(self):
		"""
		Checks the market direction using moving averages or other indicators.
		"""
		try:
		    recent_close = self.market_data['INDEX_Close'].iloc[-1]
		    recent_50_MA = self.market_data['INDEX_50_MA'].iloc[-1]
		    recent_200_MA = self.market_data['INDEX_200_MA'].iloc[-1]

		    return recent_close > recent_50_MA > recent_200_MA
		except (IndexError, KeyError):
		    logger.error("Market data is incomplete.")
		    return False

	def evaluate_stock(self, stock):
		"""
		Aggregates all CANSLIM criteria to evaluate a stock.

		Returns:
		- 'BUY' if all criteria are met
		- 'HOLD' otherwise
		"""
		c = self.check_current_earnings(stock)
		a = self.check_annual_earnings(stock)
		n = self.check_new_products(stock)
		s = self.check_supply_demand(stock)
		l = self.check_leader_laggard(stock)
		i = self.check_institutional_sponsorship(stock)
		m = self.check_market_direction()

		if all([c, a, n, s, l, i, m]):
		    return 'BUY'
		elif not any([c, a, n, s, l, i, m]):
		    return 'SELL'
		else:
		    return 'HOLD'
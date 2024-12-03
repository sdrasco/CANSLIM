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
		Placeholder: Checks if the stock's most recent quarterly EPS increased significantly compared 
		to the same quarter last year. Currently not implemented due to lack of data.
		"""
		logger.warning(f"Current earnings check is not implemented for stock: {stock}.")
		return False

	# [A]
	def check_annual_earnings(self, stock):
		"""
		Placeholder: Evaluates annual earnings growth.
		"""
		logger.warning("Annual earnings check not implemented.")
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
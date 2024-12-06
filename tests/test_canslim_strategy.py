# test_canslim_strategy.py

import unittest
import pandas as pd
from strategy.canslim_strategy import CANSLIMStrategy

class TestCANSLIMStrategy(unittest.TestCase):

    def setUp(self):
        """
        Setup mock data for testing the CANSLIM strategy.
        """
        # Mock financial data
        self.financial_data = pd.DataFrame([
            {"Ticker": "AAPL", "Fiscal Year": 2023, "Fiscal Period": "FY", "Timeframe": "annual", "Diluted EPS": 5.5},
            {"Ticker": "AAPL", "Fiscal Year": 2022, "Fiscal Period": "FY", "Timeframe": "annual", "Diluted EPS": 4.5},
        ])

        # Mock stock data
        self.stock_data = {
            "AAPL": pd.DataFrame({
                "date": pd.date_range(start="2023-01-01", periods=10),
                "Is_New_High": [False, False, True] + [False] * 7,
                "volume": [100] * 10,
                "50_Vol_Avg": [90] * 10,
                "Relative_Strength": [1.2] * 10,
                "close": [150, 152, 155, 153, 154, 155, 156, 157, 158, 160],
                "open": [148, 151, 153, 151, 153, 154, 154, 156, 157, 159],
            })
        }

        # Mock market data
        self.market_data = pd.DataFrame({
            "date": pd.date_range(start="2023-01-01", periods=10),
            "INDEX_Close": [1000, 1010, 1020, 1030, 1040, 1050, 1060, 1070, 1080, 1090],
            "INDEX_50_MA": [950] * 10,
            "INDEX_200_MA": [900] * 10,
        })

        # Instantiate CANSLIMStrategy
        self.strategy = CANSLIMStrategy(
            stock_data=self.stock_data,
            market_data=self.market_data,
            financial_data=self.financial_data,
        )

    def test_check_current_earnings(self):
        """
        Test the check_current_earnings method.
        """
        result = self.strategy.check_current_earnings("AAPL")
        self.assertTrue(result, "Current earnings check should return True for AAPL.")

        # Test case: No previous year's data
        self.strategy.financial_data = self.financial_data[
            ~((self.financial_data["Fiscal Year"] == 2022) &
              (self.financial_data["Fiscal Period"] == "Q1"))
        ]
        result = self.strategy.check_current_earnings("AAPL")
        self.assertFalse(result, "Current earnings check should return False when previous year's data is missing.")

    def test_check_annual_earnings(self):
        """
        Test the check_annual_earnings method.
        """
        result = self.strategy.check_annual_earnings("AAPL")
        self.assertTrue(result, "Annual earnings check should return True for AAPL.")

        # Test case: Missing data
        self.strategy.financial_data = pd.DataFrame([])  # Empty financials
        result = self.strategy.check_annual_earnings("AAPL")
        self.assertFalse(result, "Annual earnings check should return False when data is missing.")

    def test_check_new_products(self):
        """
        Test the check_new_products method.
        """
        result = self.strategy.check_new_products("AAPL")
        self.assertTrue(result, "New products check should return True for AAPL (52-week high).")

    def test_check_supply_demand(self):
        """
        Test the check_supply_demand method.
        """
        result = self.strategy.check_supply_demand("AAPL")
        self.assertTrue(result, "Supply and demand check should return True for AAPL (volume spike).")

    def test_check_leader_laggard(self):
        """
        Test the check_leader_laggard method.
        """
        result = self.strategy.check_leader_laggard("AAPL")
        self.assertTrue(result, "Leader/laggard check should return True for AAPL (relative strength > 1).")

    def test_check_institutional_sponsorship(self):
        """
        Test the check_institutional_sponsorship method.
        """
        result = self.strategy.check_institutional_sponsorship("AAPL")
        self.assertTrue(result, "Institutional sponsorship check should return True for AAPL (volume on up days).")

    def test_check_market_direction(self):
        """
        Test the check_market_direction method.
        """
        result = self.strategy.check_market_direction()
        self.assertTrue(result, "Market direction check should return True (bullish conditions).")

    def test_evaluate_stock(self):
        """
        Test the evaluate_stock method.
        """
        result = self.strategy.evaluate_stock("AAPL")
        self.assertEqual(result, "BUY", "AAPL should pass all criteria and return 'BUY'.")

        # Test case: Partial failure
        self.strategy.financial_data = pd.DataFrame([])  # No financial data
        result = self.strategy.evaluate_stock("AAPL")
        self.assertEqual(result, "HOLD", "AAPL should return 'HOLD' when some criteria fail.")

        # Test case: Missing stock data
        self.strategy.stock_data = {}  # Remove stock data
        result = self.strategy.evaluate_stock("AAPL")
        self.assertEqual(result, "SELL", "AAPL should return 'SELL' when stock data is missing.")

if __name__ == "__main__":
    unittest.main()
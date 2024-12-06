import unittest
from pathlib import Path
import pandas as pd
from data.data_processor import DataProcessor
from config.settings import DATA_DIR

class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment, including directories and paths.
        """
        self.base_dir = Path(DATA_DIR) / "us_stocks_sip/day_aggs_feather"
        self.output_path = Path(DATA_DIR) / "test_processed_data.feather"

    def test_process_data(self):
        """
        Test the full data processing pipeline.
        """
        # Instantiate the DataProcessor class
        processor = DataProcessor(base_dir=self.base_dir, output_path=self.output_path)
        
        # Run the process method
        processor.process()

        # Assert the processed data is not empty
        self.assertFalse(processor.data.empty, "Processed data should not be empty.")

        # Validate critical columns in the processed data
        expected_columns = [
            "open", "high", "low", "close", "volume", "50_MA", "200_MA", "Price_Change"
        ]
        for column in expected_columns:
            self.assertIn(column, processor.data.columns, f"Missing expected column: {column}")

        # Assert the output file was created
        self.assertTrue(self.output_path.exists(), "Processed data file was not saved.")

        # Load the saved file to validate its structure
        saved_data = pd.read_feather(self.output_path)
        self.assertFalse(saved_data.empty, "Saved data file should not be empty.")

    def tearDown(self):
        """
        Clean up test artifacts.
        """
        # if self.output_path.exists():
        #     self.output_path.unlink()  # Delete the test file

if __name__ == "__main__":
    unittest.main()
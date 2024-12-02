import unittest
from pathlib import Path
import pandas as pd
from data.data_processor import process_data, base_data_dir
from config.settings import DATA_DIR

class TestDataProcessor(unittest.TestCase):
    def test_process_data(self):
        # Define output path for processed data
        processed_data_path = Path(DATA_DIR) / "test_processed_data.feather"

        # Run the data processing function
        processed_data = process_data(base_data_dir, output_path=processed_data_path)

        # Assert the result is not None and not empty
        self.assertIsNotNone(processed_data, "Processed data should not be None.")
        self.assertFalse(processed_data.empty, "Processed data should not be empty.")

        # Assert the file was saved
        self.assertTrue(processed_data_path.exists(), "Processed data file was not saved.")

        # Time and validate the reading of the saved file
        df = pd.read_feather(processed_data_path)
        self.assertFalse(df.empty, "Loaded DataFrame should not be empty.")
        self.assertIn("Price_Change", df.columns, "'Price_Change' column is missing.")

        # Cleanup
        processed_data_path.unlink()  # Delete the test file after running

if __name__ == "__main__":
    unittest.main()
# CANSLIM Trading Strategy Project

This is a Python-based system for both backtesting and live trading using the CANSLIM methodology as described by William J. O'Neil in *How to Make Money in Stocks*. 

> *Disclaimer: O'Neil and his book aren't my favorite, but implementing and testing his strategy is a straightforward and fun project.*

## Current Progress and Directory Structure

### **Code Structure**
The project currently consists of the following modules:
- **`data_fetcher.py`:** Automates downloading daily aggregate stock data (`day_aggs_v1`) from Polygon.io's S3-compatible flat files. It ensures files are only downloaded if they don't already exist locally.
- **`data_processor.py`:** A script for loading, exploring, and preprocessing the downloaded data. It generates basic visualizations, such as average volume over time.
- **`config/settings.py`:** Stores configuration parameters such as API keys, file paths, and date ranges.

### **Downloaded Data**
The `day_aggs_v1` dataset has been downloaded from September 1, 2003, to November 27, 2024. The data is organized in a directory structure that mirrors Polygon.io's flat file hierarchy:

```
data/
└── us_stocks_sip/
    └── day_aggs_v1/
        ├── 2003/
        │   └── 09/
        │       ├── 2003-09-10.csv.gz
        │       ├── 2003-09-11.csv.gz
        │       └── ...
        ├── 2004/
        │   └── ...
        └── 2024/
            ├── 10/
            │   ├── 2024-10-01.csv.gz
            │   ├── 2024-10-02.csv.gz
            │   └── ...
            └── 11/
                ├── 2024-11-01.csv.gz
                └── ...
```

### **Data Fetching Logic**
- Files are downloaded only if they do not already exist locally.
- The script uses a directory structure identical to Polygon.io's flat file hierarchy to store the data.
- Data is downloaded incrementally, starting from September 2003 through November 2024.

## Using the Virtual Environment (`venv`)

To keep dependencies isolated and ensure a clean development setup, this project uses a Python virtual environment. Follow these steps to create and use the `venv`:

### 1. Create the Virtual Environment
Run the following command in the project directory:
```bash
python -m venv venv
```
This creates a directory named `venv` containing the virtual environment.

### 2. Activate the Virtual Environment
- On macOS/Linux:
  ```bash
  source venv/bin/activate
  ```
- On Windows:
  ```bash
  venv\Scripts\activate
  ```

When activated, your terminal prompt will change to show `(venv)`, indicating that the virtual environment is active.

### 3. Install Dependencies
With the virtual environment active, install the required packages:
```bash
pip install -r requirements.txt
```

### 4. Regenerate Dependencies (Optional)
If you add or update any packages, update the `requirements.txt` file:
```bash
pip freeze > requirements.txt
```

### 5. Deactivate the Virtual Environment
When you're done, deactivate the virtual environment by running:
```bash
deactivate
```

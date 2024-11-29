
# CANSLIM Trading Strategy Project

This is a Python-based system for both backtesting and live trading using the CANSLIM methodology as described by William J. O'Neil in *How to Make Money in Stocks*. 

> *Disclaimer: O'Neil and his book aren't my favorite, but implementing and testing his strategy is a straightforward and fun project.*

## Current Progress and Directory Structure

### **Code Structure**
The project is just getting started.  It currently consists of the following parts:
- **`data/data_fetcher.py`:** Automates downloading daily aggregate stock data from Polygon.io's S3-compatible flat files. Files are downloaded and stored locally in Feather format for faster processing.
- **`data/data_processor.py`:** A script for loading, exploring, and preprocessing the Feather-formatted data. It generates basic visualizations, such as average volume over time.
- **`config/settings.py`:** Stores configuration parameters such as API keys, file paths, and date ranges.
- **`main.py`:** An execution script that runs the others.

### **Downloaded Data**
The `day_aggs_v1` dataset running from September 1, 2003, to the present has been pulled and stored locally. It isn't kept here on GitHub due to licensing (and size), but any Polygon API tier can access it. The data is organized in the following directory structure:

```
data/
└── us_stocks_sip/
    └── day_aggs_feather/
        ├── 2003/
        │   └── 09/
        │       ├── 2003-09-10.feather
        │       ├── 2003-09-11.feather
        │       └── ...
        ├── 2004/
        │   └── ...
        └── 2024/
            ├── 10/
            │   ├── 2024-10-01.feather
            │   ├── 2024-10-02.feather
            │   └── ...
            └── 11/
                ├── 2024-11-01.feather
                └── ...
```

### **Data Fetching Logic**
- Files are downloaded only if they don't already exist locally in Feather format.
- The directory structure mimics Polygon.io's flat file hierarchy, but files are stored in Feather format for improved speed and efficiency.
- Data is downloaded incrementally, starting from the most recent local file.

## Using the Virtual Environment (`venv`)

To keep dependencies isolated and ensure a clean shareable development setup, this project uses a Python virtual environment. Follow these steps to create and use the `venv`:

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

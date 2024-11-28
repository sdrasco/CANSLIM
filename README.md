# CANSLIM Trading Strategy Project

This is a Python-based system for both backtesting and live trading using the CANSLIM methodology as described by William J. O'Neil in *How to Make Money in Stocks*. 

> *Disclaimer: O'Neil and his book aren't my favorite. I'm not a fan of his snake-oil-selling tone, his "cups and saucers in the charts" fantasies, or his patriotic propaganda. Still, implementing and testing his CANSLIM strategy is a straightforward and sound project.*

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

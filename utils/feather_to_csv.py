# utils/feather_to_csv.py

import pandas as pd
from pathlib import Path

# ===== EDIT THIS PATH AS NEEDED =====
FEATHER_FILE_PATH = "../data/sp_500_historic_snapshot.feather"
# ====================================

def feather_to_csv(feather_path):
    feather_path = Path(feather_path)
    if not feather_path.exists():
        print(f"Feather file not found: {feather_path}")
        return

    # Load DataFrame from Feather
    df = pd.read_feather(feather_path)

    # Construct CSV file path by replacing .feather with .csv
    csv_path = feather_path.with_suffix(".csv")

    # Write DataFrame to CSV
    df.to_csv(csv_path, index=False)
    print(f"Converted {feather_path} to {csv_path}")

if __name__ == "__main__":
    feather_to_csv(FEATHER_FILE_PATH)
import os
import pandas as pd
from pathlib import Path

def analyzing_csv():
    """
    Analyze CSV files in data/raw/ and return common columns.
    """
    # Automatically resolve base path
    BASE_DIR = Path(__file__).resolve().parents[1]
    CSV_DIR = BASE_DIR / "data/raw"

    # List CSV files
    csv_files = list(CSV_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found in:", CSV_DIR)
        return []

    column_sets = []

    for file in csv_files:
        try:
            df = pd.read_csv(file, nrows=0)  # read only headers
            column_sets.append(set(df.columns))
            print(f"Loaded columns from: {file.name}")
        except Exception as e:
            print(f"Error reading {file.name}: {e}")

    if not column_sets:
        print("No valid column sets found.")
        return []

    common_cols = set.intersection(*column_sets)
    print("\nCommon Columns Across All CSV Files:\n")
    for col in sorted(common_cols):
        print(f"- {col}")

    return list(common_cols)
# now i want to print few columns on screen by using this function
if __name__ == "__main__":
    common_columns = analyzing_csv()
    if common_columns:
        print("\nCommon columns found:")
        for col in common_columns[:5]:  # Print first 5 common columns
            print(col)
    else:
        print("No common columns found.")
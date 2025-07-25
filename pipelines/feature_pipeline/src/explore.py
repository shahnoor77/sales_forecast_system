'''import os
import pandas as pd

RAW_DATA_DIR = os.path.join(".", "data", "raw")

print(f"{'Table':<40} | Columns")
print("-" * 80)

for file in os.listdir(RAW_DATA_DIR):
    if file.endswith(".csv"):
        path = os.path.join(RAW_DATA_DIR, file)
        try:
            df = pd.read_csv(path, nrows=1)
            print(f"{file:<40} | {list(df.columns)}")
        except Exception as e:
            print(f"{file:<40} | ERROR: {e}")'''

###exploring data 
import os
import pandas as pd
from pathlib import Path

# Set path to your directory
DATA_DIR = Path("./data/raw")

def preview_all_tables(data_dir, n_rows=3):
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print("No CSV files found in the directory.")
        return

    for file in csv_files:
        print("="*80)
        print(f" File: {file.name}")
        try:
            df = pd.read_csv(file, nrows=n_rows)
            print("Columns:", list(df.columns))
            print(df.head(n_rows))
        except Exception as e:
            print(f"Failed to read {file.name}: {e}")
        print("\n") 
    # Print summary of all files
    print("="*80)
    print("Summary of all CSV files:")
    for file in csv_files:
        try:
            df = pd.read_csv(file, nrows=0)  # Read only headers
            print(f"File: {file.name} | Columns: {list(df.columns)}")
        except Exception as e:
            print(f"Failed to read {file.name}: {e}")
    # print total number of tables explored
    print(f"\nTotal tables explored: {len(csv_files)}")
    # print total number of columns across all tables with their names
    all_columns = set()
    for file in csv_files:
        try:
            df = pd.read_csv(file, nrows=0)  # Read only headers
            all_columns.update(df.columns)
        except Exception as e:
            print(f"Failed to read {file.name}: {e}")
    print(f"Total unique columns across all tables: {len(all_columns)}")
    print("Unique columns:", list(all_columns))



if __name__ == "__main__":
    preview_all_tables(DATA_DIR)

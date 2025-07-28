import os
import pandas as pd

RAW_DATA_PATH = "./data/raw"

def list_csv_files(path):
    return [f for f in os.listdir(path) if f.endswith(".csv")]

def extract_schema(file_path):
    try:
        df = pd.read_csv(file_path, nrows=5)  # Read just a few rows to infer columns
        return list(df.columns)
    except Exception as e:
        print(f"[ERROR] Failed to read {file_path}: {e}")
        return []

def generate_schema_summary():
    summary = []
    csv_files = list_csv_files(RAW_DATA_PATH)

    for file in csv_files:
        file_path = os.path.join(RAW_DATA_PATH, file)
        columns = extract_schema(file_path)
        summary.append({
            "file": file,
            "columns": columns
        })

    return summary

if __name__ == "__main__":
    schema = generate_schema_summary()
    
    # Pretty print result
    for entry in schema:
        print(f"\n {entry['file']}")
        print("Columns:", entry['columns'])

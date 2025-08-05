import pandas as pd

def summarize(df: pd.DataFrame, label: str):
    print(f"\n--- {label} ---")
    print(f"Rows: {len(df)}")
    print("Columns:", df.columns.tolist())
    print(df.describe(include='all'))
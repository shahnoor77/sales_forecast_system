import pandas as pd

df = pd.read_csv("data/transformed/final_standardized.csv")
print(" Columns:", df.columns.tolist())
#print few rows
print(" Sample data:\n", df.head())

#print summary
print(" Summary:\n", df.describe(include='all'))

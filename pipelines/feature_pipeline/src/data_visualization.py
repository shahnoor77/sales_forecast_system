import pandas as pd
import matplotlib.pyplot as plt

def plot_sales_trend(df: pd.DataFrame, out_path="data/transformed/sales_trend.png"):
    df['created_at'] = pd.to_datetime(df['created_at'])
    sales_by_day = df.groupby(df['created_at'].dt.date)['sub_total'].sum()
    sales_by_day.plot(kind='line', title='Daily Sales Trend', figsize=(10, 5))
    plt.xlabel("Date")
    plt.ylabel("Total Sales")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
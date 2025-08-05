import pandas as pd
import os
import logging

def transform_data(input_file: str, output_file: str) -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO)

    df = pd.read_csv(input_file)

    rename_map = {
        "units_sold": "quantity",
        "unit_price": "sales_price",
        "amount": "sub_total",
        "naame": "seller_name",
        "name_mp": "marketplace_name"
    }

    df.rename(columns=rename_map, inplace=True)

    expected_columns = [
        "order_id", "created_at", "sub_total", "sales_price", "quantity",
        "product_id", "product_name", "brand", "category",
        "sub_category", "sub_sub_category", "seller_name", "marketplace_name"
    ]

    final_df = df[[col for col in expected_columns if col in df.columns]]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    final_df.to_csv(output_file, index=False)
    return final_df

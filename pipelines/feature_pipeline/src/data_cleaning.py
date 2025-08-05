import pandas as pd
import os
import logging
from pathlib import Path

def merge_all_csvs(input_dir: str, output_file: str) -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO)
    try:
        orders = pd.read_csv(Path(input_dir) / "orders.csv")
        order_items = pd.read_csv(Path(input_dir) / "order_items.csv")
        product_variants = pd.read_csv(Path(input_dir) / "product_variants.csv")
        product_material_codes = pd.read_csv(Path(input_dir) / "product_material_codes.csv")
        seller_marketplaces = pd.read_csv(Path(input_dir) / "seller_marketplaces.csv")
        marketplaces = pd.read_csv(Path(input_dir) / "marketplaces.csv")
        products = pd.read_csv(Path(input_dir) / "products.csv")

        df = order_items.merge(orders, left_on="order_id", right_on="id")
        df = df.merge(product_variants, left_on="pv_id", right_on="id", how="left")
        df = df.merge(product_material_codes, left_on="pm_id", right_on="id", how="left")
        df = df.merge(seller_marketplaces, left_on="smp_id", right_on="id", how="left")
        df = df.merge(marketplaces, left_on="mp_id", right_on="id", how="left")
        df = df.merge(products, left_on="p_id", right_on="id", how="left")

        df.to_csv(output_file, index=False)
        return df

    except Exception as e:
        logging.error(f"Merge failed: {e}")
        raise
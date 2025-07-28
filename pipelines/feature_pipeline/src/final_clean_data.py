import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# ======================= Setup ========================
# Setup directories
data_dir = Path("./data/raw")
output_dir = Path("./data/transformed")
output_dir.mkdir(parents=True, exist_ok=True)

# Setup logging
log_path = output_dir / "data_pipeline.log"
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("🚀 Starting final_data pipeline...")

# ======================= Load CSVs ========================
try:
    orders = pd.read_csv(data_dir / "orders.csv", low_memory=False)
    order_items = pd.read_csv(data_dir / "order_items.csv", low_memory=False)
    product_variants = pd.read_csv(data_dir / "product_variants.csv", low_memory=False)
    product_material_codes = pd.read_csv(data_dir / "product_material_codes.csv", low_memory=False)
    seller_marketplaces = pd.read_csv(data_dir / "seller_marketplaces.csv", low_memory=False)
    marketplaces = pd.read_csv(data_dir / "marketplaces.csv", low_memory=False)
    products = pd.read_csv(data_dir / "products.csv", low_memory=False)
    logging.info("✅ All CSVs loaded successfully")
except Exception as e:
    logging.exception("❌ Failed to load CSVs")
    raise e

# ======================= Initial Checks ========================
# Check required columns in each CSV
required_orders_cols = ["id", "created_at", "seller_id", "smp_id", "status"]
required_order_items_cols = ["order_id", "sub_total", "quantity", "sales_price"]

missing_orders = [col for col in required_orders_cols if col not in orders.columns]
missing_order_items = [col for col in required_order_items_cols if col not in order_items.columns]

if missing_orders:
    logging.error(f"❌ Missing columns in orders.csv: {missing_orders}")
    raise KeyError(f"Missing columns in orders.csv: {missing_orders}")
if missing_order_items:
    logging.error(f"❌ Missing columns in order_items.csv: {missing_order_items}")
    raise KeyError(f"Missing columns in order_items.csv: {missing_order_items}")

# ======================= Merge Pipeline ========================
try:
    df = order_items.merge(orders, left_on="order_id", right_on="id", suffixes=("", "_order"))
    logging.info(f"✅ Join 1: order_items + orders | Rows: {len(df)}")
    df.to_csv(output_dir / "checkpoint_join1.csv", index=False)

    df = df.merge(product_variants, left_on="pv_id", right_on="id", how="left", suffixes=("", "_pv"))
    logging.info(f"✅ Join 2: + product_variants | Rows: {len(df)}")

    df = df.merge(product_material_codes, left_on="pm_id", right_on="id", how="left", suffixes=("", "_pm"))
    logging.info(f"✅ Join 3: + product_material_codes | Rows: {len(df)}")

    df = df.merge(seller_marketplaces, left_on="smp_id", right_on="id", how="left", suffixes=("", "_smp"))
    logging.info(f"✅ Join 4: + seller_marketplaces | Rows: {len(df)}")

    df = df.merge(marketplaces, left_on="mp_id", right_on="id", how="left", suffixes=("", "_mp"))
    logging.info(f"✅ Join 5: + marketplaces | Rows: {len(df)}")

    df = df.merge(products, left_on="p_id", right_on="id", how="left", suffixes=("", "_product"))
    logging.info(f"✅ Join 6: + products | Rows: {len(df)}")

except Exception as e:
    logging.exception("❌ Failed during joins")
    raise e

# ======================= Transformations ========================
try:
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    before_filter = len(df)

    df = df[
        (df['seller_id'] == 1) &
        (df['created_at'] >= '2025-01-01') &
        (df['created_at'] <= '2025-07-27')
    ]
    logging.info(f"✅ Filtered seller_id & date | Rows before: {before_filter}, after: {len(df)}")

    # Derived columns
    df['units_sold'] = df['quantity']
    df['amount'] = df['sub_total']
    df['unit_price'] = df['sales_price']
    df['product_name'] = df.apply(
        lambda row: row['name_pm'] if row['pv_id'] == 0 else row['name'], axis=1
    )
    df['sku'] = df.apply(
        lambda row: row['slug'] if row['pv_id'] == 0 else row['sku'], axis=1
    )
    df['marketplace_id'] = df.apply(
        lambda row: None if row['pv_id'] == 0 else row['mp_variant_id'], axis=1
    )

    # Final columns
    final_cols = [
        "pv_id", "pm_id", "order_id", "created_at", "seller_id", "smp_id", "status",
        "units_sold", "amount", "unit_price", "product_name", "sku", "marketplace_id",
        "name", "color", "name_mp", "image"
    ]

    missing_final_cols = [col for col in final_cols if col not in df.columns]
    if missing_final_cols:
        logging.warning(f"⚠️ Missing expected final columns: {missing_final_cols}")

    df = df[[col for col in final_cols if col in df.columns]]
    logging.info(f"✅ Final columns selected | Shape: {df.shape}")

except Exception as e:
    logging.exception("❌ Error during transformation")
    raise e

# ======================= Save Output ========================
try:
    final_path = output_dir / "final_merged_data.csv"
    df.to_csv(final_path, index=False)
    logging.info(f"✅ Final data saved to: {final_path}")
except Exception as e:
    logging.exception("❌ Failed to save final CSV")
    raise e

logging.info("🏁 final_data pipeline complete.\n")




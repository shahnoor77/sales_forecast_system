import pandas as pd
from sklearn.preprocessing import StandardScaler

def engineer_features(df: pd.DataFrame):
    df = df.copy()

    # Convert date
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['day_of_week'] = df['created_at'].dt.dayofweek
    df['hour'] = df['created_at'].dt.hour
    df['week_of_year'] = df['created_at'].dt.isocalendar().week

    # Text-based feature
    df['product_length'] = df['product_name'].apply(lambda x: len(str(x)))

    # Encode categorical
    df['marketplace_code'] = df['marketplace_name'].astype('category').cat.codes

    # Sort by product and time to calculate lag features
    df.sort_values(['product_name', 'created_at'], inplace=True)
    df['prev_day_sales'] = df.groupby('product_name')['sub_total'].shift(1).fillna(0)

    # Drop unnecessary columns if they exist
    drop_cols = ['order_id', 'created_at', 'product_name', 'marketplace_name']
    df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)

    # Ensure target exists
    if 'sub_total' not in df.columns:
        raise ValueError("sub_total column missing from input data")

    # Split X/y
    y = df['sub_total']
    X = df.drop('sub_total', axis=1)

    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    feature_names = X.columns.tolist()

    return X_scaled, y, scaler, feature_names

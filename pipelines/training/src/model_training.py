import os
import hopsworks
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from lightgbm import LGBMRegressor
from comet_ml import Experiment
import joblib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Comet ML API key
COMET_API_KEY = os.getenv("COMET_API_KEY")
COMET_PROJECT_NAME = os.getenv("COMET_PROJECT_NAME", "sales-forecasting")
COMET_WORKSPACE = os.getenv("COMET_WORKSPACE")

# Hopsworks credentials
HOPSWORKS_API_KEY = os.getenv("HOPSWORKS_API_KEY")
PROJECT_NAME = os.getenv("HOPSWORKS_PROJECT_NAME")

# Initialize Hopsworks client
project = hopsworks.login(api_key_value=HOPSWORKS_API_KEY, project=PROJECT_NAME)
fs = project.get_feature_store()


# Load data from Feature View
 
feature_view = fs.get_or_create_feature_view(
    name="product_sales_view",
    version=1,
    description="Feature view for sales prediction",
    labels=["sub_total"],
    query=fs.sql("""
        SELECT order_id, created_at, sub_total, sales_price, quantity, product_name, marketplace_name
        FROM product_sales_fg_1
    """)
)

# Fetch batch data
df = feature_view.get_batch_data()

 
# Feature Engineering

df['created_at'] = pd.to_datetime(df['created_at'])
df['day_of_week'] = df['created_at'].dt.dayofweek
df['hour'] = df['created_at'].dt.hour
df['product_length'] = df['product_name'].apply(lambda x: len(x))
df['marketplace_code'] = df['marketplace_name'].astype('category').cat.codes

# Drop unused columns
df = df.drop(['order_id', 'created_at', 'product_name', 'marketplace_name'], axis=1)

# Train-Test Split

X = df.drop("sub_total", axis=1)
y = df["sub_total"]

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

X_train, X_test, y_train, y_test = train_test_split(
    X_scaled, y, test_size=0.2, random_state=42
)


# Train LightGBM Model

model = LGBMRegressor()
model.fit(X_train, y_train)


# Evaluate

y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)


# Comet ML Logging

experiment = Experiment(
    api_key=COMET_API_KEY,
    project_name=COMET_PROJECT_NAME,
    workspace=COMET_WORKSPACE,
    auto_output_logging="simple"
)

experiment.set_name("lightgbm-product-sales")
experiment.log_metric("mse", mse)
experiment.log_parameters(model.get_params())


# Save and Log Model

joblib.dump(model, "model.pkl")
experiment.log_model("lightgbm_model", "model.pkl")


# Upload Model to Hopsworks
 
mr = project.get_model_registry()
model_meta = mr.python.create_model(
    name="product_sales_model",
    metrics={"mse": mse},
    model_dir=".",
    version=1,
    description="LightGBM model to predict sub_total sales"
)
model_meta.save("model.pkl")

print("Training completed and model registered successfully.")

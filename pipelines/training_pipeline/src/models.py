import joblib
import numpy as np
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, GridSearchCV
from lightgbm import LGBMRegressor

def train_and_tune_model(X, y, test_size=0.2, random_state=42):
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    # Define model and param grid
    model = LGBMRegressor(random_state=random_state)

    param_grid = {
        'n_estimators': [100, 200],
        'max_depth': [3, 5, 7],
        'learning_rate': [0.05, 0.1, 0.2],
        'num_leaves': [31, 50],
    }

    grid = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        scoring='neg_mean_squared_error',
        cv=3,
        n_jobs=-1,
        verbose=1
    )

    grid.fit(X_train, y_train)
    best_model = grid.best_estimator_

    # Evaluate
    y_pred = best_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    # Save best model
    joblib.dump(best_model, "model.pkl")

    # Return all useful outputs
    return {
        "model": best_model,
        "best_params": grid.best_params_,
        "mse": round(mse, 2),
        "rmse": round(rmse, 2),
        "r2": round(r2, 3),
        "X_test": X_test,
        "y_test": y_test,
        "y_pred": y_pred
    }

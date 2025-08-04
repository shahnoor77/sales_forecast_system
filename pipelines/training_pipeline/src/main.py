import comet_ml# Must be first import!
import logging
import numpy as np
import joblib
import matplotlib.pyplot as plt
import traceback
import os
from dotenv import load_dotenv


from src.hopsworks_config import init_hopsworks, get_sales_data
from src.feature_engineering import engineer_features
from src.models import train_and_tune_model  
from src.model_evaluation import (
    calculate_core_metrics,
    analyze_errors,
    calculate_business_impact,
    check_against_baseline,
    generate_deployment_report
)
from src.visualization import create_evaluation_plots
from src.logger import init_experiment
from src.model_registry import register_model

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

def main():
    try:
        # 1. Initialization
        logger.info("Loading environment variables")
        env = load_dotenv()
        
        logger.info("Initializing Comet experiment")
        experiment = init_experiment(
            api_key=os.environ["COMET_ML_API_KEY"],
            project_name=os.environ["COMET_ML_PROJECT_NAME"],
            workspace=os.environ["COMET_ML_WORKSPACE"]
        )
        
        logger.info("Initializing Hopsworks")
        project, fs = init_hopsworks(
            api_key=os.environ["HOPSWORKS_API_KEY"],
            project_name=os.environ["HOPSWORKS_PROJECT_NAME"]
        )

        # 2. Data Pipeline
        logger.info("Fetching sales data")
        df = get_sales_data(fs)
        experiment.log_metric("dataset_rows", len(df))
        experiment.log_metric("dataset_columns", len(df.columns))
        
        logger.info("Engineering features")
        X_scaled, y, scaler, feature_names = engineer_features(df)
        experiment.log_histogram_3d(y, name="target_distribution", step=0)

        # 3. Model Training + Tuning
        results = train_and_tune_model(X_scaled, y)

        model = results["model"]
        best_params = results["best_params"]
        mse = results["mse"]
        rmse = results["rmse"]
        r2 = results["r2"]
        X_test = results["X_test"]
        y_test = results["y_test"]
        y_pred = results["y_pred"]

        print("Model training complete.")
        print(f"Best Params: {best_params}")
        print(f"MSE: {mse}, RMSE: {rmse}, R²: {r2}")


        # Log best hyperparams and performance
        experiment.log_parameters(best_params)
        experiment.log_metrics({"mse": mse, "rmse": rmse, "r2": r2})

    # Save and log artifacts
        joblib.dump(scaler, "scaler.pkl")
        experiment.log_model("scaler", "scaler.pkl", overwrite=True)

        joblib.dump(model, "lightgbm_model.pkl")
        experiment.log_model("lightgbm_model", "lightgbm_model.pkl", overwrite=True)

    # Register model in Comet's Model Registry
        # Register model in Comet's Model Registry
        try:
            logger.info(" Attempting to register model and push to registry...")
            register_model(
                experiment=experiment,
                model=model,
                model_name="lightgbm_model",
                mse=mse
    )
            logger.success(" Model successfully pushed to Comet ML registry.")
        except Exception as e:
            logger.error(f" Error during model registration: {e}")
    


        # 4. Evaluation
        logger.info("Evaluating model")
        metrics = calculate_core_metrics(y_test, y_pred)
        experiment.log_metrics(metrics)
        
        error_stats = analyze_errors(y_test, y_pred)
        experiment.log_metrics({f"error_{k}": v for k, v in error_stats.items()})

        business_impact = calculate_business_impact(
            y_true=y_test,
            y_pred=y_pred,
            unit_cost=10.0,
            unit_price=25.0
        )
        experiment.log_metrics({f"biz_{k}": v for k, v in business_impact.items()})

        # 5. Visualizations
        logger.info("Generating evaluation visualizations")
        feature_importances = dict(zip(feature_names, model.feature_importances_))
        plots = create_evaluation_plots(
            y_true=y_test,
            y_pred=y_pred,
            feature_importances=feature_importances,
            model_name="lightgbm_model",
            target_name="sub_total"
        )
        for name, fig in plots.items():
            experiment.log_figure(figure_name=name, figure=fig, overwrite=True)
            plt.close(fig)

        # 6. Deployment Checklist
        deployment_report = generate_deployment_report(
            metrics=metrics,
            error_stats=error_stats,
            feature_importances=feature_importances
        )
        experiment.log_text(deployment_report, "deployment_checklist")
        logger.info(f"\n{deployment_report}")

        # 7. Register model
        logger.info("Registering model")
        register_model(
            experiment=experiment,
            model=model,
            model_name="lightgbm_model",
            mse=mse
        )
        joblib.dump(model, "./models/lightgbm_model.pkl")

       # Log model to Comet
        experiment.log_model(
            name="lightgbm_model",
            file_or_folder="./models/lightgbm_model.pkl",
        overwrite=True
)

        logger.info("Pipeline completed successfully")
        experiment.log_other("status", "success")

    except Exception as e:
        logger.exception(f"Pipeline failed: {str(e)}")
        if 'experiment' in locals():
            experiment.log_text(traceback.format_exc(), "exception_traceback")
        raise
    finally:
        if 'experiment' in locals():
            experiment.end()
        plt.close('all')
        logger.info("Pipeline execution completed")

if __name__ == "__main__":
    main()

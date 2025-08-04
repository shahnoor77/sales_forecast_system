# src/model_registry.py
import joblib
import tempfile
import os
import logging

# Initialize logger properly
logger = logging.getLogger(__name__)

def register_model(experiment, model, mse, model_name="sales_forecast_model"):
    """
    Register the trained model in Comet ML model registry
    
    Args:
        experiment: Comet ML experiment object
        model: The trained model object to register
        mse: Model's mean squared error (must be a float)
        model_name: Name for the registered model
    """
    try:
        # Validate mse is a numeric value
        mse_float = float(mse)
        
        # Save model to temporary file
        model_filename = f"{model_name}.pkl"
        joblib.dump(model, model_filename)
        
        # Log model to Comet ML
        experiment.log_model(
            name=model_name,
            file_or_folder=model_filename,
            overwrite=True,
            metadata={"mse": mse_float}
        )
        
        # Log metrics separately
        experiment.log_metric("mse", mse_float)
        
        logger.info(f"Model {model_name} registered successfully with MSE: {mse_float:.2f}")
        
        # Clean up temporary file
        os.remove(model_filename)
        
    except Exception as e:
        logger.error(f"Failed to register model: {str(e)}")
        # Ensure file is cleaned up even if error occurs
        if os.path.exists(model_filename):
            os.remove(model_filename)
        raise
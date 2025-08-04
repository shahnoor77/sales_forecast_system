from comet_ml import Experiment

def init_experiment(api_key, project_name, workspace):
    experiment = Experiment(
        api_key=api_key,
        project_name=project_name,
        workspace=workspace,
        auto_output_logging="simple"
    )
    return experiment
def log_metrics(experiment, mse, model):
    experiment.set_name("lightgbm-product-sales")
    experiment.log_metric("mse", mse)
    experiment.log_parameters(model.get_params())
    experiment.log_model("lightgbm_model", "model.pkl")
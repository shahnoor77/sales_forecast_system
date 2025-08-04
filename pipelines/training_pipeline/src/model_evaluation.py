from comet_ml import API
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score,explained_variance_score
import numpy as np
import pandas as pd

def calculate_core_metrics(y_true, y_pred):
    """Calculate and return all basic metrics"""
    return {
        'mse': mean_squared_error(y_true, y_pred),
        'rmse': np.sqrt(mean_squared_error(y_true, y_pred)),
        'mae': mean_absolute_error(y_true, y_pred),
        'r2': r2_score(y_true, y_pred),
        'explained_variance': explained_variance_score(y_true, y_pred)
    }



def analyze_errors(y_true, y_pred):
    errors = y_true - y_pred
    return {
        'max_overprediction': errors.min(),
        'max_underprediction': errors.max(),
        'error_std': errors.std(),
        'error_skew': pd.Series(errors).skew(),
        'percentile_5th': np.percentile(errors, 5),
        'percentile_95th': np.percentile(errors, 95)
    }



def calculate_business_impact(y_true, y_pred, unit_cost: float, unit_price: float) -> dict:
    if len(y_true) != len(y_pred):
        raise ValueError("Length of y_true and y_pred must match")

    y_true = pd.Series(y_true).fillna(0)
    y_pred = pd.Series(y_pred).fillna(0)

    errors = y_true - y_pred
    over_errors = errors[errors < 0]   # overpredictions
    under_errors = errors[errors > 0]  # underpredictions

    lost_profit = (under_errors * (unit_price - unit_cost)).sum()
    waste_cost = (abs(over_errors) * unit_cost).sum()

    total_cost = lost_profit + waste_cost
    cost_ratio = waste_cost / lost_profit if lost_profit != 0 else np.inf
    net_revenue_impact = (unit_price * y_true.sum()) - waste_cost

    results = {
        "total_predictions": len(y_true),
        "underprediction_count": len(under_errors),
        "overprediction_count": len(over_errors),
        "total_lost_profit": round(lost_profit, 2),
        "total_waste_cost": round(waste_cost, 2),
        "total_business_cost": round(total_cost, 2),
        "waste_to_profit_ratio": round(cost_ratio, 2),
        "net_revenue_impact": round(net_revenue_impact, 2),
    }

    return results





def check_against_baseline(experiment, current_metrics, baseline_thresholds):
    alerts = []
    if current_metrics['r2'] < baseline_thresholds.get('r2', 0.7):
        alerts.append('R² below threshold')
    
    experiment.log_metrics({'alert_count': len(alerts)})
    return {
        'status': 'OK' if not alerts else 'WARNING',
        'alerts': alerts
    }

def generate_deployment_report(metrics, error_stats, feature_importances=None):
 
    # Thresholds (customize these per project requirements)
    DEPLOYMENT_THRESHOLDS = {
        'r2': {'min': 0.7, 'ideal': 0.8},
        'mae': {'max': 3.0, 'ideal': 2.5},
        'max_overprediction': {'max': 15.0},
        'max_underprediction': {'max': 15.0}
    }
    
    # Evaluation Results
    report = [

        "CORE METRICS",
        f"  • R²: {metrics['r2']:.3f} ({'✓' if metrics['r2'] >= DEPLOYMENT_THRESHOLDS['r2']['min'] else '✗'})",
        f"    - {'EXCEEDS' if metrics['r2'] >= DEPLOYMENT_THRESHOLDS['r2']['ideal'] else 'Meets'} ideal threshold ({DEPLOYMENT_THRESHOLDS['r2']['ideal']})",
        "",
        f"  • MAE: {metrics['mae']:.2f} ({'✓' if metrics['mae'] <= DEPLOYMENT_THRESHOLDS['mae']['max'] else '✗'})",
        f"    - {'UNDER' if metrics['mae'] <= DEPLOYMENT_THRESHOLDS['mae']['ideal'] else 'Within'} ideal threshold ({DEPLOYMENT_THRESHOLDS['mae']['ideal']})",
        "",
        "ERROR ANALYSIS",
        f"  • Max Overprediction: {error_stats['max_overprediction']:.2f}",
        f"  • Max Underprediction: {error_stats['max_underprediction']:.2f}",
        f"  • 95% Errors Between: [{error_stats['percentile_5th']:.2f}, {error_stats['percentile_95th']:.2f}]",
        ""
    ]
    
    # Feature Importance Section (if provided)
    if feature_importances:
        report.extend([
            "TOP FEATURES",
            *[f"  • {feat}: {imp:.2%}" 
              for feat, imp in sorted(feature_importances.items(), 
                                   key=lambda x: x[1], 
                                   reverse=True)[:3]]
        ])
    
    # Final Recommendation
    meets_criteria = all([
        metrics['r2'] >= DEPLOYMENT_THRESHOLDS['r2']['min'],
        metrics['mae'] <= DEPLOYMENT_THRESHOLDS['mae']['max'],
        abs(error_stats['max_overprediction']) <= DEPLOYMENT_THRESHOLDS['max_overprediction']['max'],
        abs(error_stats['max_underprediction']) <= DEPLOYMENT_THRESHOLDS['max_underprediction']['max']
    ])
    
    report.extend([
        "",
        "RECOMMENDATION",
        f"  {'APPROVE DEPLOYMENT' if meets_criteria else 'DO NOT DEPLOY'}",
        "",
        f"{'READY FOR PRODUCTION' if meets_criteria else 'NEEDS IMPROVEMENT'}",

    ])
    
    return "\n".join(report)


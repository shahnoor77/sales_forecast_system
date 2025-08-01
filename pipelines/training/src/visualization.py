# src/model_evaluation/visualization.py
"""
Model evaluation visualization module
Generates standardized plots for model performance analysis
"""

from typing import Dict, Optional
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.metrics import r2_score

# Configure global plot settings
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("viridis")
plt.rcParams.update({
    'font.size': 12,
    'figure.titlesize': 14,
    'axes.titlesize': 13,
    'axes.labelsize': 12
})

class ModelVisualizer:
    """
    Creates consistent evaluation visualizations with configurable styling
    
    Attributes:
        model_name: Display name for plot titles
        target_name: Name of target variable (for axis labels)
        figsize: Default figure size (width, height)
    """
    
    def __init__(
        self,
        model_name: str = "Model",
        target_name: str = "sub_total",
        figsize: tuple = (10, 6)
    ):
        self.model_name = model_name
        self.target_name = target_name
        self.figsize = figsize
        
    def create_plots(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        feature_importances: Optional[Dict[str, float]] = None
    ) -> Dict[str, plt.Figure]:
        """
        Generates all evaluation plots
        
        Args:
            y_true: Array of true target values
            y_pred: Array of predicted values
            feature_importances: Dictionary of {feature_name: importance_score}
            
        Returns:
            Dictionary mapping plot names to matplotlib Figures
        """
        plots = {}
        residuals = y_true - y_pred
        r2 = r2_score(y_true, y_pred)
        
        # 1. Prediction Scatter Plot
        plots["actual_vs_predicted"] = self._create_scatter_plot(y_true, y_pred, r2)
        
        # 2. Residual Analysis
        plots["residual_analysis"] = self._create_residual_plot(y_pred, residuals)
        
        # 3. Error Distribution
        plots["error_distribution"] = self._create_error_histogram(residuals)
        
        # 4. Feature Importance (if provided)
        if feature_importances:
            plots["feature_importance"] = self._create_feature_importance_plot(
                feature_importances
            )
        
        return plots
    
    def _create_scatter_plot(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        r2: float
    ) -> plt.Figure:
        """Actual vs Predicted values scatter plot"""
        fig, ax = plt.subplots(figsize=self.figsize)
        sns.scatterplot(
            x=y_true,
            y=y_pred,
            alpha=0.6,
            ax=ax
        )
        
        # Perfect prediction line
        ax.plot(
            [min(y_true), max(y_true)], 
            [min(y_true), max(y_true)], 
            'r--',
            lw=2,
            label='Ideal'
        )
        
        ax.set(
            xlabel=f'Actual {self.target_name}',
            ylabel=f'Predicted {self.target_name}',
            title=f'{self.model_name} - Actual vs Predicted (R²={r2:.2f})'
        )
        ax.legend()
        fig.tight_layout()
        return fig
    
    def _create_residual_plot(
        self,
        y_pred: np.ndarray,
        residuals: np.ndarray
    ) -> plt.Figure:
        """Residual analysis plot"""
        fig, ax = plt.subplots(figsize=self.figsize)
        sns.residplot(
            x=y_pred,
            y=residuals,
            lowess=True,
            scatter_kws={'alpha': 0.6},
            line_kws={'color': 'red', 'lw': 2},
            ax=ax
        )
        
        # Reference lines
        ax.axhline(y=0, color='black', linestyle='--')
        ax.axhline(y=np.mean(residuals), color='green', linestyle='-')
        
        ax.set(
            xlabel='Predicted Values',
            ylabel='Residuals',
            title=f'{self.model_name} - Residual Analysis'
        )
        fig.tight_layout()
        return fig
    
    def _create_error_histogram(
        self,
        residuals: np.ndarray
    ) -> plt.Figure:
        """Prediction error distribution"""
        fig, ax = plt.subplots(figsize=self.figsize)
        sns.histplot(
            residuals,
            kde=True,
            bins=30,
            ax=ax
        )
        
        # Annotations
        ax.axvline(x=0, color='red', linestyle='--')
        ax.axvline(x=np.mean(residuals), color='green', linestyle='-')
        
        ax.set(
            xlabel='Prediction Error',
            title=f'{self.model_name} - Error Distribution\n'
                  f'Mean Error: {np.mean(residuals):.2f} ± {np.std(residuals):.2f}'
        )
        fig.tight_layout()
        return fig
    
    def _create_feature_importance_plot(
        self,
        feature_importances: Dict[str, float]
    ) -> plt.Figure:
        """Horizontal bar plot of feature importances"""
        # Sort features by importance
        features = sorted(
            feature_importances.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        fig, ax = plt.subplots(figsize=(self.figsize[0], 0.5 * self.figsize[1]))
        sns.barplot(
            x=[imp for _, imp in features],
            y=[name for name, _ in features],
            ax=ax
        )
        
        ax.set(
            xlabel='Importance Score',
            ylabel='',
            title=f'{self.model_name} - Feature Importance'
        )
        fig.tight_layout()
        return fig


# Convenience function for quick plotting
def create_evaluation_plots(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    feature_importances: Optional[Dict[str, float]] = None,
    model_name: str = "Model",
    target_name: str = "sub_total"
) -> Dict[str, plt.Figure]:
    """
    One-shot function to generate all standard evaluation plots
    
    Example:
        plots = create_evaluation_plots(
            y_test,
            y_pred,
            feature_importances=model.feature_importances_,
            model_name="SalesForecaster"
        )
    """
    visualizer = ModelVisualizer(model_name=model_name, target_name=target_name)
    return visualizer.create_plots(y_true, y_pred, feature_importances)
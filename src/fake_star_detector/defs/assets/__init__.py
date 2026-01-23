from . import simpler_model, pr_analysis
from .dbt_models import dbt_model_assets
from dagster import load_assets_from_modules

simpler_model_assets = load_assets_from_modules(
    [simpler_model], key_prefix="simpler_model", group_name="simpler_model"
)

pr_analysis_assets = load_assets_from_modules(
    [pr_analysis], key_prefix="pr_analysis", group_name="pr_analysis"
)

__all__ = [
    "simpler_model_assets",
    "dbt_model_assets",
    "pr_analysis_assets",
]

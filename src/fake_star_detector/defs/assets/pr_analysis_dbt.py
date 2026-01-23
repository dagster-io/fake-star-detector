"""
PR analysis dbt assets for fake star detection.

This module loads dbt models that analyze PR data to identify suspicious patterns.
The dbt models depend on the pull_requests and pr_creators Dagster assets.
"""

from pathlib import Path
from typing import Mapping, Any
from dagster import AssetKey, AssetExecutionContext, ResourceParam
from dagster_dbt import (
    DbtProject,
    dbt_assets,
    DbtCliResource,
    DagsterDbtTranslator,
)

from ..resources import DBT_PROJECT_PATH, DBT_PROFILES_PATH


class PrAnalysisTranslator(DagsterDbtTranslator):
    """Custom translator to prefix pr_analysis dbt assets (but not sources)."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        base_key = super().get_asset_key(dbt_resource_props)
        resource_type = dbt_resource_props.get("resource_type", "")
        if resource_type == "source":
            return base_key
        return base_key.with_prefix("pr_analysis")


dbt_project_instance = DbtProject(
    project_dir=Path(DBT_PROJECT_PATH),
    profiles_dir=Path(DBT_PROFILES_PATH),
)

try:
    dbt_project_instance.prepare_if_dev()
    dbt_manifest_path = dbt_project_instance.manifest_path
    
    if not dbt_manifest_path.exists():
        from ..resources import dbt_resource
        import subprocess
        import os
        
        dbt_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        
        env = os.environ.copy()
        if "DUCKDB_DATABASE" not in env:
            env["DUCKDB_DATABASE"] = "data/fake_star_detector.duckdb"
        
        result = subprocess.run(
            ["dbt", "parse", "--profiles-dir", DBT_PROFILES_PATH],
            cwd=DBT_PROJECT_PATH,
            env=env,
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"dbt parse failed: {result.stderr}")
    
    if not dbt_manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found at {dbt_manifest_path}")

    @dbt_assets(
        manifest=dbt_manifest_path,
        select="pr_analysis.*",
        dagster_dbt_translator=PrAnalysisTranslator(),
        deps=[
            AssetKey(["pr_analysis", "pull_requests"]),
            AssetKey(["pr_analysis", "pr_creators"]),
        ],
    )
    def pr_analysis_dbt_assets(
        context: AssetExecutionContext, dbt: ResourceParam[DbtCliResource]
    ):
        """dbt assets for PR analysis - depends on pull_requests and pr_creators Dagster assets."""
        yield from dbt.cli(
            ["build", "--select", "pr_analysis.*"], context=context
        ).stream()

    pr_analysis_dbt_model_assets = pr_analysis_dbt_assets
except Exception as e:
    import warnings
    warnings.warn(
        f"Failed to load PR analysis dbt assets: {e}. "
        "Run 'dbt compile' manually or ensure dbt project is properly configured."
    )
    pr_analysis_dbt_model_assets = []

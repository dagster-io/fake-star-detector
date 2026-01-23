"""
Complex fake star detection model using dbt.

This module loads a dbt project that analyzes GitHub Archive data to identify suspicious users
who starred repositories and estimates a FAKE STAR score using multiple heuristics.

The dbt project expects a target_repo variable set in the SQL models.
Outputs four DuckDB tables: staging actions, actor summaries, repo summaries, and fake star stats.
"""

from pathlib import Path
from typing import Mapping, Any
from dagster import AssetKey, AssetExecutionContext, ResourceParam
from dagster_dbt import (
    DbtProject,
    dbt_assets,
    DbtCliResource,
    DagsterDbtTranslator,
    get_asset_key_for_source,
)

from ..resources import DBT_PROJECT_PATH, DBT_PROFILES_PATH


class ComplexDetectorTranslator(DagsterDbtTranslator):
    """Custom translator to prefix complex_detector dbt assets (but not sources)."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        base_key = super().get_asset_key(dbt_resource_props)
        resource_type = dbt_resource_props.get("resource_type", "")
        if resource_type == "source":
            return base_key
        return base_key.with_prefix("complex_detector")


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
        select="complex_detector.*",
        dagster_dbt_translator=ComplexDetectorTranslator(),
    )
    def complex_detector_dbt_assets(
        context: AssetExecutionContext, dbt: ResourceParam[DbtCliResource]
    ):
        """dbt assets for complex fake star detection model."""
        yield from dbt.cli(
            ["build", "--select", "complex_detector.*"], context=context
        ).stream()

    complex_model_assets = complex_detector_dbt_assets
except Exception as e:
    import warnings
    warnings.warn(
        f"Failed to load dbt assets: {e}. "
        "Run 'dbt compile' manually or ensure dbt project is properly configured."
    )
    complex_model_assets = []

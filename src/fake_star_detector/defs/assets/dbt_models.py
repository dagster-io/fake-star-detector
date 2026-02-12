"""
Unified dbt assets for fake star detection.

This module loads all dbt models (complex_detector and pr_analysis) into a single
dbt assets function for easier management.
"""

from pathlib import Path
from typing import Mapping, Any, Optional, Set
from dagster import AssetKey, AssetExecutionContext, ResourceParam
from dagster_dbt import (
    DbtProject,
    dbt_assets,
    DbtCliResource,
    DagsterDbtTranslator,
)

from ..resources import DBT_PROJECT_PATH, DBT_PROFILES_PATH


class UnifiedDbtTranslator(DagsterDbtTranslator):
    """
    Custom translator that preserves dbt model prefixes and automatically maps sources to Dagster assets.

    Models in the complex_detector folder get the "complex_detector" prefix.
    Models in the pr_analysis folder get the "pr_analysis" prefix.
    Sources from "simpler_model" are automatically mapped to their corresponding Dagster assets.
    All dbt models use "duckdb" as compute_kind.
    """

    # Mapping of (source_name, table_name) -> Dagster AssetKey
    SOURCE_TO_ASSET_MAP = {
        # PR analysis sources (new source group)
        ("pr_analysis", "pull_requests"): AssetKey(["pr_analysis", "pull_requests"]),
        ("pr_analysis", "pr_creators"): AssetKey(["pr_analysis", "pr_creators"]),
        ("pr_analysis", "pr_creators_with_user_info"): AssetKey(
            ["pr_analysis", "pr_creators_with_user_info"]
        ),
        # Simpler model sources
        ("simpler_model", "stargazers"): AssetKey(["simpler_model", "stargazers"]),
        ("simpler_model", "stargazers_with_user_info"): AssetKey(
            ["simpler_model", "stargazers_with_user_info"]
        ),
        ("simpler_model", "classified_stargazers"): AssetKey(
            ["simpler_model", "classified_stargazers"]
        ),
        # Legacy mappings (in case any models still use simpler_model for PR sources)
        ("simpler_model", "pull_requests"): AssetKey(["pr_analysis", "pull_requests"]),
        ("simpler_model", "pr_creators"): AssetKey(["pr_analysis", "pr_creators"]),
    }

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        base_key = super().get_asset_key(dbt_resource_props)
        resource_type = dbt_resource_props.get("resource_type", "")

        # Don't prefix sources - they'll be handled by get_asset_key_for_source
        if resource_type == "source":
            return base_key

        # Check the model's unique_id to determine prefix
        unique_id = dbt_resource_props.get("unique_id", "")

        # If the unique_id contains complex_detector, prefix with complex_detector
        if "complex_detector" in unique_id:
            return base_key.with_prefix("complex_detector")
        # If the unique_id contains pr_analysis, prefix with pr_analysis
        elif "pr_analysis" in unique_id:
            return base_key.with_prefix("pr_analysis")

        # Default: return as-is (shouldn't happen for our models)
        return base_key

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override metadata to set compute_kind to "duckdb" for all dbt models.
        """
        metadata = super().get_metadata(dbt_resource_props)
        resource_type = dbt_resource_props.get("resource_type", "")

        # Set compute_kind to duckdb for all models (not sources)
        if resource_type == "model":
            metadata = dict(metadata) if metadata else {}
            metadata["compute_kind"] = "duckdb"

        return metadata

    def get_asset_key_for_source(
        self, dbt_source_props: Mapping[str, Any]
    ) -> Optional[AssetKey]:
        """
        Automatically map dbt sources to their corresponding Dagster assets.

        This method is called by Dagster's dbt integration when processing source dependencies.
        The dbt_source_props contains properties from the dbt manifest for a source table.

        According to Dagster docs, the source props should contain:
        - source_name: The name of the source (e.g., "simpler_model")
        - name: The name of the table within that source (e.g., "pull_requests")
        - unique_id: Full unique identifier (e.g., "source.simpler_model.pull_requests")
        """
        # The dbt manifest structure uses these property names
        source_name = dbt_source_props.get("source_name")
        table_name = dbt_source_props.get(
            "name"
        )  # For sources, "name" is the table name

        # Fallback: parse from unique_id if properties aren't available
        if not source_name or not table_name:
            unique_id = dbt_source_props.get("unique_id", "")
            if "source." in unique_id:
                # Parse unique_id format: "source.simpler_model.pull_requests"
                parts = unique_id.split(".")
                if len(parts) >= 3:
                    source_name = parts[1]  # "simpler_model"
                    table_name = parts[2]  # "pull_requests"

        # Look up in our mapping
        if source_name and table_name:
            key = (source_name, table_name)
            mapped_asset_key = self.SOURCE_TO_ASSET_MAP.get(key)
            if mapped_asset_key:
                return mapped_asset_key

        # Return None to use default behavior for unmapped sources
        return None

    def get_deps_asset_keys(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Set[AssetKey]:
        """
        Automatically resolve source dependencies to Dagster assets.

        The parent class's get_deps_asset_keys should automatically call
        get_asset_key_for_source for each source dependency. However, we
        explicitly resolve them here to ensure the mapping works correctly.
        """
        # Get base dependencies from parent class
        # This should already include source dependencies resolved via get_asset_key_for_source
        deps = super().get_deps_asset_keys(dbt_resource_props)

        resource_type = dbt_resource_props.get("resource_type", "")

        # Only process models (not sources themselves)
        if resource_type == "model":
            depends_on = dbt_resource_props.get("depends_on", {})
            sources = depends_on.get("sources", [])

            # For each source dependency, explicitly resolve it using our mapping
            # This ensures dependencies are created even if parent class doesn't handle it correctly
            for source_id in sources:
                source_str = str(source_id)

                # Parse source ID format: "source.pr_analysis.pull_requests" or "source.simpler_model.stargazers"
                if "source." in source_str:
                    parts = source_str.split(".")
                    if len(parts) >= 3:
                        source_name = parts[1]  # "pr_analysis" or "simpler_model"
                        table_name = parts[2]  # "pull_requests", "pr_creators", etc.

                        # Look up in our mapping and add to dependencies
                        key = (source_name, table_name)
                        mapped_asset_key = self.SOURCE_TO_ASSET_MAP.get(key)
                        if mapped_asset_key:
                            deps.add(mapped_asset_key)

        return deps


dbt_project_instance = DbtProject(
    project_dir=Path(DBT_PROJECT_PATH),
    profiles_dir=Path(DBT_PROFILES_PATH),
)

try:
    dbt_project_instance.prepare_if_dev()
    dbt_manifest_path = dbt_project_instance.manifest_path

    if not dbt_manifest_path.exists():
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
        select="complex_detector.* pr_analysis.*",
        dagster_dbt_translator=UnifiedDbtTranslator(),
    )
    def all_dbt_assets(
        context: AssetExecutionContext, dbt: ResourceParam[DbtCliResource]
    ):
        """
        Unified dbt assets for all models.

        Includes:
        - complex_detector.* models (complex fake star detection)
        - pr_analysis.* models (PR analysis, depends on PR Dagster assets)
        """
        yield from dbt.cli(
            ["build", "--select", "complex_detector.*", "pr_analysis.*"],
            context=context,
        ).stream()

    dbt_model_assets = all_dbt_assets
except Exception as e:
    import warnings

    warnings.warn(
        f"Failed to load dbt assets: {e}. "
        "Run 'dbt compile' manually or ensure dbt project is properly configured."
    )
    dbt_model_assets = []

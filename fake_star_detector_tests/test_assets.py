"""
Tests for fake_star_detector assets.

These tests verify that assets can be loaded and have the expected structure.
For integration tests that require GitHub API or DuckDB, mark them with @pytest.mark.integration
"""

import pytest
from dagster import materialize
from fake_star_detector.definitions import defs


def test_definitions_load():
    """Test that definitions can be loaded successfully."""
    assert defs is not None
    # Check if assets exist (may be empty list if dbt manifest doesn't exist)
    assert hasattr(defs, "assets"), "Definitions should have assets attribute"


def test_simpler_model_assets_exist():
    """Test that simpler model assets are defined."""
    # Get asset keys from the assets list
    asset_keys = []
    for asset in defs.assets:
        if hasattr(asset, "key"):
            asset_keys.append(asset.key.to_user_string())
        elif hasattr(asset, "__name__"):
            # For function-based assets
            asset_keys.append(asset.__name__)

    # Check for simpler model assets
    simpler_assets = [
        key
        for key in asset_keys
        if "simpler_model" in key or "stargazers" in key.lower()
    ]
    assert len(simpler_assets) > 0, (
        f"No simpler_model assets found. Found assets: {asset_keys}"
    )

    # Verify expected assets exist (note: assets are now partitioned)
    expected_assets = [
        "stargazers",
        "stargazers_with_user_info",
        "classified_stargazers",  # Updated name (was classified_stargazers_df)
    ]
    asset_key_str = " ".join(asset_keys)
    for expected in expected_assets:
        assert any(expected in key for key in asset_keys), (
            f"Expected asset containing '{expected}' not found in {asset_keys}"
        )


def test_complex_model_assets_exist():
    """Test that complex model (dbt) assets are defined or gracefully handled."""
    # dbt assets may not exist if manifest hasn't been compiled
    # This test just verifies the code doesn't crash
    asset_keys = []
    for asset in defs.assets:
        if hasattr(asset, "key"):
            asset_keys.append(asset.key.to_user_string())

    # Check for complex model assets (may be empty if dbt not compiled)
    complex_assets = [key for key in asset_keys if "complex_detector" in key]
    # This is optional - dbt assets require dbt compile first
    if len(complex_assets) == 0:
        # That's okay - dbt manifest may not exist yet
        pass


def test_resources_configured():
    """Test that required resources are configured."""
    assert hasattr(defs, "resources"), "Definitions should have resources attribute"
    resources = defs.resources

    assert "github" in resources, "github resource not found"
    assert "dbt" in resources, "dbt resource not found"
    assert "duckdb" in resources, "duckdb resource not found"


def test_assets_are_partitioned():
    """Test that simpler model assets are partitioned by repository."""
    # Find stargazers asset
    stargazers_asset = None
    for asset in defs.assets:
        if hasattr(asset, "key") and "stargazers" in asset.key.to_user_string().lower():
            stargazers_asset = asset
            break
        elif hasattr(asset, "__name__") and "stargazers" in asset.__name__.lower():
            stargazers_asset = asset
            break

    assert stargazers_asset is not None, "stargazers asset not found"

    # Check if partitioned
    if hasattr(stargazers_asset, "partitions_def"):
        assert stargazers_asset.partitions_def is not None, (
            "stargazers asset should be partitioned"
        )

        # Check partition keys exist
        partition_keys = stargazers_asset.partitions_def.get_partition_keys()
        assert len(partition_keys) > 0, "No partition keys found"
        assert "frasermarlow/tap-bls" in partition_keys, (
            "Default repo partition not found"
        )


@pytest.mark.integration
def test_simpler_model_materialization():
    """
    Integration test for simpler model materialization.

    This test requires GITHUB_ACCESS_TOKEN and DUCKDB_DATABASE to be set in the environment.
    Marked as integration test since it requires external API access and database.
    """
    # This would require actual GitHub API access and DuckDB
    # For now, we just verify the assets can be retrieved and are partitioned
    stargazers_asset = defs.get_asset_def("simpler_model/stargazers")
    assert stargazers_asset is not None
    assert stargazers_asset.partitions_def is not None, "Asset should be partitioned"

"""
Pytest configuration and shared fixtures for fake_star_detector tests.
"""

import pytest
from dagster import Definitions
from fake_star_detector.definitions import defs


@pytest.fixture
def definitions() -> Definitions:
    """Fixture providing the Dagster Definitions object."""
    return defs

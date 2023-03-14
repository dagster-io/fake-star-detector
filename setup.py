import glob

from setuptools import find_packages, setup

setup(
    name="fake_star_detector",
    packages=find_packages(exclude=["fake_star_detector_tests"]),
    # package data paths are relative to the package key
    package_data={
        "fake_star_detector": ["../" + path for path in glob.glob("dbt_project/**", recursive=True)]
    },
    install_requires=[
        "dagster",
        "dagster-cloud[serverless]",
        "dagster-dbt",
        "PyGithub",
        "pandas",
        "matplotlib",
        "nbconvert",
        "nbformat",
        "ipykernel",
        "jupytext",
        "dbt-core",
        "dbt-bigquery",
        # packaging v22 has build compatibility issues with dbt as of 2022-12-07
        "packaging<22.0",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)

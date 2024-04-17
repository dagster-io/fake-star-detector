import glob

from setuptools import find_packages, setup

setup(
    name="fake_star_detector",
    packages=find_packages(exclude=["fake_star_detector_tests"]),
    # package data paths are relative to the package key
    package_data={"fake_star_detector": ["../" + path for path in glob.glob("dbt_project/**", recursive=True)]},
    install_requires=[
        "dagster==1.2.2",
        "dagster-cloud[serverless]==1.2.2",
        "dagster-dbt==0.18.2",
        "PyGithub",
        "Pydantic<2",
        "pandas==1.5.3",
        "matplotlib",
        "nbconvert",
        "nbformat",
        "ipykernel",
        "jupytext",
        "dbt-core==1.4",
        "dbt-bigquery==1.4",
        # packaging v22 has build compatibility issues with dbt as of 2022-12-07
        "packaging<22.0",
        "pendulum<3.0",
    ],
    extras_require={"dev": ["dagit==1.2.2", "pytest"]},
)

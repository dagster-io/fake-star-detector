from setuptools import find_packages, setup

setup(
    name="my_project_1",
    packages=find_packages(exclude=["my_project_1_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud[serverless]",
        "PyGithub",
        "pandas",
        "matplotlib",
        "nbconvert",
        "nbformat",
        "ipykernel",
        "jupytext",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)

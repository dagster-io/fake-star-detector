from dagster import Definitions, define_asset_job

from .defs.assets import (
    simpler_model_assets,
    dbt_model_assets,
    pr_analysis_assets,
)
from .defs.partitions import REPOSITORY_PARTITIONS
from .defs.resources import dbt_resource, duckdb_resource, github_resource

all_assets_list = list(simpler_model_assets) + list(pr_analysis_assets)
if dbt_model_assets:
    all_assets_list.append(dbt_model_assets)

# Job for stargazer pipeline: collects stargazer data from GitHub
stargazer_pipeline_job = define_asset_job(
    name="stargazer_pipeline",
    selection=[
        "simpler_model/stargazers",
        "simpler_model/stargazers_with_user_info",
        "simpler_model/classified_stargazers",
    ],
    partitions_def=REPOSITORY_PARTITIONS,
    description="Collects stargazer data from GitHub API for a specific repository. Run with partition_key='owner/repo' to process a specific repository.",
)

# Job for PR pipeline: collects PR data from GitHub
pr_pipeline_job = define_asset_job(
    name="pr_pipeline",
    selection=[
        "pr_analysis/pull_requests",
        "pr_analysis/pr_creators",
    ],
    partitions_def=REPOSITORY_PARTITIONS,
    description="Collects pull request data from GitHub API for a specific repository. Run with partition_key='owner/repo' to process a specific repository.",
)

# Combined job: runs both pipelines in sequence for a repository
repo_data_collection_job = define_asset_job(
    name="repo_data_collection",
    selection=[
        # Stargazer pipeline
        "simpler_model/stargazers",
        "simpler_model/stargazers_with_user_info",
        "simpler_model/classified_stargazers",
        # PR pipeline
        "pr_analysis/pull_requests",
        "pr_analysis/pr_creators",
    ],
    partitions_def=REPOSITORY_PARTITIONS,
    description="Collects both stargazer and PR data from GitHub API for a specific repository. Run with partition_key='owner/repo' to process a specific repository.",
)

defs = Definitions(
    assets=all_assets_list,
    jobs=[
        stargazer_pipeline_job,
        pr_pipeline_job,
        repo_data_collection_job,
    ],
    resources={
        "github": github_resource,
        "dbt": dbt_resource,
        "duckdb": duckdb_resource,
    },
)

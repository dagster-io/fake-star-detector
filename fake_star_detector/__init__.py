from dagster import Definitions, ConfigurableResource, EnvVar
from dagster_dbt import dbt_cli_resource
from github import Github

from .assets import simpler_model_assets, complex_model_assets, DBT_PROJECT_PATH, DBT_PROFILES

class GithubAPI(ConfigurableResource):
    access_token: str

    def get_client(self) -> Github:
        return Github(
            login_or_token=self.access_token,
            retry=3,
            per_page=100
        )

defs = Definitions(
    assets=[*simpler_model_assets, *complex_model_assets],
    resources={
        "github": GithubAPI(access_token=EnvVar("GITHUB_ACCESS_TOKEN")),
        "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    }
)

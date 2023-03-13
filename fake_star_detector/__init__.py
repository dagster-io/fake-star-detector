from dagster import Definitions, load_assets_from_package_module, resource, StringSource, file_relative_path
from dagster_dbt import dbt_cli_resource
from github import Github

from .assets import simpler_model_assets, complex_model_assets, DBT_PROJECT_PATH, DBT_PROFILES


@resource(config_schema={"access_token": StringSource})
def github_api(init_context):
    # TODO: new resource & config api?
    return Github(login_or_token=init_context.resource_config["access_token"], retry=3, per_page=100)



defs = Definitions(
    assets=[*simpler_model_assets, *complex_model_assets],
    resources={
        "github_api": github_api.configured({"access_token": {"env": "GITHUB_ACCESS_TOKEN"}}),
        "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    }
)

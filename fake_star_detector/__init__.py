from dagster import Definitions, load_assets_from_modules, resource, StringSource
from github import Github

from . import assets


@resource(config_schema={"access_token": StringSource})
def github_api(init_context):
    # TODO: new resource & config api?
    return Github(login_or_token=init_context.resource_config["access_token"], retry=3, per_page=100)


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "github_api": github_api.configured({"access_token": {"env": "GITHUB_ACCESS_TOKEN"}}),
    }
)

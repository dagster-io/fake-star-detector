#####################################################################################################################
### This loads a dbt project which uses GitHub Archive data to identify suspicious users who starred
### the given repo, and estimates a FAKE STAR score for that repository using two separate heuristics
### to catch different types of fake accounts.
###
### **INPUT:**
## "target_repo" in "../../dbt_project/models/fake_star_detector/stg_all_actions_for_actors_who_starred_repo.sql"
### e.g. {% set target_repo = 'frasermarlow/tap-bls' %}
###
###
### OUTPUT: 4 BigQuery tables + estimated FAKE STAR score:
###  1) stg_all_actions_for_actors_who_starred_repo: all activity for users who starred repo in time period
###  2) starring_actor_summary: table with 1 row per user, summarizing similarity to other users in set
###  3) starring_actor_repo_summary: table with 1 row per repo, summarizing similarity to other repos actors touched
###  4) fake_star_stats: fake star summary stats (FAKE STAR score)
###
###
### NOTES:
### - by default, this script only evaluates stars in 2023
#####################################################################################################################


from dagster import file_relative_path
from dagster_dbt import load_assets_from_dbt_project

DBT_PROJECT_PATH = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES = file_relative_path(__file__, "../../dbt_project/config")

complex_model_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["complex_detector"]
)

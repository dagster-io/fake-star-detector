{{ config(materialized='table') }}

SELECT
    *,
    CASE
        WHEN repo IN (SELECT repo FROM {{ ref('stg_spammy_repos') }})
        THEN 'suspected-activity_cluster'
        ELSE 'unknown'
    END as fake_acct
FROM {{ ref('stg_starring_actor_repo_clusters') }}
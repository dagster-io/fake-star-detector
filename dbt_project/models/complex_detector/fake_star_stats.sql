{{ config(materialized='table') }}

SELECT
    COUNT(*) as total_users_starring_repo,
    COUNTIF(REGEXP_CONTAINS(fake_acct, 'suspected')) as fake_stars,
    COUNTIF(REGEXP_CONTAINS(fake_acct, 'cluster')) as fake_stars_activity_cluster,
    COUNTIF(REGEXP_CONTAINS(fake_acct, 'low')) as fake_stars_low_activity,
    COUNTIF(fake_acct = 'unknown') as real_stars,
    COUNTIF(REGEXP_CONTAINS(fake_acct, 'suspected')) / COUNT(*) * 100 as p_fake
FROM {{ ref('starring_actor_summary') }}

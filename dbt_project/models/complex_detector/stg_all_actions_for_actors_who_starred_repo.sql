{{ config(materialized='table') }} -- materialize as a table to save compute in the future

-- ## pull ALL activity for users who starred repo in time period

{% set target_repo = 'frasermarlow/tap-bls' %}
-- optional params (default to only 2023 data )
{% set start_star_lookabck_yymmdd = '230101' %} -- start date (Jan 1, 2023)
{% set end_yymmdd = '231231' %} -- end date (include all of 2023)


WITH watch_event_actors AS (
  SELECT
    DISTINCT actor.login as actor
  FROM `githubarchive.day.20*`
  WHERE (_TABLE_SUFFIX BETWEEN '{{start_star_lookabck_yymmdd}}' AND '{{end_yymmdd}}')
    AND type = 'WatchEvent' -- starred
    AND repo.name = '{{target_repo}}'
    AND actor.login NOT LIKE '%[bot]'-- reduce table size
)
SELECT
  actor.login as actor,
  CONCAT(type, IFNULL(JSON_EXTRACT(payload, '$.action'), '')) as event,
  DATE(created_at) date,
  created_at,
  actor.avatar_url,
  repo.name repo,
  org.login org,
  payload,
  IF(repo.name = '{{target_repo}}', TRUE, FALSE) as is_target_repo,
  IF(type = 'WatchEvent', TRUE, FALSE) as is_star,
FROM `githubarchive.day.20*`
WHERE
  (_TABLE_SUFFIX BETWEEN '{{start_star_lookabck_yymmdd}}' AND '{{end_yymmdd}}')
  AND actor.login IN (SELECT actor FROM watch_event_actors)
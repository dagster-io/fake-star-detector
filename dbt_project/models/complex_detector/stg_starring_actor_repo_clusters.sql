-- ## summarize activity per repo & cross join to compare to every other repo in set
-- summary table with 1 row per repo
WITH repo_summary AS (
    SELECT
    repo,
    ARRAY_AGG(DISTINCT event) as events,
    MIN(created_at) as min_activity,
    MAX(created_at) as max_activity,
    MIN(IF(is_target_repo AND is_star, created_at, NULL)) as star_time,
    ARRAY_AGG(DISTINCT DATE(created_at)) dates,
    COUNT(*)n,
    ARRAY_AGG(DISTINCT actor IGNORE NULLS) actors,
    ARRAY_AGG(DISTINCT IFNULL(org, 'no org')) orgs,
    FROM {{ ref('stg_all_actions_for_actors_who_starred_repo') }}

    -- exclude actors who interacted with very large number of repos (avoid memory exceeded error)
    WHERE actor NOT IN (
        SELECT actor FROM {{ ref('stg_starring_actor_overlap') }} WHERE n_repos > 200
    )
    GROUP BY 1
),
-- cross join repo_summary to compare each repo to each other repo in set
-- use nested queries to summarize arrays easily
repo_overlap AS (
    SELECT
    *,
    ARRAY_LENGTH(events_overlap) n_events_overlap,
    ARRAY_LENGTH(dates_overlap) n_dates_overlap,
    ARRAY_LENGTH(actor_overlap) n_actor_overlap,
    ARRAY_LENGTH(org_overlap) n_org_overlap,
    ARRAY_LENGTH(events_overlap)/n_events as p_events_overlap,
    ARRAY_LENGTH(dates_overlap)/n_dates as p_dates_overlap,
    ARRAY_LENGTH(actor_overlap)/n_actors as p_actor_overlap,
    ARRAY_LENGTH(org_overlap)/n_orgs as p_org_overlap,
    FROM (
        SELECT
            a.*,
            ARRAY_LENGTH(a.events) as n_events,
            ARRAY_LENGTH(a.dates) as n_dates,
            ARRAY_LENGTH(a.actors) as n_actors,
            ARRAY_LENGTH(a.orgs) as n_orgs,
            b.repo as repo2,
            ARRAY(
            SELECT * FROM a.events
            INTERSECT DISTINCT
            SELECT * FROM b.events
            ) AS events_overlap,
            ARRAY(
            SELECT * FROM a.dates
            INTERSECT DISTINCT
            SELECT * FROM b.dates
            ) AS dates_overlap,
            ARRAY(
            SELECT * FROM a.actors
            INTERSECT DISTINCT
            SELECT * FROM b.actors
            ) AS actor_overlap,
            ARRAY(
            SELECT * FROM a.orgs
            INTERSECT DISTINCT
            SELECT * FROM b.orgs
            ) AS org_overlap,
        FROM repo_summary a
        CROSS JOIN repo_summary b
        WHERE a.repo != b.repo
            -- only compare repos w/ 2+ actors in set (avoid memory exceeded error)
            AND ARRAY_LENGTH(a.actors) >= 2
            AND ARRAY_LENGTH(b.actors) >= 2
    )
    WHERE ARRAY_LENGTH(actor_overlap) >= 2 -- limit table size (repos w/ at least 2 actors in common)
)
-- final repo summary table
SELECT * EXCEPT(events_overlap,dates_overlap,actor_overlap,org_overlap),
 -- get DISTINCT values for overlap arrays
 ARRAY(SELECT DISTINCT a FROM a.events_overlap a ) as events_overlap,
 ARRAY(SELECT DISTINCT a FROM a.dates_overlap a ) as dates_overlap,
 ARRAY(SELECT DISTINCT a FROM a.actor_overlap a ) as actor_overlap,
 ARRAY(SELECT DISTINCT a FROM a.org_overlap a ) as org_overlap,
 n/n_actors as actions_per_actor
FROM (
 SELECT
   -- keep all data about original repo (use ANY_VALUE b/c rows are duplicated)
   repo,
   ANY_VALUE(events)events,
   ANY_VALUE(min_activity)min_activity,
   ANY_VALUE(max_activity)max_activity,
   ANY_VALUE(star_time)star_time,
   ANY_VALUE(dates)dates,
   ANY_VALUE(n)n,
   ANY_VALUE(actors)actors,
   ANY_VALUE(orgs)orgs,
   ANY_VALUE(n_events)n_events,
   ANY_VALUE(n_dates)n_dates,
   ANY_VALUE(n_actors)n_actors,
   ANY_VALUE(n_orgs)n_orgs,
   -- collapse overlap data to summarize similarity to other repos in set
   ARRAY_CONCAT_AGG(events_overlap) as events_overlap,
   ARRAY_CONCAT_AGG(dates_overlap) as dates_overlap,
   ARRAY_CONCAT_AGG(actor_overlap) as actor_overlap,
   ARRAY_CONCAT_AGG(org_overlap) as org_overlap,
   AVG(n_events_overlap)n_events_overlap,
   AVG(n_dates_overlap)n_dates_overlap,
   AVG(n_actor_overlap)n_actor_overlap,
   AVG(n_org_overlap)n_org_overlap,
   AVG(p_events_overlap)p_events_overlap,
   AVG(p_dates_overlap)p_dates_overlap,
   AVG(p_actor_overlap)p_actor_overlap,
   AVG(p_org_overlap)p_org_overlap,
 FROM repo_overlap
 GROUP BY 1
 ) a

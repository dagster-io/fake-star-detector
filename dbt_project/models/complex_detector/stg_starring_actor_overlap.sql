-- ## summarize activity per actor & cross join to compare to every other actor in set
-- summary table with 1 row per actor
WITH actor_summary AS (
  SELECT
    actor,
    ARRAY_AGG(DISTINCT event) as events,
    MIN(created_at) as min_activity,
    MAX(created_at) as max_activity,
    MIN(IF(is_target_repo AND is_star, created_at, NULL)) as star_time,
    ARRAY_AGG(DISTINCT DATE(created_at)) dates,
    COUNT(*)n,
    ARRAY_AGG(DISTINCT avatar_url IGNORE NULLS) actor_avatars,
    ARRAY_AGG(DISTINCT repo IGNORE NULLS) repos,
    ARRAY_AGG(DISTINCT IFNULL(org, 'no org')) orgs,
  FROM {{ ref('stg_all_actions_for_actors_who_starred_repo') }}
  GROUP BY 1
),
-- cross join actor_summary to compare each user to each other user in set
-- use nested queries to summarize arrays easily
actor_overlap AS (
 SELECT
   *, -- keep all data about original user + overlap with comparison user
   -- calculate additional stats on overlap arrays
   ARRAY_LENGTH(events_overlap) n_events_overlap,
   ARRAY_LENGTH(dates_overlap) n_dates_overlap,
   ARRAY_LENGTH(repo_overlap) n_repo_overlap,
   ARRAY_LENGTH(org_overlap) n_org_overlap,
   ARRAY_LENGTH(events_overlap)/n_events as p_events_overlap,
   ARRAY_LENGTH(dates_overlap)/n_dates as p_dates_overlap,
   ARRAY_LENGTH(repo_overlap)/n_repos as p_repo_overlap,
   ARRAY_LENGTH(org_overlap)/n_orgs as p_org_overlap,
 FROM (
   SELECT
     a.*, -- keep all data about original user
     -- calculate additional stats for original user
     ARRAY_LENGTH(a.events) as n_events,
     ARRAY_LENGTH(a.dates) as n_dates,
     ARRAY_LENGTH(a.repos) as n_repos,
     ARRAY_LENGTH(a.orgs) as n_orgs,
     b.actor as actor2, -- the comparison user
     -- calculate overlapping events, dates, repos, and orgs
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
       SELECT * FROM a.repos
       INTERSECT DISTINCT
       SELECT * FROM b.repos
     ) AS repo_overlap,
     ARRAY(
       SELECT * FROM a.orgs
       INTERSECT DISTINCT
       SELECT * FROM b.orgs
     ) AS org_overlap,
   FROM actor_summary a
   CROSS JOIN actor_summary b
   WHERE a.actor != b.actor
 )
)
-- final actor summary table: 1 row per actor showing average amount of overlap with other actors in set
SELECT * EXCEPT(events_overlap,dates_overlap,repo_overlap,org_overlap),
 -- get DISTINCT values from overlap arrays
 ARRAY(SELECT DISTINCT a FROM a.events_overlap a ) as events_overlap,
 ARRAY(SELECT DISTINCT a FROM a.dates_overlap a ) as dates_overlap,
 ARRAY(SELECT DISTINCT a FROM a.repo_overlap a ) as repo_overlap,
 ARRAY(SELECT DISTINCT a FROM a.org_overlap a ) as org_overlap,
 n/n_repos as actions_per_repo
FROM (
 SELECT
   -- keep all data about original user (use ANY_VALUE b/c rows are duplicated)
   actor,
   ANY_VALUE(events)events,
   ANY_VALUE(min_activity)min_activity,
   ANY_VALUE(max_activity)max_activity,
   ANY_VALUE(star_time)star_time,
   ANY_VALUE(dates)dates,
   ANY_VALUE(n)n,
   ANY_VALUE(actor_avatars)actor_avatars,
   ANY_VALUE(repos)repos,
   ANY_VALUE(orgs)orgs,
   ANY_VALUE(n_events)n_events,
   ANY_VALUE(n_dates)n_dates,
   ANY_VALUE(n_repos)n_repos,
   ANY_VALUE(n_orgs)n_orgs,
   -- collapse overlap data to summarize similarity to other users in set
   ARRAY_CONCAT_AGG(events_overlap) as events_overlap,
   ARRAY_CONCAT_AGG(dates_overlap) as dates_overlap,
   ARRAY_CONCAT_AGG(repo_overlap) as repo_overlap,
   ARRAY_CONCAT_AGG(org_overlap) as org_overlap,
   AVG(n_events_overlap)n_events_overlap,
   AVG(n_dates_overlap)n_dates_overlap,
   AVG(n_repo_overlap)n_repo_overlap,
   AVG(n_org_overlap)n_org_overlap,
   AVG(p_events_overlap)p_events_overlap,
   AVG(p_dates_overlap)p_dates_overlap,
   AVG(p_repo_overlap)p_repo_overlap,
   AVG(p_org_overlap)p_org_overlap,
 FROM actor_overlap
 GROUP BY 1
 ) a

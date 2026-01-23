{{ config(materialized='table') }}

-- Identify first-time PR contributors and correlate with stargazer status
-- First-time contributors who also starred can be suspicious
SELECT
    pcs.repo,
    pcs.username,
    pcs.pr_count,
    pcs.first_pr_at,
    pcs.is_first_time_contributor,
    pcs.is_stargazer,
    pcs.starred_at,
    pcs.account_created_at,
    pcs.total_additions,
    pcs.total_deletions,
    pcs.avg_comments,
    pcs.followers,
    pcs.following,
    pcs.public_repos,
    -- Suspicious pattern flags
    CASE 
        WHEN pcs.is_first_time_contributor AND pcs.is_stargazer THEN true 
        ELSE false 
    END as is_first_time_and_stargazer,
    -- Check if starred before first PR (could indicate genuine interest)
    CASE 
        WHEN pcs.is_stargazer AND pcs.starred_at < pcs.first_pr_at THEN true 
        ELSE false 
    END as starred_before_first_pr,
    -- Check if starred same day as first PR (more suspicious)
    CASE 
        WHEN pcs.is_stargazer AND DATE(pcs.starred_at) = DATE(pcs.first_pr_at) THEN true 
        ELSE false 
    END as starred_same_day_as_pr,
    -- Low profile indicators
    CASE 
        WHEN COALESCE(pcs.followers, 0) < 2 
            AND COALESCE(pcs.following, 0) < 2 
            AND COALESCE(pcs.public_repos, 0) < 5 
        THEN true 
        ELSE false 
    END as has_low_profile
FROM {{ ref('stg_pr_creators_with_stargazers') }} pcs
WHERE pcs.is_first_time_contributor = true

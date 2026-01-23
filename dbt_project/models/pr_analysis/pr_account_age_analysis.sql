{{ config(materialized='table') }}

-- Analyze account age vs PR activity patterns
-- New accounts creating PRs shortly after creation can be suspicious
SELECT
    pcs.repo,
    pcs.username,
    pcs.pr_count,
    pcs.first_pr_at,
    pcs.account_created_at,
    pcs.is_first_time_contributor,
    pcs.is_stargazer,
    pcs.starred_at,
    pcs.total_additions,
    pcs.total_deletions,
    pcs.avg_comments,
    -- Calculate days between account creation and first PR
    CASE 
        WHEN pcs.account_created_at IS NOT NULL AND pcs.first_pr_at IS NOT NULL 
        THEN DATE_DIFF('day', DATE(pcs.account_created_at), DATE(pcs.first_pr_at))
        ELSE NULL 
    END as days_from_account_to_first_pr,
    -- Flag accounts created recently before first PR
    CASE 
        WHEN pcs.account_created_at IS NOT NULL 
            AND pcs.first_pr_at IS NOT NULL 
            AND DATE_DIFF('day', DATE(pcs.account_created_at), DATE(pcs.first_pr_at)) <= 30 
        THEN true 
        ELSE false 
    END as is_new_account_pr,
    -- Flag accounts created same day as first PR
    CASE 
        WHEN pcs.account_created_at IS NOT NULL 
            AND pcs.first_pr_at IS NOT NULL 
            AND DATE(pcs.account_created_at) = DATE(pcs.first_pr_at) 
        THEN true 
        ELSE false 
    END as is_same_day_account_and_pr,
    -- Flag accounts created after 2022 (more likely to be fake based on heuristic)
    CASE 
        WHEN pcs.account_created_at IS NOT NULL 
            AND pcs.account_created_at > '2022-01-01'::TIMESTAMP 
        THEN true 
        ELSE false 
    END as is_recent_account
FROM {{ ref('stg_pr_creators_with_stargazers') }} pcs
WHERE pcs.account_created_at IS NOT NULL

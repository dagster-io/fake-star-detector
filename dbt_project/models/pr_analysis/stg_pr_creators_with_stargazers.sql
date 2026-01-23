{{ config(materialized='table') }}

-- Join PR creators with stargazer data to identify overlap
-- Users who both starred the repo AND created PRs
SELECT
    pc.repo,
    pc.username,
    pc.pr_count,
    pc.first_pr_at,
    pc.last_pr_at,
    pc.total_additions,
    pc.total_deletions,
    pc.avg_comments,
    pc.is_first_time_contributor,
    -- Stargazer information
    CASE WHEN sg.user IS NOT NULL THEN true ELSE false END as is_stargazer,
    sg.date as starred_at,
    -- User profile information (if available)
    sui.created_at as account_created_at,
    sui.followers,
    sui.following,
    sui.public_repos,
    sui.public_gists,
    sui.bio,
    sui.email,
    sui.company
FROM {{ source('pr_analysis', 'pr_creators') }} pc
LEFT JOIN {{ source('simpler_model', 'stargazers') }} sg
    ON pc.repo = sg.repo AND pc.username = sg.user
LEFT JOIN {{ source('simpler_model', 'stargazers_with_user_info') }} sui
    ON pc.repo = sui.repo AND pc.username = sui.username

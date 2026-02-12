{{ config(materialized='table') }}

-- Join PR creators with stargazer data to identify overlap
-- Users who both starred the repo AND created PRs
-- User profile information comes from pr_creators_with_user_info (all PR creators),
-- with fallback to stargazers_with_user_info for any fields not yet captured.
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
    -- User profile information (prefer pr_creators_with_user_info, fall back to stargazers_with_user_info)
    COALESCE(pui.created_at, sui.created_at) as account_created_at,
    COALESCE(pui.followers, sui.followers) as followers,
    COALESCE(pui.following, sui.following) as following,
    COALESCE(pui.public_repos, sui.public_repos) as public_repos,
    COALESCE(pui.public_gists, sui.public_gists) as public_gists,
    COALESCE(pui.bio, sui.bio) as bio,
    COALESCE(pui.email, sui.email) as email,
    COALESCE(pui.company, sui.company) as company
FROM {{ source('pr_analysis', 'pr_creators') }} pc
LEFT JOIN {{ source('simpler_model', 'stargazers') }} sg
    ON pc.repo = sg.repo AND pc.username = sg.user
LEFT JOIN {{ source('pr_analysis', 'pr_creators_with_user_info') }} pui
    ON pc.repo = pui.repo AND pc.username = pui.username
LEFT JOIN {{ source('simpler_model', 'stargazers_with_user_info') }} sui
    ON pc.repo = sui.repo AND pc.username = sui.username

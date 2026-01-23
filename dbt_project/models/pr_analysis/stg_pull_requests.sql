{{ config(materialized='table') }}

-- Staging model for pull requests with additional derived fields
SELECT
    pr.repo,
    pr.pr_number,
    pr.title,
    pr.state,
    pr.user_login,
    pr.created_at,
    pr.updated_at,
    pr.closed_at,
    pr.merged_at,
    pr.merged,
    pr.merged_by,
    pr.additions,
    pr.deletions,
    pr.changed_files,
    pr.commits,
    pr.comments,
    pr.review_comments,
    -- Derived fields
    COALESCE(pr.additions, 0) + COALESCE(pr.deletions, 0) as total_lines_changed,
    COALESCE(pr.comments, 0) + COALESCE(pr.review_comments, 0) as total_engagement,
    -- Check if this is the user's first PR to this repo
    ROW_NUMBER() OVER (
        PARTITION BY pr.repo, pr.user_login 
        ORDER BY pr.created_at ASC
    ) as pr_sequence_number
FROM {{ source('pr_analysis', 'pull_requests') }} pr
WHERE pr.user_login IS NOT NULL

{{ config(materialized='table') }}

-- Flag PRs with low engagement indicators
-- Low engagement can be suspicious when combined with other signals
SELECT
    pr.repo,
    pr.pr_number,
    pr.user_login,
    pr.created_at,
    pr.merged,
    pr.total_lines_changed,
    pr.total_engagement,
    pr.comments,
    pr.review_comments,
    pr.pr_sequence_number,
    -- Engagement quality flags
    CASE 
        WHEN pr.total_engagement = 0 THEN true 
        ELSE false 
    END as has_zero_engagement,
    CASE 
        WHEN pr.comments = 0 AND pr.review_comments = 0 THEN true 
        ELSE false 
    END as has_no_discussion,
    CASE 
        WHEN pr.total_lines_changed < 10 THEN true 
        ELSE false 
    END as is_minimal_change,
    -- Combined low quality score
    CASE 
        WHEN pr.total_engagement = 0 
            AND pr.total_lines_changed < 10 
            AND pr.pr_sequence_number = 1
        THEN true 
        ELSE false 
    END as is_low_quality_first_pr
FROM {{ ref('stg_pull_requests') }} pr

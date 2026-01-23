{{ config(materialized='table') }}

-- Aggregate summary statistics for PR analysis
-- Combines signals from engagement, first-time contributors, and account age
WITH engagement_stats AS (
    SELECT
        repo,
        COUNT(*) as total_prs,
        SUM(CASE WHEN has_zero_engagement THEN 1 ELSE 0 END) as zero_engagement_prs,
        SUM(CASE WHEN is_minimal_change THEN 1 ELSE 0 END) as minimal_change_prs,
        SUM(CASE WHEN is_low_quality_first_pr THEN 1 ELSE 0 END) as low_quality_first_prs
    FROM {{ ref('pr_engagement_quality') }}
    GROUP BY repo
),

first_time_stats AS (
    SELECT
        repo,
        COUNT(*) as first_time_contributors,
        SUM(CASE WHEN is_first_time_and_stargazer THEN 1 ELSE 0 END) as first_time_and_stargazer_count,
        SUM(CASE WHEN starred_same_day_as_pr THEN 1 ELSE 0 END) as starred_same_day_count,
        SUM(CASE WHEN has_low_profile THEN 1 ELSE 0 END) as low_profile_first_timers
    FROM {{ ref('pr_first_time_contributors') }}
    GROUP BY repo
),

account_age_stats AS (
    SELECT
        repo,
        COUNT(*) as contributors_with_account_info,
        SUM(CASE WHEN is_new_account_pr THEN 1 ELSE 0 END) as new_account_prs,
        SUM(CASE WHEN is_same_day_account_and_pr THEN 1 ELSE 0 END) as same_day_account_prs,
        SUM(CASE WHEN is_recent_account THEN 1 ELSE 0 END) as recent_account_count,
        AVG(days_from_account_to_first_pr) as avg_days_to_first_pr
    FROM {{ ref('pr_account_age_analysis') }}
    GROUP BY repo
)

SELECT
    e.repo,
    -- PR engagement metrics
    e.total_prs,
    e.zero_engagement_prs,
    e.minimal_change_prs,
    e.low_quality_first_prs,
    ROUND(e.zero_engagement_prs * 100.0 / NULLIF(e.total_prs, 0), 2) as pct_zero_engagement,
    -- First-time contributor metrics
    COALESCE(ft.first_time_contributors, 0) as first_time_contributors,
    COALESCE(ft.first_time_and_stargazer_count, 0) as first_time_and_stargazer_count,
    COALESCE(ft.starred_same_day_count, 0) as starred_same_day_count,
    COALESCE(ft.low_profile_first_timers, 0) as low_profile_first_timers,
    -- Account age metrics
    COALESCE(aa.new_account_prs, 0) as new_account_prs,
    COALESCE(aa.same_day_account_prs, 0) as same_day_account_prs,
    COALESCE(aa.recent_account_count, 0) as recent_account_count,
    ROUND(aa.avg_days_to_first_pr, 1) as avg_days_to_first_pr,
    -- Suspicion score (combination of signals)
    COALESCE(ft.first_time_and_stargazer_count, 0) 
        + COALESCE(ft.starred_same_day_count, 0) 
        + COALESCE(aa.same_day_account_prs, 0) 
        + COALESCE(ft.low_profile_first_timers, 0) as suspicious_pr_signals
FROM engagement_stats e
LEFT JOIN first_time_stats ft ON e.repo = ft.repo
LEFT JOIN account_age_stats aa ON e.repo = aa.repo

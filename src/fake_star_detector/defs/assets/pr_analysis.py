"""
Pull Request analysis assets for fake star detection.

This module fetches PR data from repositories and analyzes PR creators
to identify patterns that may indicate suspicious activity.
"""

import datetime

from github import RateLimitExceededException

from dagster import AssetExecutionContext, MetadataValue, ResourceParam, asset
from dagster_duckdb import DuckDBResource

from ..partitions import REPOSITORY_PARTITIONS
from ..resources import GithubAPI


def _save_pr_records_to_db(conn, records: list, context: AssetExecutionContext) -> None:
    """Helper function to save PR records to DuckDB."""
    if not records:
        return
    
    conn.executemany(
        """
        INSERT OR REPLACE INTO simpler_model__pull_requests 
        (repo, pr_number, title, state, user_login, created_at, updated_at, 
         closed_at, merged_at, merged, merged_by, additions, deletions, 
         changed_files, commits, comments, review_comments)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["repo"], r["pr_number"], r["title"], r["state"], r["user_login"],
                r["created_at"], r["updated_at"], r["closed_at"], r["merged_at"],
                r["merged"], r["merged_by"], r["additions"], r["deletions"],
                r["changed_files"], r["commits"], r["comments"], r["review_comments"],
            )
            for r in records
        ],
    )
    context.log.info(f"Saved {len(records)} PR records to database")


@asset(
    compute_kind="GitHub API",
    partitions_def=REPOSITORY_PARTITIONS,
)
def pull_requests(
    context: AssetExecutionContext,
    github: ResourceParam[GithubAPI],
    duckdb: ResourceParam[DuckDBResource],
) -> None:
    """
    Retrieve pull requests created in the past 2 years for a repository from the GitHub API and store in DuckDB.

    This asset is partitioned by repository. Each partition processes one repository.
    Only PRs created within the last 2 years are included to focus on recent activity.

    **GitHub API Docs**:
    * https://pygithub.readthedocs.io/en/latest/github_objects/PullRequest.html
    """
    repo_name = context.partition_key

    # Calculate the cutoff date (2 years ago)
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=730)

    context.log.info(
        f"Starting PR extract for {repo_name} (filtering PRs created after {cutoff_date.date()})"
    )
    try:
        pulls_response = github.get_repo_pulls(repo_name, state="all")
    except Exception as e:
        exception_handling_result = github.handle_exception(e, context)
        if exception_handling_result is None:
            context.log.error(
                f"That repository cannot be found. Please check that {repo_name} is correct."
            )
            raise e
        elif exception_handling_result is False:
            context.log.error(f"An error was encountered: {e}")
            raise e
        else:
            raise e

    # Handle rate limit exceptions during pagination
    max_retries = 5
    retry_count = 0
    pulls_list = None
    
    while retry_count < max_retries:
        try:
            pulls_list = list(pulls_response)
            break
        except RateLimitExceededException as e:
            retry_count += 1
            exception_handling_result = github.handle_exception(e, context)
            if exception_handling_result is False:
                context.log.error(f"Rate limit error cannot be retried: {e}")
                raise e
            elif exception_handling_result is None:
                context.log.error(f"Unexpected exception handling result for rate limit: {e}")
                raise e
            # If exception_handling_result is True, it means we waited and should retry
            # Get a fresh pulls_response for the retry
            context.log.info(f"Retrying PR pagination (attempt {retry_count}/{max_retries})")
            pulls_response = github.get_repo_pulls(repo_name, state="all")
        except Exception as e:
            # For other exceptions during pagination, raise immediately
            raise e
    
    if pulls_list is None:
        raise Exception(f"Failed to fetch PRs after {max_retries} retries")
    
    # Filter PRs to only include those created in the past 2 years
    filtered_pulls = [
        pr for pr in pulls_list
        if pr.created_at and pr.created_at >= cutoff_date
    ]
    
    total_prs = len(pulls_list)
    filtered_count = len(filtered_pulls)
    
    if total_prs > filtered_count:
        context.log.info(
            f"Filtered {total_prs - filtered_count} PRs older than 2 years. "
            f"Keeping {filtered_count} PRs created after {cutoff_date.date()}"
        )
    
    pulls_list = filtered_pulls

    if not pulls_list:
        context.log.info(f"No PRs found for {repo_name}")
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS simpler_model__pull_requests (
                    repo VARCHAR,
                    pr_number INTEGER,
                    title VARCHAR,
                    state VARCHAR,
                    user_login VARCHAR,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    closed_at TIMESTAMP,
                    merged_at TIMESTAMP,
                    merged BOOLEAN,
                    merged_by VARCHAR,
                    additions INTEGER,
                    deletions INTEGER,
                    changed_files INTEGER,
                    commits INTEGER,
                    comments INTEGER,
                    review_comments INTEGER,
                    PRIMARY KEY (repo, pr_number)
                )
            """)
            conn.execute(
                "DELETE FROM simpler_model__pull_requests WHERE repo = ?", (repo_name,)
            )
        context.add_output_metadata({"count": 0, "preview": MetadataValue.md("No PRs found")})
        return

    initial_rate_limit = int(pulls_list[0]._headers.get("x-ratelimit-remaining", 0))
    final_rate_limit = int(pulls_list[-1]._headers.get("x-ratelimit-remaining", 0))
    api_calls_spent = initial_rate_limit - final_rate_limit

    context.log.info(
        f"Completed PR extract for {repo_name} with {len(pulls_list)} PRs. | "
        f"Cost {api_calls_spent} api tokens. {final_rate_limit} tokens remaining."
    )

    BATCH_SIZE = 100  # Save to DB every N PRs

    # Create table if not exists (do this first for incremental saves)
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS simpler_model__pull_requests (
                repo VARCHAR,
                pr_number INTEGER,
                title VARCHAR,
                state VARCHAR,
                user_login VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                closed_at TIMESTAMP,
                merged_at TIMESTAMP,
                merged BOOLEAN,
                merged_by VARCHAR,
                additions INTEGER,
                deletions INTEGER,
                changed_files INTEGER,
                commits INTEGER,
                comments INTEGER,
                review_comments INTEGER,
                PRIMARY KEY (repo, pr_number)
            )
        """)

        # Check which PRs have already been processed
        already_processed = conn.execute(
            "SELECT pr_number FROM simpler_model__pull_requests WHERE repo = ?",
            (repo_name,),
        ).fetchall()
        already_processed_set = {row[0] for row in already_processed}

    # Filter out already processed PRs
    prs_to_process = [pr for pr in pulls_list if pr.number not in already_processed_set]
    
    already_done_count = len(already_processed_set)
    remaining_count = len(prs_to_process)
    total_pr_count = len(pulls_list)

    context.log.info(
        f"Found {total_pr_count} total PRs. "
        f"{already_done_count} already processed, {remaining_count} remaining."
    )

    if remaining_count == 0:
        context.log.info("All PRs already processed. Nothing to do.")
        with duckdb.get_connection() as conn:
            pr_count_result = conn.execute(
                "SELECT COUNT(*) as cnt FROM simpler_model__pull_requests WHERE repo = ?",
                (repo_name,),
            ).fetchone()
            pr_count = pr_count_result[0] if pr_count_result else 0
            pr_preview_df = conn.execute(
                "SELECT repo, pr_number, title, state, user_login, created_at, merged FROM simpler_model__pull_requests WHERE repo = ? LIMIT 5",
                (repo_name,),
            ).fetchdf()
        context.add_output_metadata({
            "count": pr_count,
            "new_this_run": 0,
            "preview": MetadataValue.md(pr_preview_df.to_markdown() if not pr_preview_df.empty else "No data"),
        })
        return

    # Process PRs with incremental saves
    pr_records_batch = []
    processed_count = 0
    error_occurred = False

    try:
        for index, pr in enumerate(prs_to_process):
            try:
                # Handle rate limit exceptions when accessing PR properties
                max_property_retries = 5
                property_retry_count = 0
                pr_record = None
                
                while property_retry_count < max_property_retries:
                    try:
                        pr_record = {
                            "repo": repo_name,
                            "pr_number": pr.number,
                            "title": pr.title,
                            "state": pr.state,
                            "user_login": pr.user.login if pr.user else None,
                            "created_at": pr.created_at.isoformat() if pr.created_at else None,
                            "updated_at": pr.updated_at.isoformat() if pr.updated_at else None,
                            "closed_at": pr.closed_at.isoformat() if pr.closed_at else None,
                            "merged_at": pr.merged_at.isoformat() if pr.merged_at else None,
                            "merged": pr.merged,
                            "merged_by": pr.merged_by.login if pr.merged_by else None,
                            "additions": pr.additions,
                            "deletions": pr.deletions,
                            "changed_files": pr.changed_files,
                            "commits": pr.commits,
                            "comments": pr.comments,
                            "review_comments": pr.review_comments,
                        }
                        break
                    except RateLimitExceededException as e:
                        property_retry_count += 1
                        exception_handling_result = github.handle_exception(e, context)
                        if exception_handling_result is False or exception_handling_result is None:
                            raise e
                        context.log.info(
                            f"Retrying PR property access for PR #{pr.number} "
                            f"(attempt {property_retry_count}/{max_property_retries})"
                        )

                if pr_record is None:
                    context.log.warning(f"Failed to access properties for PR #{pr.number}, skipping")
                    continue

                pr_records_batch.append(pr_record)
                processed_count += 1

                # Save batch periodically
                if len(pr_records_batch) >= BATCH_SIZE:
                    with duckdb.get_connection() as conn:
                        _save_pr_records_to_db(conn, pr_records_batch, context)
                    pr_records_batch = []
                    context.log.info(
                        f"Progress: {processed_count + already_done_count}/{total_pr_count} "
                        f"({processed_count} new this run)"
                    )

            except Exception as e:
                # Save current batch before re-raising
                error_occurred = True
                context.log.error(f"Error processing PR #{pr.number}: {e}")
                if pr_records_batch:
                    context.log.info(f"Saving {len(pr_records_batch)} PR records before error...")
                    with duckdb.get_connection() as conn:
                        _save_pr_records_to_db(conn, pr_records_batch, context)
                raise

    finally:
        # Always save remaining records (even on error or success)
        if pr_records_batch and not error_occurred:
            with duckdb.get_connection() as conn:
                _save_pr_records_to_db(conn, pr_records_batch, context)

    # Get final stats and preview
    with duckdb.get_connection() as conn:
        pr_count_result = conn.execute(
            "SELECT COUNT(*) as cnt FROM simpler_model__pull_requests WHERE repo = ?",
            (repo_name,),
        ).fetchone()
        pr_count = pr_count_result[0] if pr_count_result else 0

        pr_preview_df = conn.execute(
            "SELECT repo, pr_number, title, state, user_login, created_at, merged FROM simpler_model__pull_requests WHERE repo = ? LIMIT 5",
            (repo_name,),
        ).fetchdf()

    context.log.info(f"Completed processing. Total PRs in database: {pr_count}")
    context.add_output_metadata({
        "count": pr_count,
        "new_this_run": processed_count,
        "preview": MetadataValue.md(pr_preview_df.to_markdown() if not pr_preview_df.empty else "No data"),
    })


@asset(
    compute_kind="duckdb",
    partitions_def=REPOSITORY_PARTITIONS,
    deps=["pull_requests"],
)
def pr_creators(
    context: AssetExecutionContext,
    duckdb: ResourceParam[DuckDBResource],
) -> None:
    """
    Extract unique PR creators with their PR activity summary from the pull_requests table.

    This asset aggregates PR data per user to identify:
    - Number of PRs created
    - First and last PR dates
    - Total additions/deletions
    - Average engagement (comments)
    - Whether they are a first-time contributor (only 1 PR)
    """
    repo_name = context.partition_key

    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS simpler_model__pr_creators (
                repo VARCHAR,
                username VARCHAR,
                pr_count INTEGER,
                first_pr_at TIMESTAMP,
                last_pr_at TIMESTAMP,
                total_additions INTEGER,
                total_deletions INTEGER,
                avg_comments DOUBLE,
                is_first_time_contributor BOOLEAN,
                PRIMARY KEY (repo, username)
            )
        """)

        conn.execute(
            "DELETE FROM simpler_model__pr_creators WHERE repo = ?", (repo_name,)
        )

        conn.execute(
            """
            INSERT INTO simpler_model__pr_creators
            SELECT 
                repo,
                user_login as username,
                COUNT(*) as pr_count,
                MIN(created_at) as first_pr_at,
                MAX(created_at) as last_pr_at,
                SUM(COALESCE(additions, 0)) as total_additions,
                SUM(COALESCE(deletions, 0)) as total_deletions,
                AVG(COALESCE(comments, 0) + COALESCE(review_comments, 0)) as avg_comments,
                COUNT(*) = 1 as is_first_time_contributor
            FROM simpler_model__pull_requests
            WHERE repo = ? AND user_login IS NOT NULL
            GROUP BY repo, user_login
        """,
            (repo_name,),
        )

        creator_count_result = conn.execute(
            "SELECT COUNT(*) as cnt FROM simpler_model__pr_creators WHERE repo = ?",
            (repo_name,),
        ).fetchone()
        creator_count = creator_count_result[0] if creator_count_result else 0

        first_time_count_result = conn.execute(
            """
            SELECT COUNT(*) as cnt 
            FROM simpler_model__pr_creators 
            WHERE repo = ? AND is_first_time_contributor = true
        """,
            (repo_name,),
        ).fetchone()
        first_time_count = first_time_count_result[0] if first_time_count_result else 0

        preview_df = conn.execute(
            """
            SELECT * FROM simpler_model__pr_creators 
            WHERE repo = ? 
            ORDER BY pr_count DESC 
            LIMIT 5
        """,
            (repo_name,),
        ).fetchdf()

    context.add_output_metadata(
        {
            "total_creators": creator_count,
            "first_time_contributors": first_time_count,
            "first_time_percentage": round((first_time_count / creator_count * 100), 2)
            if creator_count > 0
            else 0,
            "preview": MetadataValue.md(
                preview_df.to_markdown() if not preview_df.empty else "No data"
            ),
        }
    )

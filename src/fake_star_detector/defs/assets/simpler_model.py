import datetime

from github import GithubException, RateLimitExceededException

from dagster import AssetExecutionContext, MetadataValue, ResourceParam, asset
from dagster_duckdb import DuckDBResource

from ..partitions import REPOSITORY_PARTITIONS
from ..resources import GithubAPI


@asset(
    compute_kind="GitHub API",
    partitions_def=REPOSITORY_PARTITIONS,
)
def stargazers(
    context: AssetExecutionContext,
    github: ResourceParam[GithubAPI],
    duckdb: ResourceParam[DuckDBResource],
) -> None:
    """
    Retrieve a raw list of all users who starred the repo from the GitHub API, and store in DuckDB.

    This asset is partitioned by repository. Each partition processes one repository.

    **GitHub token is required.** Instructions:
    * Go to https://github.com/settings/tokens and generate a personal access token with the `gist` permission.
    * Create a `.env` file with the following contents:
        ```
        GITHUB_ACCESS_TOKEN=ghp_YOUR_TOKEN_HERE
        ```
        For more details, visit https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets.

    **GitHub API Docs**:
    * https://pygithub.readthedocs.io/en/latest/github_objects/Stargazer.html
    """
    repo_name = context.partition_key

    context.log.info(f"Starting extract for {repo_name}")
    try:
        stargazers_response = github.get_repo_stargazers(repo_name)
    except Exception as e:
        exception_handling_result = github.handle_exception(e, context)
        if exception_handling_result is None:
            context.log.error(
                f"That repository cannot be found.  Please check that {repo_name} is correct."
            )
            raise e
        elif exception_handling_result is False:
            context.log.error(f"An error was encountered: {e}")
            raise e
        else:
            raise e

    # Handle rate limit and server errors during pagination
    max_retries = 5
    retry_count = 0
    stargazers_list = None

    while retry_count < max_retries:
        try:
            stargazers_list = list(stargazers_response)
            break
        except (RateLimitExceededException, GithubException) as e:
            retry_count += 1
            exception_handling_result = github.handle_exception(e, context)
            if exception_handling_result is False:
                context.log.error(f"Error cannot be retried: {e}")
                raise e
            elif exception_handling_result is None:
                context.log.error(f"Unexpected exception handling result: {e}")
                raise e
            # If exception_handling_result is True, it means we waited and should retry
            # Get a fresh stargazers_response for the retry
            context.log.info(
                f"Retrying stargazers pagination (attempt {retry_count}/{max_retries})"
            )
            stargazers_response = github.get_repo_stargazers(repo_name)
        except Exception as e:
            # For other exceptions during pagination, raise immediately
            raise e

    if stargazers_list is None:
        raise Exception(f"Failed to fetch stargazers after {max_retries} retries")

    initial_rate_limit = int(stargazers_list[0]._headers["x-ratelimit-remaining"])
    final_rate_limit = int(stargazers_list[-1]._headers["x-ratelimit-remaining"])
    api_calls_spent = initial_rate_limit - final_rate_limit

    context.log.info(
        f"Completed extract for {repo_name} with {len(stargazers_list)} Stargazers. | "
        f"Cost {api_calls_spent} api tokens. {final_rate_limit} tokens remaining."
    )

    # Handle rate limit exceptions when accessing stargazer properties
    stargazer_records_for_insertion = []
    for index, stargazer in enumerate(stargazers_list):
        max_property_retries = 5
        property_retry_count = 0
        stargazer_record = None

        while property_retry_count < max_property_retries:
            try:
                stargazer_record = {
                    "repo": repo_name,
                    "user": stargazer.user.login,
                    "date": stargazer.starred_at.date().isoformat(),
                }
                break
            except (RateLimitExceededException, GithubException) as e:
                property_retry_count += 1
                exception_handling_result = github.handle_exception(e, context)
                if exception_handling_result is False:
                    context.log.error(f"Error cannot be retried for stargazer: {e}")
                    raise e
                elif exception_handling_result is None:
                    context.log.error(f"Unexpected exception handling result: {e}")
                    raise e
                # If exception_handling_result is True, we waited and should retry
                context.log.info(
                    f"Retrying stargazer property access (attempt {property_retry_count}/{max_property_retries})"
                )
            except Exception as e:
                # For other exceptions, raise immediately
                context.log.error(f"Error accessing stargazer properties: {e}")
                raise e

        if stargazer_record is None:
            raise Exception(
                f"Failed to access stargazer properties after {max_property_retries} retries"
            )

        stargazer_records_for_insertion.append(stargazer_record)

        # Log progress every 1000 stargazers
        if (index + 1) % 1000 == 0:
            context.log.info(
                f"Processed {index + 1}/{len(stargazers_list)} stargazers..."
            )

    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS simpler_model__stargazers (
                repo VARCHAR,
                user VARCHAR,
                date DATE,
                PRIMARY KEY (repo, user, date)
            )
        """)

        conn.execute(
            "DELETE FROM simpler_model__stargazers WHERE repo = ?", (repo_name,)
        )

        conn.executemany(
            "INSERT INTO simpler_model__stargazers (repo, user, date) VALUES (?, ?, ?)",
            [
                (record["repo"], record["user"], record["date"])
                for record in stargazer_records_for_insertion
            ],
        )

        stargazer_count_result = conn.execute(
            "SELECT COUNT(*) as cnt FROM simpler_model__stargazers WHERE repo = ?",
            (repo_name,),
        ).fetchone()
        stargazer_count = (
            stargazer_count_result[0]
            if stargazer_count_result
            else len(stargazer_records_for_insertion)
        )

        stargazer_preview_df = conn.execute(
            "SELECT * FROM simpler_model__stargazers WHERE repo = ? LIMIT 5",
            (repo_name,),
        ).fetchdf()

    context.add_output_metadata(
        {
            "count": stargazer_count,
            "preview": MetadataValue.md(
                stargazer_preview_df.to_markdown()
                if not stargazer_preview_df.empty
                else "No data"
            ),
        }
    )


def _save_user_records_to_db(
    conn, records: list, context: AssetExecutionContext
) -> None:
    """Helper function to save user records to DuckDB."""
    if not records:
        return

    conn.executemany(
        """
        INSERT OR REPLACE INTO simpler_model__stargazers_with_user_info 
        (repo, username, starred_at, created_at, updated_at, bio, blog, company, name, email,
         followers, following, location, hireable, public_gists, public_repos,
         starred_url, subscriptions_url, twitter_username, raw_data, url, user_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["repo"],
                r["username"],
                r["starred_at"],
                r["created_at"],
                r["updated_at"],
                r["bio"],
                r["blog"],
                r["company"],
                r["name"],
                r["email"],
                r["followers"],
                r["following"],
                r["location"],
                r["hireable"],
                r["public_gists"],
                r["public_repos"],
                r["starred_url"],
                r["subscriptions_url"],
                r["twitter_username"],
                r["raw_data"],
                r["url"],
                r["user_url"],
            )
            for r in records
        ],
    )
    context.log.info(f"Saved {len(records)} user records to database")


@asset(
    compute_kind="GitHub API",
    partitions_def=REPOSITORY_PARTITIONS,
    deps=["stargazers"],
)
def stargazers_with_user_info(
    context: AssetExecutionContext,
    duckdb: ResourceParam[DuckDBResource],
    github: ResourceParam[GithubAPI],
) -> None:
    """
    Retrieve individual detailed profiles of stargazers from the GitHub API, and store directly in DuckDB.

    This asset is partitioned by repository and processes stargazers for the current partition.
    Supports incremental processing - will skip users already in the database and save progress
    periodically. On error, saves current progress so reruns can resume from where it stopped.

    **GitHub token is required.** Instructions:
    * Go to https://github.com/settings/tokens and generate a personal access token with the `gist` permission.
    * Create a `.env` file with the following contents:
        ```
        GITHUB_ACCESS_TOKEN=ghp_YOUR_TOKEN_HERE
        ```

        For more details, visit https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets.

    **GitHub API Docs**:
    * https://pygithub.readthedocs.io/en/latest/github_objects/NamedUser.html
    """
    repo_name = context.partition_key
    BATCH_SIZE = 100  # Save to DB every N users

    # Create table if not exists (do this first so we can check for existing users)
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS simpler_model__stargazers_with_user_info (
                repo VARCHAR,
                username VARCHAR,
                starred_at DATE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                bio VARCHAR,
                blog VARCHAR,
                company VARCHAR,
                name VARCHAR,
                email VARCHAR,
                followers INTEGER,
                following INTEGER,
                location VARCHAR,
                hireable BOOLEAN,
                public_gists INTEGER,
                public_repos INTEGER,
                starred_url VARCHAR,
                subscriptions_url VARCHAR,
                twitter_username VARCHAR,
                raw_data VARCHAR,
                url VARCHAR,
                user_url VARCHAR,
                PRIMARY KEY (repo, username)
            )
        """)

        # Get list of stargazers to process
        stargazers_result = conn.execute(
            """
            SELECT user, date 
            FROM simpler_model__stargazers 
            WHERE repo = ?
            ORDER BY date
        """,
            (repo_name,),
        ).fetchall()

        # Check which users have already been processed
        already_processed = conn.execute(
            """
            SELECT username FROM simpler_model__stargazers_with_user_info
            WHERE repo = ?
        """,
            (repo_name,),
        ).fetchall()
        already_processed_set = {row[0] for row in already_processed}

    # Filter out already processed users
    users_to_process = [
        (username, date)
        for username, date in stargazers_result
        if username not in already_processed_set
    ]

    total_stargazer_count = len(stargazers_result)
    already_done_count = len(already_processed_set)
    remaining_count = len(users_to_process)

    context.log.info(
        f"Found {total_stargazer_count} total stargazers. "
        f"{already_done_count} already processed, {remaining_count} remaining."
    )

    if remaining_count == 0:
        context.log.info("All users already processed. Nothing to do.")
        with duckdb.get_connection() as conn:
            user_info_preview_df = conn.execute(
                "SELECT * FROM simpler_model__stargazers_with_user_info WHERE repo = ? LIMIT 5",
                (repo_name,),
            ).fetchdf()
        context.add_output_metadata(
            {
                "total_processed": already_done_count,
                "preview": MetadataValue.md(
                    user_info_preview_df.to_markdown()
                    if not user_info_preview_df.empty
                    else "No data"
                ),
            }
        )
        return

    if remaining_count > 4995:
        context.log.info(
            f"The list is {remaining_count} items long, which exceeds the API limit. "
            "This might take a while."
        )

    # Process users with incremental saves
    user_records_batch = []
    processed_count = 0
    error_occurred = False

    try:
        for index, (username, starred_date_value) in enumerate(users_to_process):
            try:
                user_object = github.get_user_details(username, context)
                if not user_object:
                    continue

                # Normalize the starred_at date
                if isinstance(starred_date_value, str):
                    normalized_starred_date = datetime.datetime.strptime(
                        starred_date_value, "%Y-%m-%d"
                    ).date()
                elif isinstance(starred_date_value, datetime.date):
                    normalized_starred_date = starred_date_value
                else:
                    normalized_starred_date = starred_date_value

                # Handle rate limit exceptions when accessing user properties
                max_property_retries = 5
                property_retry_count = 0
                user_record = None

                while property_retry_count < max_property_retries:
                    try:
                        user_record = {
                            "repo": repo_name,
                            "username": user_object.login,
                            "starred_at": normalized_starred_date.isoformat()
                            if hasattr(normalized_starred_date, "isoformat")
                            else str(normalized_starred_date),
                            "created_at": user_object.created_at.isoformat()
                            if user_object.created_at
                            else None,
                            "updated_at": user_object.updated_at.isoformat()
                            if user_object.updated_at
                            else None,
                            "bio": user_object.bio,
                            "blog": user_object.blog,
                            "company": user_object.company,
                            "name": user_object.name,
                            "email": user_object.email,
                            "followers": user_object.followers,
                            "following": user_object.following,
                            "location": user_object.location,
                            "hireable": user_object.hireable,
                            "public_gists": user_object.public_gists,
                            "public_repos": user_object.public_repos,
                            "starred_url": user_object.starred_url,
                            "subscriptions_url": user_object.subscriptions_url,
                            "twitter_username": user_object.twitter_username,
                            "raw_data": str(user_object.raw_data)
                            if user_object.raw_data
                            else None,
                            "url": user_object.url,
                            "user_url": "https://github.com/" + user_object.login,
                        }
                        break
                    except (RateLimitExceededException, GithubException) as e:
                        property_retry_count += 1
                        exception_handling_result = github.handle_exception(e, context)
                        if (
                            exception_handling_result is False
                            or exception_handling_result is None
                        ):
                            raise e
                        context.log.info(
                            f"Retrying user property access for {user_object.login} "
                            f"(attempt {property_retry_count}/{max_property_retries})"
                        )

                if user_record is None:
                    context.log.warning(
                        f"Failed to access properties for user {username}, skipping"
                    )
                    continue

                user_records_batch.append(user_record)
                processed_count += 1

                # Save batch periodically
                if len(user_records_batch) >= BATCH_SIZE:
                    with duckdb.get_connection() as conn:
                        _save_user_records_to_db(conn, user_records_batch, context)
                    user_records_batch = []
                    context.log.info(
                        f"Progress: {processed_count + already_done_count}/{total_stargazer_count} "
                        f"({processed_count} new this run)"
                    )

            except Exception as e:
                # Save current batch before re-raising
                error_occurred = True
                context.log.error(f"Error processing user {username}: {e}")
                if user_records_batch:
                    context.log.info(
                        f"Saving {len(user_records_batch)} records before error..."
                    )
                    with duckdb.get_connection() as conn:
                        _save_user_records_to_db(conn, user_records_batch, context)
                raise

    finally:
        # Always save remaining records (even on error or success)
        if user_records_batch and not error_occurred:
            with duckdb.get_connection() as conn:
                _save_user_records_to_db(conn, user_records_batch, context)

    # Get final stats and preview
    with duckdb.get_connection() as conn:
        final_count_result = conn.execute(
            "SELECT COUNT(*) FROM simpler_model__stargazers_with_user_info WHERE repo = ?",
            (repo_name,),
        ).fetchone()
        final_count = final_count_result[0] if final_count_result else 0

        user_info_preview_df = conn.execute(
            "SELECT * FROM simpler_model__stargazers_with_user_info WHERE repo = ? LIMIT 5",
            (repo_name,),
        ).fetchdf()

    context.log.info(f"Completed processing. Total records in database: {final_count}")
    context.add_output_metadata(
        {
            "total_processed": final_count,
            "new_this_run": processed_count,
            "preview": MetadataValue.md(
                user_info_preview_df.to_markdown()
                if not user_info_preview_df.empty
                else "No data"
            ),
        }
    )


@asset(
    compute_kind="duckdb",
    partitions_def=REPOSITORY_PARTITIONS,
    deps=["stargazers_with_user_info"],
)
def classified_stargazers(
    context: AssetExecutionContext, duckdb: ResourceParam[DuckDBResource]
) -> None:
    """
    Analyze stargazers and classify them as fake or real using SQL logic in DuckDB.

    This asset is partitioned by repository and processes data for the current partition.
    """
    repo_name = context.partition_key

    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS simpler_model__classified_stargazers (
                repo VARCHAR,
                username VARCHAR,
                starred_at DATE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                bio VARCHAR,
                blog VARCHAR,
                company VARCHAR,
                name VARCHAR,
                email VARCHAR,
                followers INTEGER,
                following INTEGER,
                location VARCHAR,
                hireable BOOLEAN,
                public_gists INTEGER,
                public_repos INTEGER,
                starred_url VARCHAR,
                subscriptions_url VARCHAR,
                twitter_username VARCHAR,
                raw_data VARCHAR,
                url VARCHAR,
                user_url VARCHAR,
                matches_fake_heuristic INTEGER,
                PRIMARY KEY (repo, username)
            )
        """)

        conn.execute(
            "DELETE FROM simpler_model__classified_stargazers WHERE repo = ?",
            (repo_name,),
        )

        conn.execute(
            """
            INSERT INTO simpler_model__classified_stargazers
            SELECT 
                repo,
                username,
                starred_at,
                created_at,
                updated_at,
                bio,
                blog,
                company,
                name,
                email,
                followers,
                following,
                location,
                hireable,
                public_gists,
                public_repos,
                starred_url,
                subscriptions_url,
                twitter_username,
                raw_data,
                url,
                user_url,
                CASE 
                    WHEN followers < 2 
                        AND following < 2 
                        AND public_gists = 0 
                        AND public_repos < 5 
                        AND created_at > '2022-01-01'::TIMESTAMP
                        AND email IS NULL
                        AND bio IS NULL
                        AND (blog IS NULL OR blog = '')
                        AND DATE(starred_at) = DATE(updated_at) 
                        AND DATE(starred_at) = DATE(created_at)
                        AND hireable IS NULL
                    THEN 1
                    ELSE 0
                END as matches_fake_heuristic
            FROM simpler_model__stargazers_with_user_info
            WHERE repo = ?
        """,
            (repo_name,),
        )

        # Get preview for metadata (for this repo)
        preview_df = conn.execute(
            "SELECT * FROM simpler_model__classified_stargazers WHERE repo = ? LIMIT 5",
            (repo_name,),
        ).fetchdf()

        # Get fake count (for this repo)
        fake_count_result = conn.execute(
            """
            SELECT COUNT(*) as cnt 
            FROM simpler_model__classified_stargazers 
            WHERE repo = ? AND matches_fake_heuristic = 1
        """,
            (repo_name,),
        ).fetchone()
        fake_count = fake_count_result[0] if fake_count_result else 0

        total_count_result = conn.execute(
            """
            SELECT COUNT(*) as cnt 
            FROM simpler_model__classified_stargazers
            WHERE repo = ?
        """,
            (repo_name,),
        ).fetchone()
        total_count = total_count_result[0] if total_count_result else 0

    # Log metadata for easy debugging
    context.add_output_metadata(
        {
            "preview": MetadataValue.md(
                preview_df.to_markdown() if not preview_df.empty else "No data"
            ),
            "fake_count": fake_count,
            "total_count": total_count,
            "fake_percentage": round((fake_count / total_count * 100), 2)
            if total_count > 0
            else 0,
        }
    )

import calendar
import datetime
import os
import time
from pathlib import Path
from typing import Optional

from dagster import AssetExecutionContext, ConfigurableResource, EnvVar
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from github import Github, GithubException, RateLimitExceededException
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

_current_file_path = Path(__file__).resolve()
_project_root_directory = _current_file_path.parent.parent.parent.parent
DBT_PROJECT_PATH = str(_project_root_directory / "dbt_project")
DBT_PROFILES_PATH = str(_project_root_directory / "dbt_project" / "config")

GMTOFFSET = (calendar.timegm(time.localtime()) - calendar.timegm(time.gmtime())) / 3600


class GithubAPI(ConfigurableResource):
    """GitHub API resource with retry logic and helper methods."""

    access_token: str

    def get_client(self) -> Github:
        """Get the underlying PyGithub client."""
        return Github(login_or_token=self.access_token, retry=3, per_page=100)

    def _wait_with_github_rate_limit(self, state: RetryCallState) -> float:
        """
        Custom wait function that respects GitHub's retry-after header when available.
        Falls back to exponential backoff otherwise.
        """
        exception = state.outcome.exception()

        if isinstance(exception, RateLimitExceededException):
            try:
                retry_after = int(exception.headers.get("retry-after", 0))
                if retry_after > 0:
                    return float(retry_after)
            except (ValueError, AttributeError, TypeError):
                pass

            try:
                rate_limit_reset = exception.headers.get("x-ratelimit-reset")
                if rate_limit_reset:
                    reset_datetime = datetime.datetime.fromtimestamp(
                        int(rate_limit_reset)
                    )
                    retry_delay = reset_datetime - (
                        datetime.datetime.utcnow()
                        + datetime.timedelta(hours=GMTOFFSET)
                    )
                    return max(60.0, retry_delay.total_seconds() + 60)
            except (ValueError, AttributeError, TypeError):
                pass

        return wait_exponential(multiplier=1, min=4, max=60)(state)

    def _api_call_with_retry(self, func):
        """
        Execute a GitHub API call with exponential backoff retry logic.

        Retries up to 4 times (5 total attempts) on rate limit and server errors.
        Uses exponential backoff starting at 4 seconds, doubling each retry, max 60 seconds.
        Respects GitHub's retry-after header when rate limited.
        """
        @retry(
            stop=stop_after_attempt(5),
            wait=self._wait_with_github_rate_limit,
            retry=retry_if_exception_type((RateLimitExceededException, GithubException)),
            reraise=True,
        )
        def _retryable_call():
            return func()

        return _retryable_call()

    def get_repo_stargazers(self, repo_name: str):
        """
        Get stargazers with dates for a repository.

        Args:
            repo_name: Repository name in format "owner/repo"

        Returns:
            PaginatedList of Stargazer objects
        """
        return self._api_call_with_retry(
            lambda: self.get_client().get_repo(repo_name).get_stargazers_with_dates()
        )

    def get_repo_pulls(self, repo_name: str, state: str = "all"):
        """
        Get all pull requests for a repository.

        Args:
            repo_name: Repository name in format "owner/repo"
            state: Filter by PR state - "open", "closed", or "all" (default: "all")

        Returns:
            PaginatedList of PullRequest objects
        """
        return self._api_call_with_retry(
            lambda: self.get_client().get_repo(repo_name).get_pulls(state=state)
        )

    def get_user_details(
        self, username: str, context: Optional[AssetExecutionContext] = None
    ):
        """
        Get user details from GitHub API with retry logic.

        Args:
            username: GitHub username
            context: Optional Dagster context for logging

        Returns:
            NamedUser object if found, None if not found or on non-retryable errors

        Raises:
            Exception: For retryable errors that exceed retry limit
        """
        try:
            user_details = self._api_call_with_retry(
                lambda: self.get_client().get_user(username)
            )
            if context:
                remaining_api_tokens = int(
                    user_details._headers["x-ratelimit-remaining"]
                )
                if remaining_api_tokens % 100 == 0:
                    context.log.info(f"{remaining_api_tokens} tokens left")
            return user_details
        except Exception as e:
            exception_handling_result = self.handle_exception(e, context)
            if exception_handling_result is None:
                if context:
                    context.log.debug(f"User {username} not found.")
                return None
            elif exception_handling_result is False:
                if context:
                    context.log.warning(f"Non-retryable error for user {username}: {e}")
                return None
            else:
                raise e

    def _calculate_retry_delay_for_rate_limit_exception(self, rate_limit_exception):
        """
        Calculate retry delay when a RateLimitExceededException is hit.

        RateLimitExceededException can include either a short term throttle pushback
        or hitting the hourly cap of 5,000 tokens.
        """
        try:
            retry_after_seconds = int(rate_limit_exception.headers["retry-after"])
            return [
                (
                    f"API throttle hit: {rate_limit_exception.data}. "
                    f"Error type {rate_limit_exception.__class__.__name__}. "
                    f"Retry in {retry_after_seconds} seconds."
                ),
                retry_after_seconds,
            ]
        except Exception:
            rate_limit_reset_timestamp = rate_limit_exception.headers.get(
                "x-ratelimit-reset"
            )
            if rate_limit_reset_timestamp is not None:
                reset_datetime = datetime.datetime.fromtimestamp(
                    int(rate_limit_reset_timestamp)
                )
                retry_delay = reset_datetime - (
                    datetime.datetime.utcnow() + datetime.timedelta(hours=GMTOFFSET)
                )
                return [
                    (
                        f"API limit hit: {rate_limit_exception.data}. "
                        f"Error type {rate_limit_exception.__class__.__name__}. "
                        f"Retry in {':'.join(str(retry_delay).split(':')[:2])} "
                        f"[{reset_datetime.time()}]"
                    ),
                    retry_delay.total_seconds() + 60,
                ]
            else:
                return [
                    (
                        f"API issue: {rate_limit_exception.data}. "
                        f"Error type {rate_limit_exception.__class__.__name__}. "
                        "I am going to wait ten minutes while you figure this out."
                    ),
                    600,
                ]

    def handle_exception(
        self, github_exception: Exception, context: Optional[AssetExecutionContext] = None
    ):
        """
        Handle GitHub API exceptions with appropriate retry logic.

        Returns:
            True: Continue retrying (for rate limits and server errors)
            False: Stop processing (for credential/auth errors)
            None: Skip this item (for not found errors)
        """
        exception_class_name = github_exception.__class__.__name__
        log_func = context.log.warning if context else print

        if exception_class_name == "RateLimitExceededException":
            retry_info = self._calculate_retry_delay_for_rate_limit_exception(
                github_exception
            )
            retry_message, retry_delay_seconds = retry_info
            log_func(retry_message)
            log_func(
                f"I am going to wait {round(retry_delay_seconds / 60)} minutes, then continue."
            )
            time.sleep(retry_delay_seconds)
            log_func("done waiting.")
            return True
        elif exception_class_name == "GithubException":
            log_func(
                f"I ran into a server error - I will retry in one minute | Error: {github_exception}"
            )
            time.sleep(60)
            return True
        elif exception_class_name == "UnknownObjectException":
            log_func(
                f"The item requested does not exist on GitHub - I will skip this one | Error: {github_exception}"
            )
            return None
        elif exception_class_name == "BadCredentialsException":
            log_func(f"Your GitHub API credentials failed | Error: {github_exception}")
            time.sleep(300)
            return False
        elif exception_class_name == "TwoFactorException":
            log_func(
                f"Github requires a onetime password for two-factor authentication | Error: {github_exception}"
            )
            time.sleep(600)
            return False
        elif exception_class_name == "BadUserAgentException":
            log_func(
                f"The GitHub raised a bad user agent header error. | Error: {github_exception}"
            )
            time.sleep(300)
            return False
        elif exception_class_name == "BadAttributeException":
            log_func(
                f"Github returned an attribute with the wrong type. | Error: {github_exception}"
            )
            time.sleep(300)
            return False
        elif exception_class_name == "IncompletableObject":
            log_func(
                "Cannot request an object from Github because the data returned did not include a URL."
                f" | Error: {github_exception}"
            )
            time.sleep(300)
            return False
        else:
            log_func(f"I ran into an error - I will skip this one | Error: {github_exception}")
            return False


github_resource = GithubAPI(access_token=EnvVar("GITHUB_ACCESS_TOKEN"))

default_duckdb_database_path = os.getenv(
    "DUCKDB_DATABASE", "data/fake_star_detector.duckdb"
)
duckdb_resource = DuckDBResource(database=default_duckdb_database_path)

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES_PATH,
)

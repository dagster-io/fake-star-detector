import calendar
import datetime
import time

import pandas as pd
from dagster import asset, Field, MetadataValue, OpExecutionContext

GMTOFFSET = (calendar.timegm(time.localtime()) - calendar.timegm(time.gmtime())) / 3600


@asset(
    group_name="stargazers",
    required_resource_keys={"github_api"},
    compute_kind="GitHub API",
    config_schema={
        "name": Field(str, default_value="tap-bls"),
        "repo": Field(str, default_value="frasermarlow/tap-bls"),
    },
)
def raw_stargazers(context: OpExecutionContext) -> list:
    """
    1: Retrieve a raw list of all users who starred the repo from the GitHub API.

    **GitHub token is required.** Instructions:
    * Go to https://github.com/settings/tokens and generate a personal access token with the `gist` permission.
    * Create a `.env` file with the following contents:
        ```
        MY_GITHUB_TOKEN=ghp_YOUR_TOKEN_HERE
        ```
        For more details, visit https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets.

    **Shift+click the "Materialize" button to add configuration**:
    * Specify the repository name in `ops:github_stargazers:config:repo` run config to analyze a repo at your choice.

    **GitHub API Docs**:
    * https://pygithub.readthedocs.io/en/latest/github_objects/Stargazer.html
    """

    name = context.op_config["name"]
    repo_name = context.op_config["repo"]

    context.log.info(f"Starting extract for {repo_name}")
    do_call = True
    while do_call:
        try:
            github_api_call = context.resources.github_api.get_repo(
                repo_name
            ).get_stargazers_with_dates()
            do_call = False
        except Exception as e:
            response = _handle_exception(context, e)
            if response:
                do_call = True
            elif response is None:
                context.log.info(
                    f"That repository cannot be found.  Please check that {repo_name} is correct."
                )
                do_call = True
                exit()
            else:
                context.log.info(f"An error was encountered: {e}")
                do_call = True
                exit()

    starlist = list(github_api_call)

    context.log.info(
        "Completed extract for {name} with {num_total} Stargazers. | Cost {num_calls_spent} api"
        " tokens. {num_calls_left} tokens remaining.".format(
            name=name,
            num_total=len(starlist),
            num_calls_spent=(
                int(starlist[0]._headers["x-ratelimit-remaining"])
                - int(starlist[-1]._headers["x-ratelimit-remaining"])
            ),
            num_calls_left=int(starlist[-1]._headers["x-ratelimit-remaining"]),
        )
    )

    # Log metadata for easy debugging
    context.add_output_metadata({"raw_count": len(starlist)})

    return starlist


@asset(group_name="stargazers", compute_kind="pandas")
def stargazer_names_df(context: OpExecutionContext, raw_stargazers: list) -> pd.DataFrame:
    """
    2: Create clean list of stargazers
    """
    sg_df = pd.DataFrame(
        [
            {
                "user": stargazer.user.login,
                "date": stargazer.starred_at.date(),
            }
            for stargazer in raw_stargazers
        ]
    )
    context.log.info(f"Total stargazers found: {len(sg_df.index)}")
    # Log metadata for easy debugging
    context.add_output_metadata({"preview": MetadataValue.md(sg_df.head().to_markdown())})

    return sg_df


@asset(
    group_name="stargazers",
    compute_kind="GitHub API",
    required_resource_keys={"github_api"},
)
def stargazers_with_user_info(context: OpExecutionContext, stargazer_names_df: pd.DataFrame) -> list:
    """
    3: Retrieve individual detailed profiles of stargazers from the GitHub API.


    **GitHub token is required.** Instructions:
    * Go to https://github.com/settings/tokens and generate a personal access token with the `gist` permission.
    * Create a `.env` file with the following contents:
        ```
        MY_GITHUB_TOKEN=ghp_YOUR_TOKEN_HERE
        ```

        For more details, visit https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets.

    **GitHub API Docs**:
    * https://pygithub.readthedocs.io/en/latest/github_objects/NamedUser.html
    """
    allUsersObjs = []
    stargazer_names_df.sort_values(by=["date"], inplace=True)
    if len(stargazer_names_df.index) > 4995:
        context.log.info(
            f"The list is {len(stargazer_names_df)} items long, which exceeds the API limit."
            "  So this might take a while."
        )

    for _, stargazer in stargazer_names_df.iterrows():
        usrObj = _see_if_user_exists(context, stargazer["user"])
        if usrObj:
            setattr(usrObj, "starred_at", stargazer["date"])
            allUsersObjs.append(usrObj)
        else:
            continue
    return allUsersObjs


@asset(group_name="stargazers", compute_kind="pandas")
def classified_stargazers_df(context: OpExecutionContext, stargazers_with_user_info: list) -> pd.DataFrame:
    """
    4: Buildout dataframe of valuable attributes for these stargazers and analyze.
    """
    starers_dataframe = pd.DataFrame(
        [
            {
                "username": user_obj.login,
                "starred_at": user_obj.starred_at,
                "created_at": user_obj.created_at,
                "updated_at": user_obj.updated_at,
                "bio": user_obj.bio,
                "blog": user_obj.blog,
                "company": user_obj.company,
                "name": user_obj.name,
                "email": user_obj.email,
                "followers": user_obj.followers,
                "following": user_obj.following,
                "location": user_obj.location,
                "hireable": user_obj.hireable,
                "public_gists": user_obj.public_gists,
                "public_repos": user_obj.public_repos,
                "starred_url": user_obj.starred_url,
                "subscriptions_url": user_obj.subscriptions_url,
                "twitter_username": user_obj.twitter_username,
                "raw_data": user_obj.raw_data,
                "url": user_obj.url,
                "user_url": "https://github.com/" + user_obj.login,
            }
            for user_obj in stargazers_with_user_info
        ]
    )
    starers_dataframe["matches_fake_heuristic"] = starers_dataframe.apply(_validate_star, axis=1)
    context.log.info(
        f"Score is {round(starers_dataframe['matches_fake_heuristic'].mean()*100,2)}% fake stars"
    )
    fake_stars = starers_dataframe.query("matches_fake_heuristic == 1")["username"]
    context.log.info(
        f"{len(fake_stars.values.tolist())} suspect profiles found - here is the list:"
        f" {fake_stars.values.tolist()}"
    )
    return starers_dataframe

def _validate_star(row: pd.DataFrame) -> int:
    # Checks this profile against the criteria gleaned form the 100 fake profiles.
    # Returns True if this matches a fake profile.
    if (
        (row["followers"] < 2)
        and (row["following"] < 2)
        and (row["public_gists"] == 0)
        and (row["public_repos"] < 5)
        and (row["created_at"] > datetime.date(2022, 1, 1))
        and (row["email"] == None)
        and (row["bio"] == None)
        and (not row["blog"])
        and (row["starred_at"] == row["updated_at"].date() == row["created_at"].date())
        and not isinstance(row["hireable"], bool)
    ):
        return 1
    else:
        return 0


def _see_if_user_exists(context: OpExecutionContext, user: str):
    while True:
        try:
            userDetails = context.resources.github_api.get_user(
                user
            )  # i.e. NamedUser(login="bfgray3")
            tokensRemaining = int(userDetails._headers["x-ratelimit-remaining"])
            if tokensRemaining % 500 == 0:
                context.log.info(f"{tokensRemaining} tokens left")
        except Exception as e:
            response = _handle_exception(e, context)
            if response:
                continue
            elif response is None:
                context.log.info(f"User {user} not found.")
                return False
            else:
                return False
        return userDetails


def _get_retry_at(e):
    """
    Is called when a RateLimitExceededException is hit.
    RateLimitExceededException can include either a short term throttle pushback or hitting the hourly cap of 5,000 tokens.
    """
    try:
        retry_after = int(e.headers["retry-after"])
        return [
            (
                f"API throttle hit: {e.data}. Error type {e.__class__.__name__}. Retry in"
                f" {retry_after} seconds."
            ),
            retry_after,
        ]
    except Exception as exc:
        x_rate_limit_reset = e.headers["x-ratelimit-reset"]
        if x_rate_limit_reset != None:
            # TODO: x-ratelimit-reset may not exist (i.e. for Secondary rate limits)
            reset_at = datetime.datetime.fromtimestamp(int(x_rate_limit_reset))
            retry_at = reset_at - (datetime.datetime.utcnow() + datetime.timedelta(hours=GMTOFFSET))
            return [
                (
                    f"API limit hit: {e.data}. Error type {e.__class__.__name__}. Retry in"
                    f" {':'.join(str(retry_at).split(':')[:2])} [{reset_at.time()}]"
                ),
                retry_at.total_seconds() + 60,
            ]
        else:
            return [
                (
                    f"API issue: {e.data}. Error type {e.__class__.__name__}. I am going to wait"
                    " ten minutes while you figure this out."
                ),
                600,
            ]


def _handle_exception(context: OpExecutionContext, e: Exception):
    """
    Given a GitHub error, wait until the reset time or otherwise return the handling pattern.
    Note this is used in a 'while True' loop so 'continue' restarts the loop iteration, whereas 'False' movest to the next item in the 'for' loop.
    :param e:  error from GitHub
    :return: True for 'continue', False for 'False', None for 'unkown'
    """
    if e.__class__.__name__ == "RateLimitExceededException":  # API rate limit reached
        context.log.info(_get_retry_at(e)[0])
        context.log.info(
            f"I am going to wait {round(_get_retry_at(e)[1] / 60)} minutes, then continue."
        )
        time.sleep(_get_retry_at(e)[1])
        context.log.info(f"done waiting.")
        return True
    elif e.__class__.__name__ == "ConnectionError":  # API not connecting
        context.log.info(
            f"I cannot connect to the API. Please check your network connection.  Pausing for 10"
            f" mins."
        )
        time.sleep(600)
        return True
    elif e.__class__.__name__ == "GithubException":
        context.log.info(f"I ran into a server error - I will rety in one minute | Error: {e}")
        time.sleep(60)
        return True
    elif e.__class__.__name__ == "UnknownObjectException":  # User not found
        context.log.info(
            f"The item requested does not exist on GitHub - I will skip this one | Error: {e}"
        )
        return None
    elif e.__class__.__name__ == "BadCredentialsException":
        context.log.info(f"Your GitHub API credentials failed | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "TwoFactorException":
        context.log.info(
            f"Github requires a onetime password for two-factor authentication | Error: {e}"
        )
        time.sleep(600)
        return False
    elif e.__class__.__name__ == "BadUserAgentException":
        context.log.info(f"The GitHub raised a bad user agent header error. | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "BadAttributeException":
        context.log.info(f"Github returned an attribute with the wrong type. | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "IncompletableObject":
        context.log.info(
            "Cannot request an object from Github because the data returned did not include a URL."
            f" | Error: {e}"
        )
        time.sleep(300)
        return False
    else:
        context.log.info(f"I ran into an error - I will skip this one | Error: {e}")
        return False

import calendar
import datetime
import pickle
import time

import jupytext
import nbformat
import pandas as pd
from github import InputFileContent
from nbconvert.preprocessors import ExecutePreprocessor

from dagster import MetadataValue, OpExecutionContext, asset, Config

GMTOFFSET = (calendar.timegm(time.localtime()) - calendar.timegm(time.gmtime())) / 3600

class StargazerConfig(Config):
    repo: str = "frasermarlow/tap-bls"

@asset(
    required_resource_keys={"github"},
    compute_kind="GitHub API",
)
def stargazers(context: OpExecutionContext, config: StargazerConfig) -> pd.DataFrame:
    """
    Retrieve a raw list of all users who starred the repo from the GitHub API, and convert to
    a Pandas DataFrame.

    **GitHub token is required.** Instructions:
    * Go to https://github.com/settings/tokens and generate a personal access token with the `gist` permission.
    * Create a `.env` file with the following contents:
        ```
        GITHUB_ACCESS_TOKEN=ghp_YOUR_TOKEN_HERE
        ```
        For more details, visit https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets.

    **Shift+click the "Materialize" button to add configuration**:
    * Specify the repository name in `ops:raw_stargazers:config:repo` run config to analyze a repo at your choice.

    **GitHub API Docs**:
    * https://pygithub.readthedocs.io/en/latest/github_objects/Stargazer.html
    """
    repo_name = config.repo

    context.log.info(f"Starting extract for {repo_name}")
    do_call = True
    while do_call:
        try:
            response = context.resources.github.get_client().get_repo(
                repo_name
            ).get_stargazers_with_dates()
            do_call = False
        except Exception as e:
            response = _handle_exception(e)
            if response:
                do_call = True
            elif response is None:
                context.log.error(
                    f"That repository cannot be found.  Please check that {repo_name} is correct."
                )
                raise e
            else:
                context.log.error(f"An error was encountered: {e}")
                raise e

    starlist = list(response)

    context.log.info(
        "Completed extract for {name} with {num_total} Stargazers. | Cost {num_calls_spent} api"
        " tokens. {num_calls_left} tokens remaining.".format(
            name=repo_name,
            num_total=len(starlist),
            num_calls_spent=(
                int(starlist[0]._headers["x-ratelimit-remaining"])
                - int(starlist[-1]._headers["x-ratelimit-remaining"])
            ),
            num_calls_left=int(starlist[-1]._headers["x-ratelimit-remaining"]),
        )
    )

    sg_df = pd.DataFrame(
        [
            {
                "user": stargazer.user.login,
                "date": stargazer.starred_at.date(),
            }
            for stargazer in starlist
        ]
    )
    # Log metadata for easy debugging
    context.add_output_metadata({
        "count": len(sg_df.index),
        "preview": MetadataValue.md(sg_df.head().to_markdown()),
    })

    return sg_df


@asset(
    compute_kind="GitHub API",
    required_resource_keys={"github"},
)
def stargazers_with_user_info(
    context: OpExecutionContext, stargazers: pd.DataFrame
) -> pd.DataFrame:
    """
    Retrieve individual detailed profiles of stargazers from the GitHub API, and convert to a
    Pandas DataFrame.


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
    allUsersObjs = []
    stargazers.sort_values(by=["date"], inplace=True)
    if len(stargazers.index) > 4995:
        context.log.info(
            f"The list is {len(stargazers)} items long, which exceeds the API limit."
            "  So this might take a while."
        )

    for i, stargazer in stargazers.iterrows():
        usrObj = _see_if_user_exists(context, stargazer["user"])
        if usrObj:
            setattr(usrObj, "starred_at", stargazer["date"])
            allUsersObjs.append(usrObj)
        else:
            continue

        if i % 100 == 0:
            context.log.debug(
                f"Completed {i} of {len(stargazers.index)} stargazers."
            )

    df = pd.DataFrame(
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
            for user_obj in allUsersObjs
        ]
    )

    # Log metadata for easy debugging
    context.add_output_metadata({
        "preview": MetadataValue.md(df.head().to_markdown()),
    })
    return df


@asset(compute_kind="pandas")
def classified_stargazers_df(
    context: OpExecutionContext, stargazers_with_user_info: pd.DataFrame
) -> pd.DataFrame:
    """
    Buildout dataframe of valuable attributes for these stargazers and analyze.
    """
    stargazers_with_user_info["matches_fake_heuristic"] = stargazers_with_user_info.apply(
        _validate_star,
        axis=1,
    )

    # Log metadata for easy debugging
    context.add_output_metadata({
        "preview": MetadataValue.md(stargazers_with_user_info.head().to_markdown()),
    })
    return stargazers_with_user_info


def _validate_star(row: pd.DataFrame) -> int:
    # Checks this profile against the criteria gleaned form the 100 fake profiles.
    # Returns True if this matches a fake profile.
    if (
        (row["followers"] < 2)
        and (row["following"] < 2)
        and (row["public_gists"] == 0)
        and (row["public_repos"] < 5)
        and (row["created_at"] > pd.Timestamp(datetime.date(2022, 1, 1)))
        and (row["email"] is None)
        and (row["bio"] is None)
        and (not row["blog"])
        and (row["starred_at"] == row["updated_at"].date() == row["created_at"].date())
        and not isinstance(row["hireable"], bool)
    ):
        return 1
    else:
        return 0


@asset(compute_kind="Notebook")
def real_vs_raw_stars_report(classified_stargazers_df: pd.DataFrame) -> str:
    """
    Jupyter notebook with github star plots.
    """
    markdown = f"""
# Github Stars

```python
import matplotlib.pyplot as plt
import pandas as pd
import pickle
classified_stargazers_df = pickle.loads({pickle.dumps(classified_stargazers_df)!r})
```

## Real stars, real users
```python
classified_stargazers_df["date"] = pd.to_datetime(classified_stargazers_df["starred_at"]).dt.date
real_stargazers = classified_stargazers_df.loc[classified_stargazers_df['matches_fake_heuristic'] == 0]
real_stars_by_date = real_stargazers[["date"]].groupby("date").size().reset_index(name="count").sort_values(["date"])
real_stars_by_date["total_github_stars"] = real_stars_by_date["count"].cumsum()
```

## Raw stars, all users including fake/bot accounts
```python
raw_stars_by_date = classified_stargazers_df[["date"]].groupby("date").size().reset_index(name="count").sort_values(["date"])
raw_stars_by_date["total_github_stars"] = raw_stars_by_date["count"].cumsum()
```


## Real vs Raw stars
```python
ax = raw_stars_by_date.sort_values('date').plot(label="all stars", x="date", y="total_github_stars")
real_stars_by_date.sort_values('date').plot(label="real stars", x="date", y="total_github_stars", ax=ax)
plt.show()
```

    """
    nb = jupytext.reads(markdown, "md")
    ExecutePreprocessor().preprocess(nb)
    return nbformat.writes(nb)


@asset(
    compute_kind="Gist",
    required_resource_keys={"github"},
)
def github_stars_notebook_gist(context: OpExecutionContext, real_vs_raw_stars_report: str) -> str:
    """
    Share the notebook as a GitHub Gist.

    **GitHub token is required by this asset.** Instructions:
    * Go to https://github.com/settings/tokens and generate a personal access token with the `gist` permission.
    * Create a `.env` file with the following contents:
        ```
        GITHUB_ACCESS_TOKEN=ghp_YOUR_TOKEN_HERE
        ```

        For more details, visit https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets.


    **GitHub API Docs**:
    * https://pygithub.readthedocs.io/en/latest/github_objects/AuthenticatedUser.html?highlight=create_gist#github.AuthenticatedUser.AuthenticatedUser.create_gist
    """
    gist = context.resources.github.get_client().get_user().create_gist(
        public=False,
        files={
            "github_stars.ipynb": InputFileContent(real_vs_raw_stars_report),
        },
    )
    context.log.info(f"Notebook created at {gist.html_url}")

    # Log gist url as a clickable link in the UI
    context.add_output_metadata({"gist_url": MetadataValue.url(gist.html_url)})
    return gist.html_url

def _see_if_user_exists(context: OpExecutionContext, user: str):
    while True:
        try:
            userDetails = context.resources.github.get_client().get_user(
                user
            )  # i.e. NamedUser(login="bfgray3")
            tokensRemaining = int(userDetails._headers["x-ratelimit-remaining"])
            if tokensRemaining % 100 == 0:
                context.log.info(f"{tokensRemaining} tokens left")
        except Exception as e:
            response = _handle_exception(e)
            if response:
                continue
            elif response is None:
                print(f"User {user} not found.")
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
    except Exception:
        x_rate_limit_reset = e.headers["x-ratelimit-reset"]
        if x_rate_limit_reset is not None:
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


def _handle_exception(e: Exception):
    """
    Given a GitHub error, wait until the reset time or otherwise return the handling pattern.
    Note this is used in a 'while True' loop so 'continue' restarts the loop iteration, whereas 'False' movest to the next item in the 'for' loop.
    :param e:  error from GitHub
    :return: True for 'continue', False for 'False', None for 'unkown'
    """
    if e.__class__.__name__ == "RateLimitExceededException":  # API rate limit reached
        print(_get_retry_at(e)[0])
        print(
            f"I am going to wait {round(_get_retry_at(e)[1] / 60)} minutes, then continue."
        )
        time.sleep(_get_retry_at(e)[1])
        print("done waiting.")
        return True
    elif e.__class__.__name__ == "GithubException":
        print(f"I ran into a server error - I will rety in one minute | Error: {e}")
        time.sleep(60)
        return True
    elif e.__class__.__name__ == "UnknownObjectException":  # User not found
        print(
            f"The item requested does not exist on GitHub - I will skip this one | Error: {e}"
        )
        return None
    elif e.__class__.__name__ == "BadCredentialsException":
        print(f"Your GitHub API credentials failed | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "TwoFactorException":
        print(
            f"Github requires a onetime password for two-factor authentication | Error: {e}"
        )
        time.sleep(600)
        return False
    elif e.__class__.__name__ == "BadUserAgentException":
        print(f"The GitHub raised a bad user agent header error. | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "BadAttributeException":
        print(f"Github returned an attribute with the wrong type. | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "IncompletableObject":
        print(
            "Cannot request an object from Github because the data returned did not include a URL."
            f" | Error: {e}"
        )
        time.sleep(300)
        return False
    else:
        print(f"I ran into an error - I will skip this one | Error: {e}")
        return False

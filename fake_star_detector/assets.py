import calendar
import datetime
import time

import pandas as pd
from dagster import asset, Field, OpExecutionContext

GMTOFFSET = (calendar.timegm(time.localtime()) -
             calendar.timegm(time.gmtime()))/3600


@asset(group_name="stargazers", required_resource_keys={"github_api"}, compute_kind="github api", config_schema={"name": Field(str, default_value="tap-bls"), "repo": Field(str, default_value="frasermarlow/tap-bls")})
def stargazers(context: OpExecutionContext) -> list:
    """
    1: Retrieve a raw list of all users who starred the repo
    """

    name = context.op_config['name']
    repo_name = context.op_config['repo']

    context.log.info(f"Starting extract for {repo_name}")
    do_call = True
    while do_call:
        try:
            github_api_call = context.resources.github_api.get_repo(
                repo_name).get_stargazers_with_dates()
            do_call = False
        except Exception as e:
            response = _handle_exception(context, e)
            if response:
                do_call = True
            elif response is None:
                context.log.info(
                    f"That repository cannot be found.  Please check that {repo_name} is correct.")
                do_call = True
                exit()
            else:
                context.log.info(f"An error was encountered: {e}")
                do_call = True
                exit()

    starlist = list(github_api_call)

    context.log.info(
        f"Completed extract for {name} with {len(starlist)} Stargazers. | Cost {int(starlist[0]._headers['x-ratelimit-remaining'])-int(starlist[-1]._headers['x-ratelimit-remaining'])} api tokens. {int(starlist[-1]._headers['x-ratelimit-remaining'])} tokens remaining.")
    return starlist


@asset(group_name="stargazers", compute_kind="pandas")
def this_repo_stargazers_list(
        context,
        stargazers: list) -> pd.DataFrame:
    """
    2: Create clean list of stargazers
    """
    this_repo_sg_df = pd.DataFrame(
        [
            {
                "user": stargazer.user.login,
                # "user-private-repos": stargazer.user.total_private_repos,
                "date": stargazer.starred_at.date()
            }
            for stargazer in stargazers
        ]
    )
    context.log.info(f"Total stargazers found: {len(this_repo_sg_df.index)}")
    return this_repo_sg_df


@asset(required_resource_keys={"github_api"}, group_name="stargazers", compute_kind="github api")
def this_repo_gh_stars(context, this_repo_stargazers_list: pd.DataFrame) -> list:
    """
    3: Retrieve individual detailed profiles of stargazers
    """
    allUsersObjs = []
    this_repo_stargazers_list.sort_values(by=['date'], inplace=True)
    if len(this_repo_stargazers_list.index) > 4995:
        context.log.info(
            f"The list is {len(this_repo_stargazers_list)} items long, which exceeds the API limit.  So this might take a while.")

    for index, stargazer in this_repo_stargazers_list.iterrows():
        usrObj = _see_if_user_exists(context, stargazer['user'])
        if usrObj:
            setattr(usrObj, 'starred_at', stargazer['date'])
            allUsersObjs.append(usrObj)
        else:
            continue
    return allUsersObjs


@asset(group_name="stargazers", compute_kind="pandas")
def stargazers_dataframe(context, this_repo_gh_stars: list) -> pd.DataFrame:
    """
    4: Buildout dataframe of valuable attributes for these stargazers and analyze
    :param context:
    :param stargazers:
    :return:
    """
    starers_dataframe = pd.DataFrame(
        [
            {
                "username":             usrObj.login,
                "starred_at":           usrObj.starred_at,
                "created_at":           usrObj.created_at,
                "updated_at":           usrObj.updated_at,
                "bio":                  usrObj.bio,
                "blog":                 usrObj.blog,
                "company":              usrObj.company,
                "name":                 usrObj.name,
                "email":                usrObj.email,
                "followers":            usrObj.followers,
                "following":            usrObj.following,
                "location":             usrObj.location,
                "hireable":             usrObj.hireable,
                "public_gists":         usrObj.public_gists,
                "public_repos":         usrObj.public_repos,
                "starred_url":          usrObj.starred_url,
                "subscriptions_url":    usrObj.subscriptions_url,
                "twitter_username":     usrObj.twitter_username,
                "raw_data":             usrObj.raw_data,
                "url": usrObj.url,
                "user_url": "https://github.com/" + usrObj.login,
            }
            for usrObj in this_repo_gh_stars
        ]
    )
    starers_dataframe['matches_fake_heuristic'] = starers_dataframe.apply(
        _validate_star, axis=1)
    context.log.info(
        f"Score is {round(starers_dataframe['matches_fake_heuristic'].mean()*100,2)}% fake stars")
    fakeStars = starers_dataframe.query(
        'matches_fake_heuristic == 1')['username']
    context.log.info(
        f"{len(fakeStars.values.tolist())} suspect profiles found - here is the list: {fakeStars.values.tolist()}")
    return starers_dataframe


def _validate_star(row) -> int:
    # Checks this profile against the criteria gleaned form the 100 fake profiles.
    # Returns True if this matches a fake profile.
    if (row['followers'] < 2) \
            and (row['following'] < 2) \
            and (row['public_gists'] == 0) \
            and (row['public_repos'] < 5) \
            and (row['created_at'] > datetime.date(2022, 1, 1)) \
            and (row['email'] == None) \
            and (row['bio'] == None) \
            and (not row['blog']) \
            and (row['starred_at'] == row['updated_at'].date() == row['created_at'].date()) \
            and not isinstance(row['hireable'], bool):
        return 1
    else:
        return 0


def _see_if_user_exists(context, user):
    while True:
        try:
            userDetails = context.resources.github_api.get_user(
                user)  # i.e. NamedUser(login="bfgray3")
            tokensRemaining = int(
                userDetails._headers["x-ratelimit-remaining"])
            if (tokensRemaining % 500 == 0):
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
        return [f"API throttle hit: {e.data}. Error type {e.__class__.__name__}. Retry in {retry_after} seconds.", retry_after]
    except Exception as exc:
        xRateLimitReset = e.headers["x-ratelimit-reset"]
        if xRateLimitReset != None:
            # TODO: x-ratelimit-reset may not exist (i.e. for Secondary rate limits)
            reset_at = datetime.datetime.fromtimestamp(int(xRateLimitReset))
            retry_at = reset_at - \
                (datetime.datetime.utcnow() + datetime.timedelta(hours=GMTOFFSET))
            return [f"API limit hit: {e.data}. Error type {e.__class__.__name__}. Retry in {':'.join(str(retry_at).split(':')[:2])} [{reset_at.time()}]", retry_at.total_seconds()+60]
        else:
            return [f"API issue: {e.data}. Error type {e.__class__.__name__}. I am going to wait ten minutes while you figure this out.", 600]


def _handle_exception(passedContext, e):
    """
    Given a GitHub error, wait until the reset time or otherwise return the handling pattern.
    Note this is used in a 'while True' loop so 'continue' restarts the loop iteration, whereas 'False' movest to the next item in the 'for' loop.
    :param e:  error from GitHub
    :return: True for 'continue', False for 'False', None for 'unkown'
    """
    if e.__class__.__name__ == "RateLimitExceededException":  # API rate limit reached
        passedContext.log.info(_get_retry_at(e)[0])
        passedContext.log.info(
            f"I am going to wait {round(_get_retry_at(e)[1] / 60)} minutes, then continue.")
        time.sleep(_get_retry_at(e)[1])
        passedContext.log.info(f"done waiting.")
        return True
    elif e.__class__.__name__ == "ConnectionError":  # API not connecting
        passedContext.log.info(
            f"I cannot connect to the API. Please check your network connection.  Pausing for 10 mins.")
        time.sleep(600)
        return True
    elif e.__class__.__name__ == "GithubException":
        passedContext.log.info(
            f"I ran into a server error - I will rety in one minute | Error: {e}")
        time.sleep(60)
        return True
    elif e.__class__.__name__ == "UnknownObjectException":  # User not found
        passedContext.log.info(
            f"The item requested does not exist on GitHub - I will skip this one | Error: {e}")
        return None
    elif e.__class__.__name__ == "BadCredentialsException":
        passedContext.log.info(
            f"Your GitHub API credentials failed | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "TwoFactorException":
        passedContext.log.info(
            f"Github requires a onetime password for two-factor authentication | Error: {e}")
        time.sleep(600)
        return False
    elif e.__class__.__name__ == "BadUserAgentException":
        passedContext.log.info(
            f"The GitHub raised a bad user agent header error. | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "BadAttributeException":
        passedContext.log.info(
            f"Github returned an attribute with the wrong type. | Error: {e}")
        time.sleep(300)
        return False
    elif e.__class__.__name__ == "IncompletableObject":
        passedContext.log.info(
            f"Cannot request an object from Github because the data returned did not include a URL. | Error: {e}")
        time.sleep(300)
        return False
    else:
        passedContext.log.info(
            f"I ran into an error - I will skip this one | Error: {e}")
        return False

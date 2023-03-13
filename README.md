# fake-star-detector: A Dagster tutorial

This is a simple Dagster project to analyze the number of fake GitHub stars on any GitHub repository.  It is a companion to the blogpost found [on the Dagster blog](https:dagster.io/blog).

## Getting started

### Install instructions

Build a virtual environment
```commandline
python3 -m venv venv
source venv/bin/activate
```
Install Dagster and our other dependencies - see https://docs.dagster.io/getting-started/install
Note for M1 Mac users you may need to use `pip install dagster dagit --find-links=https://github.com/dagster-io/build-grpcio/wiki/Wheels`

```commandline
pip install -e ".[dev]"
```

Next, create a `.env` file and add the required environment variables:
```
GITHUB_ACCESS_TOKEN=<<GITHUB_ACCESS_TOKEN>>
```

Start the Dagster UI web server:

```commandline
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Specifying the repo to analyze

You can Shift+click "Materialize all" on the asset graph page, and specify the repository you want to analyze in the configuration, such as:
<img width="600" alt="asset config" src="https://user-images.githubusercontent.com/4531914/219830488-7f783e01-a4f9-4691-9421-13c7c7431b93.png">


## Explanation of the repo

This repo is a Dagster project and involves 4 assets:

1) Asset `stargazers`: We call the GitHub API and retrieve a list of users who have starred the repo at the top of the file.
2) Asset `this_repo_stargazers_list`: We turn the GItHub response into a Python list of usernames
3) Asset `this_repo_gh_stars`: We look up each user in turn and pull their detailed profile
4) Asset `stargazers_dataframe`: We analyze each profile and match to our heuristic to determine if they are fake or not

In addition to the above, we have ops:

a) `validate_star`: Matching a profile against the heuristic
b) `see_if_user_exists`: Verifying that a user still exists before pulling the full details
c) `handle_exception`: Handling exceptions for the GitHub API call.  This op calls on `get_retry_at` which returns the `x-ratelimit-reset`value for the GitHub API.

Currently, the pipeline will simply return a result in the Dagster UI as in `INFO` event type such as "Score is 12.34% fake stars" and will provide a list of usernames flagged as fake.

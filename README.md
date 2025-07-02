# API Data Flow

## Have you inherited a bunch of Census API Endpoints Recently?

Are you an agency who works with [Tyler Tech's Open Data Platform](https://www.tylertech.com/products/data-insights/open-data-platform)? 
Do you have a bunch of Census API Endpoints with data that needs collecting?
Not enough humans to do the downloading? Then this app might be a solution
to your data problem. Sorry to hear about the humans though - Good luck
with that.

Designed to work with the Open Data Platfrom this app can be molded
to look a table of endpoints on the platform, and will pull and wrangle
those data and ship them off as one unified table back to a different
table on the platform. 

Make your table of endpoints a handy table of contents for your analyses
and/or a collection of metadata about your data pipeline sources and now
your endpoints are a public service, a process control table, and a dash of 
infrastructure as code...err, well, tables at least!


## Overview

This project is for managing CT-related data flow for external, public
sources from API's to/from the [CT Open Data Portal](https://data.ct.gov/).
But others are welcome to use an adapt to their specific needs.

## Usage

This is for ***public-facing data*** only. Under no circumstances should
content/data containing sensitive or potentially identifiable be pushed
to this repository. 


### Environmental Variables

Currently, for ease of reuse, the app expects some environmental variables
to exist in order to run.

- CENSUS_API_KEY: Your Census API key.
- TABLE_SOURCE: The Open Data Platform "Four by Four" [Identifier](https://dev.socrata.com/docs/endpoints).
- TABLE_TARGET: The Destination table for the data on the Open Data Platform.
- USERNAME: Open Data Platform Username - if needed.
- PASSWORD: Open Data Platfrom Password - if needed.
- TOKEN: Open Data Platform Application [token](https://dev.socrata.com/docs/app-tokens.html).
- DOMAIN: Domain name of Portal e.g. [data.ct.gov](https://data.ct.gov)

> Take care not to print, or commit sensitive information and/or 
> environmental variables to your repos and logs.

### UV

This app currently uses [uv](https://github.com/astral-sh/uv) for managing dependencies
and exectuion. `uv` is included in the provided codespace devcontainer.json. Otherwise
users should have it installed locally. Read the `uv` [installation instructions](https://github.com/astral-sh/ruff?tab=readme-ov-file#installation)
on GitHub for steps on how install it your local environment.

### Running the App

- Locally: 
    - Clone the repo.
    - Set the required environmental variables.
    - Ensure `uv` is installed. 
    - Inside of the terminal, `cd` to the repo folder and run `uv run src/api-data-flow/main.py`.
- Docker
    - Clone the repo.
    - Execute the `build.sh` file to build the image.
    - Execute the `run.sh` file to run the container and pull data from the Census API to your destionation table.

## Developer Experience

### Devcontainers

In this repo is a template `devcontainer.json` for developers who prefer to work inside
codespaces or containers.

### Github Actions

[Ruff](https://github.com/astral-sh/ruff) is used to check the project on a push or pull request.
If using a GitHub Codespace with the devcontainer.json provided `Ruff` will automatically
format code on save as well.

### Pre-Commit

[pre-commit](https://pre-commit.com/) is used and configured to have `Ruff`
fix and format code in a commit. It will block the commit until the issues
are resolved and/or the suggested fixes are committed.

### Weekly Dependabot Scans

There is a provided `dependabot.yml` that directs GitHub's [dependabot](https://docs.github.com/en/code-security/getting-started/dependabot-quickstart-guide#about-dependabot)
to scan for vulnerabilities and security updates weekly.  Dependabot scans
the `devcontainers` and `pip` ecosystems.

# API Data Flow

## Overview

This project is for managing CT-related data flow for external, public
sources from API's to/from the [CT Open Data Portal](https://data.ct.gov/).

## Usage

This is for ***public-facing data*** only. Under no circumstances should
content/data containing sensitive or potentially identifiable be pushed
to this repository. 

## Github Actions

`Ruff` is used to check the project on a push or pull request.


## Pre-Commit

[pre-commit](https://pre-commit.com/) is used and configured to have `Ruff`
fix and format code in a commit.

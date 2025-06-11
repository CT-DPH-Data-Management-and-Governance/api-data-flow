from pathlib import Path
import polars as pl
# import requests

# TODO: consider using pydantic and a target class


def grab_file_targets(filepath: None | Path = None) -> pl.DataFrame:
    # setting a default local to this repo
    if filepath is None:
        filepath = Path.cwd() / "data" / "targets" / "acs-api.parquet"

    return pl.read_parquet(filepath)


def grab_api_data(endpoint: str | None) -> pl.LazyFrame:
    return None

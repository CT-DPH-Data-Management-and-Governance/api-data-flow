from dataops.models import CensusAPIEndpoint
import polars as pl
import logging


def fetch_data(endpoints: list[str]) -> pl.LazyFrame:
    """
    Retrieve data from census api endpoints, wrangle, and make human-readable.
    """

    all_frames = []

    for endpoint in endpoints:
        logging.info(f"Fetching data from URL: {endpoint}")
        df = CensusAPIEndpoint.from_url(endpoint).fetch_tidy_data().lazy()
        all_frames.append(df)

    all_frames = pl.concat(all_frames)

    with_iid = all_frames.drop(["row_id", "date_pulled"]).with_columns(
        pl.struct(
            "dataset",
            "year",
            "concept",
            "geo_id",
            "ucgid",
            "geo_name",
            "variable_id",
            "variable_name",
            "value",
            "value_type",
        )
        .hash()
        .alias("instance_id")
    )

    url_id = with_iid.select(pl.col("full_url"), pl.col("instance_id")).unique()

    all_frames = (
        with_iid.drop("full_url")
        .unique()
        .join(url_id, on="instance_id")
        .with_row_index("row_id")
    )

    return all_frames


def pull_endpoints(df: pl.DataFrame) -> list[str]:
    """Retrieve a list of public census api endpoints."""

    return df.select(pl.col("endpoint").struct.unnest()).to_series().to_list()

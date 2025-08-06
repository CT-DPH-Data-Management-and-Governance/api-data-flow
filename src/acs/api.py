from dataops.apis.acs import APIEndpoint, APIData
import polars as pl
import logging


def fetch_data_from_endpoints(endpoints: list[str]) -> pl.LazyFrame:
    """
    Retrieve data from census api endpoints, wrangle, and make human-readable.
    """

    all_frames = []

    for endpoint in endpoints:
        logging.info(f"Fetching data from URL: {endpoint}")
        endpoint = APIEndpoint.from_url(endpoint)
        endpoint_data = APIData(endpoint=endpoint).long()
        all_frames.append(endpoint_data)

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

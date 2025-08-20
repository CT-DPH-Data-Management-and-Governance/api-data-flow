from dataops.apis.acs import APIEndpoint, APIData
import logging
from sodapy import Socrata
from dataops.socrata.data import fetch_data
from dataops.settings.flow import AppSettings
from datetime import datetime as dt
import polars as pl


def fetch_endpoints(
    source: str | None = None,
    settings: AppSettings | None = None,
) -> pl.LazyFrame:
    """
    Return a LazyFrame of all the endpoints.
    """

    if settings is None:
        settings = AppSettings()

    if source is None:
        source = settings.api.source.id

    data = fetch_data(source=source, settings=settings)

    output = data.with_columns(pl.col("date_last_pulled").str.to_datetime())

    return output


def needs_refresh(
    source: str | None = None,
    active_only: bool = True,
    refresh: str = "1y",
    settings: AppSettings | None = None,
) -> pl.LazyFrame:
    """
    Return a LazyFrame of only new endpoints or ones needing
    a data update according to the refresh variable.

    refresh uses the polars and datetime offset_by calendar
    string notation.
    """

    if settings is None:
        settings = AppSettings()

    if source is None:
        source = settings.api.source.id

    data = fetch_data(source=source, settings=settings)
    today = dt.today()

    output = (
        data.with_columns(pl.col("date_last_pulled").str.to_datetime())
        .with_columns(pl.col("date_last_pulled").dt.offset_by(refresh).alias("refresh"))
        .filter(pl.col("refresh").le(pl.lit(today)))
    )

    if active_only:
        output = (
            output.filter(pl.col("active").eq("1"))
            .drop(pl.col("refresh"))
            .lazy()
            .collect()
            .lazy()
        )
    else:
        output = output.drop(pl.col("refresh")).lazy().collect().lazy()

    return output


def update_source(
    data: pl.DataFrame | pl.LazyFrame,
    source: str | None = None,
    settings: AppSettings | None = None,
) -> pl.LazyFrame:
    """Update the endpoint source date last pulled based on row id."""

    if settings is None:
        settings = AppSettings()

    if source is None:
        source = settings.api.source.id

    now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    new = data.lazy().with_columns(pl.lit(now).alias("date_last_pulled"))
    dict_new = new.collect().to_dicts()

    with Socrata(
        settings.api.domain,
        settings.account.token.get_secret_value(),
        settings.account.username,
        settings.account.password.get_secret_value(),
    ) as client:
        client.upsert(source, dict_new)

    return new


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

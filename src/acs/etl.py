from sodapy import Socrata
from dataops.portal import fetch_data
from dataops.models import ApplicationSettings
from datetime import datetime as dt
import polars as pl


def needs_refresh(
    source: str | None = None,
    refresh: str = "1y",
    settings: ApplicationSettings | None = None,
) -> pl.LazyFrame:
    """
    Return a LazyFrame of only new endpoints or ones needing
    a data update according to the refresh variable.

    refresh uses the polars and datetime offset_by calendar
    string notation.
    """

    df = fetch_data(source=source, settings=settings)
    today = dt.today()
    new = (
        df.with_columns(pl.col("date_last_pulled").str.to_datetime())
        .with_columns(pl.col("date_last_pulled").dt.offset_by(refresh).alias("refresh"))
        .filter((pl.col("refresh").le(pl.lit(today))) & (pl.col("active").eq("1")))
        .drop(pl.col("refresh"))
    )
    return new


def update_source(
    data: pl.DataFrame | pl.LazyFrame,
    source: str | None = None,
    settings: ApplicationSettings | None = None,
) -> pl.LazyFrame:
    """Update the endpoint source date last pulled based on row id."""
    if settings is None:
        settings = ApplicationSettings()

    if source is None:
        source = settings.source_id

    now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    new = data.lazy().with_columns(pl.lit(now).alias("date_last_pulled"))
    dict_new = new.collect().to_dicts()

    with Socrata(
        settings.domain,
        settings.socrata_token,
        settings.socrata_user,
        settings.socrata_pass,
    ) as client:
        client.upsert(source, dict_new)

    return new

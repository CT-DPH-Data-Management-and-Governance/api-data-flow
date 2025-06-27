from dataops.models import CensusAPIEndpoint
import polars as pl
from dotenv import load_dotenv
import datetime as dt
import os
import logging
from sodapy import Socrata
import sys

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] - %(levelname)s - %(message)s"
)

today = dt.datetime.today().date()
TODAY = today.strftime("%Y-%m-%d")
CUTOFF = pl.Series([today]).dt.offset_by("-6mo").item()

# environmental variables/secrets
logging.info("Attempting to load environmental variables.")
if load_dotenv():
    CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
    TABLE_SOURCE = os.getenv("TABLE_SOURCE")
    TABLE_TARGET = os.getenv("TABLE_TARGET")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    TOKEN = os.getenv("TOKEN")
    DOMAIN = os.getenv("DOMAIN")
    logging.info("Environmental variables successfully loaded.")
else:
    logging.critical("Environmental variables cannot be loaded - exiting process.")
    sys.exit(1)


def fetch_data(urls: list[str]) -> pl.LazyFrame:
    """
    Retrieve data from census api endpoints, wrangle, and make human-readable.
    """

    all_frames = []

    for url in urls:
        logging.info(f"Fetching data from URL: {url}")
        df = CensusAPIEndpoint.from_url(url).fetch_tidy_data().lazy()
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


def fetch_source() -> pl.DataFrame:
    """Retrieve the source dataframe"""
    with Socrata(DOMAIN, TOKEN, USERNAME, PASSWORD) as client:
        source = client.get_all(TABLE_SOURCE)

    return pl.DataFrame(source)


def pull_urls(df: pl.DataFrame) -> list[str]:
    """Retrieve a list of public census api endpoints."""

    return df.select(pl.col("url").struct.unnest()).to_series().to_list()


def ship_it(data: list):
    with Socrata(DOMAIN, TOKEN, USERNAME, PASSWORD) as client:
        client.replace(TABLE_TARGET, data)


def only_new(cutoff: dt.date = CUTOFF):
    df = fetch_source()
    new = df.with_columns(pl.col("date_last_pulled").str.to_datetime()).filter(
        (pl.col("date_last_pulled").le(cutoff)) & (pl.col("active").eq("1"))
    )
    return new


def update_source(source: pl.DataFrame):
    new = source.with_columns(pl.lit(TODAY).alias("date_last_pulled")).to_dicts()

    with Socrata(DOMAIN, TOKEN, USERNAME, PASSWORD) as client:
        client.upsert(TABLE_SOURCE, new)


def main():
    """Entrypoint into the census api data flow app."""

    logging.info("Fetching endpoint data.")
    lf = fetch_data(pull_urls())
    logging.info("Endpoint data lazily loaded.")

    data = lf.with_columns(pl.lit(TODAY).alias("date_pulled")).collect().to_dicts()

    logging.info("Pushing data to the ODP.")
    ship_it(data)
    logging.info("Data successfully pushed to the ODP.")


if __name__ == "__main__":
    main()

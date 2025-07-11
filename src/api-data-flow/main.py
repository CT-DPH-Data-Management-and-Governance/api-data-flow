from dataops.portal import replace_data
import polars as pl
import datetime as dt
import logging
from etl import only_new, update_source
from fetch import fetch_data, pull_endpoints

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] - %(levelname)s - %(message)s"
)

today = dt.datetime.today().date()
TODAY = today.strftime("%Y-%m-%d")
CUTOFF = pl.Series([today]).dt.offset_by("-6mo").item()


def main():
    """Entrypoint into the census api data flow app."""

    logging.info("Fetching endpoint data.")
    source = only_new()

    if not source.is_empty():
        endpoints = pull_endpoints(source)
        lf = fetch_data(endpoints)
        logging.info("Endpoint data lazily loaded.")

        data = lf.with_columns(pl.lit(TODAY).alias("date_pulled")).collect()

        logging.info("Pushing data to the ODP.")
        replace_data(data)
        logging.info("Data successfully pushed to the ODP.")

        logging.info("Updating Source metadata.")
        update_source(source)
        logging.info("Metadata sucessfully updated.")

    else:
        logging.info("No eligible endpoints - ending process.")


if __name__ == "__main__":
    main()
